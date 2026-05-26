import MySQLdb
import MySQLdb.cursors

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

# ==========================================================
# LARGE-1: MySQL -> MySQL 대용량 단순 적재 (테이블 그룹 1)
# ==========================================================
# - 수천만 ~ 수억 건: BIGINT PK 로 STEP_SIZE 구간 분할 SELECT (전체 스캔 금지)
# - 소스·타겟 동일 구조 → SELECT * → REPLACE
# - 암호화 컬럼 이행은 ../enc/prod_mysql_enc_mig_v1 (HTTP 변환)
#
# TABLE_CONFIG 에 테이블·PK·start/end 만 추가
# Pools (Admin): large_mysql_copy_1 / slots 4

SRC_MYSQL: Dict[str, Any] = {
    "host": "10.10.1.10",
    "port": 3306,
    "user": "root",
    "passwd": "src_mysql_pass123!",
    "db": "member_db",
    "charset": "utf8mb4",
}

TGT_MYSQL: Dict[str, Any] = {
    "host": "10.10.1.20",
    "port": 3306,
    "user": "root",
    "passwd": "tgt_mysql_pass456@",
    "db": "member_db",
    "charset": "utf8mb4",
}

STEP_SIZE = 100_000  # PK 구간 폭 (4억 건·STEP 10만 → 약 4000회 SELECT)
CHUNK_SIZE = 5000  # fetchmany·커밋 단위
INSERT_STRATEGY = "REPLACE"  # REPLACE | INSERT | INSERT IGNORE

# range.col: BIGINT PK 컬럼 / start·end: 이관 id 범위 (정수) — 샘플 3테이블
TABLE_CONFIG: Dict[str, Dict[str, Any]] = {
    "big_mbr_base": {
        "range": {"col": "mbr_no", "start": 1, "end": 50_000_000},  # 약 5천만
    },
    "big_mbr_hist": {
        "range": {"col": "hist_id", "start": 1, "end": 80_000_000},  # 약 8천만
    },
    # big_mbr_addr 는 range select 가 느리거나 key 가 없을 수 있어 large_4 전용 DAG 로 분리
    # - DAG: large_4_mysql_copy_single_table_v1
}


def _quote_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _iter_bigint_ranges(start_id: int, end_id: int, step: int) -> List[Tuple[int, int]]:
    ranges: List[Tuple[int, int]] = []
    current = int(start_id)
    end_i = int(end_id)
    step_i = int(step)
    while current <= end_i:
        current_end = min(current + step_i - 1, end_i)
        ranges.append((current, current_end))
        current += step_i
    return ranges


def _mysql_copy_table(table: str, **context: Any) -> None:
    cfg = TABLE_CONFIG[table]
    range_cfg: Dict[str, Any] = dict(cfg.get("range", {}) or {})
    range_col = range_cfg.get("col")
    start_id = range_cfg.get("start")
    end_id = range_cfg.get("end")

    src_cfg = dict(SRC_MYSQL)
    src_cfg["cursorclass"] = MySQLdb.cursors.SSCursor  # 서버 사이드 커서 (대용량 메모리 방지)
    tgt_cfg = dict(TGT_MYSQL)

    if (
        range_col
        and start_id is not None
        and end_id is not None
        and isinstance(start_id, int)
        and isinstance(end_id, int)
    ):
        id_ranges = _iter_bigint_ranges(int(start_id), int(end_id), STEP_SIZE)
    else:
        id_ranges = [(start_id, end_id)]

    src_conn = None
    tgt_conn = None
    try:
        src_conn = MySQLdb.connect(**src_cfg)
        tgt_conn = MySQLdb.connect(**tgt_cfg)
        tgt_cur = tgt_conn.cursor()

        insert_sql: Optional[str] = None
        total = 0

        for r_start, r_end in id_ranges:
            src_cur = src_conn.cursor()
            query = f"SELECT * FROM {_quote_ident(table)}"
            params: List[Any] = []

            if range_col and isinstance(r_start, int) and isinstance(r_end, int):
                query += f" WHERE {_quote_ident(range_col)} BETWEEN %s AND %s"
                params = [r_start, r_end]
            elif range_col and r_start is not None:
                query += f" WHERE {_quote_ident(range_col)} >= %s"
                params = [r_start]
            elif range_col and r_end is not None:
                query += f" WHERE {_quote_ident(range_col)} <= %s"
                params = [r_end]

            print(f"[LARGE-1][{table}] range {r_start}~{r_end} {query}", flush=True)
            src_cur.execute(query, tuple(params))

            col_count = len(src_cur.description)
            placeholders = ", ".join(["%s"] * col_count)
            if INSERT_STRATEGY == "REPLACE":
                insert_sql = f"REPLACE INTO {_quote_ident(table)} VALUES ({placeholders})"
            elif INSERT_STRATEGY == "INSERT":
                insert_sql = f"INSERT INTO {_quote_ident(table)} VALUES ({placeholders})"
            elif INSERT_STRATEGY == "INSERT IGNORE":
                insert_sql = f"INSERT IGNORE INTO {_quote_ident(table)} VALUES ({placeholders})"
            else:
                raise ValueError(f"Unsupported INSERT_STRATEGY: {INSERT_STRATEGY}")

            chunk_in_range = 0
            while True:
                rows = src_cur.fetchmany(CHUNK_SIZE)
                if not rows:
                    break
                tgt_cur.executemany(insert_sql, rows)
                tgt_conn.commit()
                chunk_in_range += len(rows)
                total += len(rows)

            try:
                src_cur.close()
            except Exception:
                pass
            print(
                f" >>> [LARGE-1][{table}] range {r_start}~{r_end} rows={chunk_in_range} total={total}",
                flush=True,
            )

        print(f"[LARGE-1][{table}] finished total={total}", flush=True)
    except Exception as e:
        print(f"!!! [LARGE-1][{table}] failed: {e}", flush=True)
        raise
    finally:
        if src_conn:
            try:
                src_conn.close()
            except Exception:
                pass
        if tgt_conn:
            try:
                tgt_conn.close()
            except Exception:
                pass


default_args = {
    "owner": "large",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="large_mysql_copy_1_v1",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    tags=["large", "large_1", "mysql", "copy", "migration"],
    max_active_tasks=4,  # 테이블 여러 개일 때 최대 4개 migrate_* 동시
) as dag:
    for table_name in TABLE_CONFIG:
        PythonOperator(
            task_id=f"migrate_{table_name}",
            python_callable=_mysql_copy_table,
            pool="large_mysql_copy_1",
            pool_slots=1,
            op_kwargs={"table": table_name},
        )
