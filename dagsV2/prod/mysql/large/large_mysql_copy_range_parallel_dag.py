import MySQLdb
import MySQLdb.cursors

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

# ==========================================================
# LARGE: PK 구간별 Airflow 태스크 병렬 (단일 초대형 테이블용)
# ==========================================================
# - 테이블 1개를 STEP_SIZE 구간마다 migrate_<table>_<start>_<end> 태스크로 분리
# - max_active_tasks=4 → 구간 최대 4개 동시 이행 (한 테이블 내 병렬)
# - STEP_SIZE 를 크게 잡을 것 (예: 1천만) — 태스크 수 = (end-start)/STEP
#   4억 건·STEP 10만 → 약 4000 태스크 (UI·스케줄러 부담) → STEP 1000만 권장 → 약 40 태스크
#
# Pools (Admin): large_mysql_range / slots 4

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

STEP_SIZE = 10_000_000  # 구간당 태스크 1개 — 대용량일수록 크게
CHUNK_SIZE = 5000
INSERT_STRATEGY = "REPLACE"

# 단일 테이블·PK 범위 (예: 4천만 건 → STEP 1천만 → 태스크 4개)
TABLE_CONFIG: Dict[str, Dict[str, Any]] = {
    # "big_mbr": {
    #     "range": {"col": "mbr_no", "start": 1, "end": 40_000_000},
    # },
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


def _mysql_copy_range(
    table: str,
    range_col: str,
    range_start: int,
    range_end: int,
    **context: Any,
) -> None:
    """PK 구간 1개만 이행 (Airflow 태스크 1개 = 구간 1개)."""
    src_cfg = dict(SRC_MYSQL)
    src_cfg["cursorclass"] = MySQLdb.cursors.SSCursor
    tgt_cfg = dict(TGT_MYSQL)

    src_conn = None
    tgt_conn = None
    try:
        src_conn = MySQLdb.connect(**src_cfg)
        tgt_conn = MySQLdb.connect(**tgt_cfg)
        src_cur = src_conn.cursor()
        tgt_cur = tgt_conn.cursor()

        query = (
            f"SELECT * FROM {_quote_ident(table)} "
            f"WHERE {_quote_ident(range_col)} BETWEEN %s AND %s"
        )
        params = (range_start, range_end)
        print(f"[LARGE-RANGE][{table}] {range_start}~{range_end} {query}", flush=True)
        src_cur.execute(query, params)

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

        total = 0
        while True:
            rows = src_cur.fetchmany(CHUNK_SIZE)
            if not rows:
                break
            tgt_cur.executemany(insert_sql, rows)
            tgt_conn.commit()
            total += len(rows)
            print(
                f" >>> [LARGE-RANGE][{table}] {range_start}~{range_end} {total} rows...",
                flush=True,
            )
        print(f"[LARGE-RANGE][{table}] {range_start}~{range_end} done total={total}", flush=True)
    except Exception as e:
        print(f"!!! [LARGE-RANGE][{table}] {range_start}~{range_end} failed: {e}", flush=True)
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
    dag_id="large_mysql_copy_range_parallel_v1",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    tags=["large", "mysql", "copy", "range", "parallel"],
    max_active_tasks=4,
) as dag:
    for table_name, cfg in TABLE_CONFIG.items():
        range_cfg = dict(cfg.get("range", {}) or {})
        range_col = range_cfg.get("col")
        start_id = range_cfg.get("start")
        end_id = range_cfg.get("end")
        if not range_col or start_id is None or end_id is None:
            raise ValueError(f"{table_name}: range.col, start, end (int) required")
        if not isinstance(start_id, int) or not isinstance(end_id, int):
            raise ValueError(f"{table_name}: start/end must be int")

        for r_start, r_end in _iter_bigint_ranges(int(start_id), int(end_id), STEP_SIZE):
            PythonOperator(
                task_id=f"migrate_{table_name}_{r_start}_{r_end}",
                python_callable=_mysql_copy_range,
                pool="large_mysql_range",
                pool_slots=1,
                op_kwargs={
                    "table": table_name,
                    "range_col": range_col,
                    "range_start": r_start,
                    "range_end": r_end,
                },
            )
