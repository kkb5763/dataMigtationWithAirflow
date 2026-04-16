import MySQLdb
import MySQLdb.cursors

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from typing import Any, Dict, Tuple

# ==========================================================
# MySQL -> MySQL (Sample) : 10만건 범위 단위 SELECT/INSERT 반복
# ==========================================================
# 목적:
# - "전체 SELECT 후 fetchmany" 방식이 아니라,
#   10만건(또는 지정 step) 범위를 쪼개서 쿼리 자체를 반복 실행
# - 대용량 테이블에서 장시간 커서 유지/트랜잭션 부담을 줄이는 샘플


SRC_MYSQL_CONFIG: Dict[str, Any] = {
    "host": "10.10.1.10",
    "port": 3306,
    "user": "root",
    "passwd": "src_mysql_pass123!",
    "db": "source_db",
    "charset": "utf8mb4",
}

TGT_MYSQL_CONFIG: Dict[str, Any] = {
    "host": "10.10.1.20",
    "port": 3306,
    "user": "root",
    "passwd": "tgt_mysql_pass456@",
    "db": "target_db",
    "charset": "utf8mb4",
}

# ==========================================================
# 테이블별 범위 설정 (필수)
# 구조: table: (range_col, start, end)
# ==========================================================
TABLE_CONFIG: Dict[str, Tuple[str, int, int]] = {
    # 예시) 숫자형 PK 범위 기반
    "sample_table": ("id", 1, 1_000_000),
}

# 10만 건 단위(범위 폭)로 반복
STEP_SIZE = 100_000

# 내부 적재 배치(메모리 조절용)
FETCH_SIZE = 5_000

INSERT_STRATEGY = "REPLACE"  # "REPLACE" | "INSERT" | "INSERT IGNORE"


def _quote_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _worker(table: str, range_col: str, start: int, end: int, **context: Any) -> None:
    src_conn = None
    tgt_conn = None
    try:
        src_conn = MySQLdb.connect(**SRC_MYSQL_CONFIG)
        tgt_conn = MySQLdb.connect(**TGT_MYSQL_CONFIG)

        # 소스는 SSCursor로 스트리밍(fetchmany)
        src_cur = src_conn.cursor(MySQLdb.cursors.SSCursor)
        tgt_cur = tgt_conn.cursor()

        current = int(start)
        total = 0

        while current <= end:
            current_end = min(current + STEP_SIZE - 1, end)
            query = (
                f"SELECT * FROM {_quote_ident(table)} "
                f"WHERE {_quote_ident(range_col)} BETWEEN %s AND %s"
            )
            src_cur.execute(query, (current, current_end))

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

            chunk_rows = 0
            while True:
                rows = src_cur.fetchmany(FETCH_SIZE)
                if not rows:
                    break
                tgt_cur.executemany(insert_sql, rows)
                tgt_conn.commit()
                chunk_rows += len(rows)
                total += len(rows)

            print(
                f"[{table}] range {current}~{current_end} done (rows={chunk_rows}, total={total})",
                flush=True,
            )

            current += STEP_SIZE

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
    "owner": "sample",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="sample_mysql_to_mysql_chunk_v1",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    tags=["sample", "mysql", "chunk"],
    max_active_tasks=1,
) as dag:
    for table_name, (col, start, end) in TABLE_CONFIG.items():
        PythonOperator(
            task_id=f"migrate_{table_name}",
            python_callable=_worker,
            op_kwargs={
                "table": table_name,
                "range_col": col,
                "start": start,
                "end": end,
            },
        )

