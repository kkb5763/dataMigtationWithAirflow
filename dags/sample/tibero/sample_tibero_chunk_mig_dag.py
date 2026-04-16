import jaydebeapi
import MySQLdb

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from typing import Any, Dict, Tuple

# ==========================================================
# Tibero -> MySQL (Sample) : 10만건 범위 단위 SELECT/INSERT 반복
# ==========================================================
# 목적:
# - Tibero에서 "전체 SELECT"를 길게 유지하지 않고,
#   10만건(또는 지정 step) 범위를 쪼개서 쿼리 자체를 반복 실행


TIBERO_CONFIG: Dict[str, Any] = {
    "host": "10.10.4.10",
    "port": 8629,
    "sid": "tibero",
    "user": "sys",
    "pass": "tibero_src_pwd",
    "jdbc_jar": "/data/airflow/lib/thr_jdbc.jar",
    "driver": "com.tmax.tibero.jdbc.TbDriver",
    # schema 지정이 필요하면 table을 "SCHEMA.TABLE"로 넣으세요.
}

MYSQL_CONFIG: Dict[str, Any] = {
    "host": "10.10.1.20",
    "user": "root",
    "passwd": "tgt_mysql_pass456@",
    "db": "target_db",
}

# ==========================================================
# 테이블별 범위 설정 (필수)
# 구조: table: (range_col, start, end)
# ==========================================================
TABLE_CONFIG: Dict[str, Tuple[str, int, int]] = {
    "SAMPLE_TABLE": ("ID", 1, 1_000_000),
}

STEP_SIZE = 100_000
FETCH_SIZE = 2_000
INSERT_STRATEGY = "REPLACE"


def _sql_literal(v: int) -> str:
    return str(int(v))


def _worker(table: str, range_col: str, start: int, end: int, **context: Any) -> None:
    t_conn = None
    m_conn = None
    try:
        m_conn = MySQLdb.connect(**MYSQL_CONFIG)
        m_cur = m_conn.cursor()

        url = f"jdbc:tibero:thin:@{TIBERO_CONFIG['host']}:{TIBERO_CONFIG['port']}:{TIBERO_CONFIG['sid']}"
        t_conn = jaydebeapi.connect(
            TIBERO_CONFIG["driver"],
            url,
            [TIBERO_CONFIG["user"], TIBERO_CONFIG["pass"]],
            TIBERO_CONFIG["jdbc_jar"],
        )
        t_cur = t_conn.cursor()

        current = int(start)
        total = 0

        while current <= end:
            current_end = min(current + STEP_SIZE - 1, end)
            query = (
                f"SELECT * FROM {table} "
                f"WHERE {range_col} >= {_sql_literal(current)} AND {range_col} <= {_sql_literal(current_end)}"
            )
            print(f"[{table}] 실행 쿼리: {query}", flush=True)

            t_cur.execute(query)

            col_count = len(t_cur.description)
            placeholders = ", ".join(["%s"] * col_count)
            if INSERT_STRATEGY == "REPLACE":
                insert_sql = f"REPLACE INTO {table} VALUES ({placeholders})"
            elif INSERT_STRATEGY == "INSERT":
                insert_sql = f"INSERT INTO {table} VALUES ({placeholders})"
            elif INSERT_STRATEGY == "INSERT IGNORE":
                insert_sql = f"INSERT IGNORE INTO {table} VALUES ({placeholders})"
            else:
                raise ValueError(f"Unsupported INSERT_STRATEGY: {INSERT_STRATEGY}")

            chunk_rows = 0
            while True:
                rows = t_cur.fetchmany(FETCH_SIZE)
                if not rows:
                    break
                m_cur.executemany(insert_sql, rows)
                m_conn.commit()
                chunk_rows += len(rows)
                total += len(rows)

            print(
                f"[{table}] range {current}~{current_end} done (rows={chunk_rows}, total={total})",
                flush=True,
            )
            current += STEP_SIZE

    finally:
        if t_conn:
            try:
                t_conn.close()
            except Exception:
                pass
        if m_conn:
            try:
                m_conn.close()
            except Exception:
                pass


default_args = {
    "owner": "sample",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="sample_tibero_to_mysql_chunk_v1",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    tags=["sample", "tibero", "chunk"],
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

