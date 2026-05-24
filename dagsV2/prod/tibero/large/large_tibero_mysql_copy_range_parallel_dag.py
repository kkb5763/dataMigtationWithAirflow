import jaydebeapi
import MySQLdb

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from typing import Any, Dict, Tuple

# ==========================================================
# LARGE: Tibero 구간당 Airflow 태스크 1개 (초대형 테이블)
# ==========================================================
# sample 의 _worker 1회분(구간 1개)을 태스크로 분리 → max_active_tasks 로 구간 병렬
# STEP_SIZE 를 크게 (1천만) 잡아 태스크 수 제한

TIBERO_CONFIG: Dict[str, Any] = {
    "host": "10.10.4.10",
    "port": 8629,
    "sid": "tibero",
    "user": "sys",
    "pass": "tibero_src_pwd",
    "jdbc_jar": "/data/airflow/lib/thr_jdbc.jar",
    "driver": "com.tmax.tibero.jdbc.TbDriver",
}

TGT_MYSQL_BASE: Dict[str, Any] = {
    "host": "10.10.1.20",
    "user": "root",
    "passwd": "tgt_mysql_pass456@",
    "charset": "utf8mb4",
}

MYSQL_DB_MAP: Dict[str, str] = {"MEMBER": "member_db"}

TABLE_CONFIG: Dict[str, Tuple[str, int, int]] = {
    # "MEMBER.BIG_MBR_BASE": ("MBR_NO", 1, 40_000_000),
}

STEP_SIZE = 10_000_000  # 태스크당 구간 폭
FETCH_SIZE = 2_000
INSERT_STRATEGY = "REPLACE"


def _sql_literal(v: int) -> str:
    return str(int(v))


def _split_schema_table(full_name: str) -> Tuple[str, str]:
    schema, table = full_name.split(".", 1)
    return schema.upper(), table


def _mysql_cfg(schema: str) -> Dict[str, Any]:
    cfg = dict(TGT_MYSQL_BASE)
    cfg["db"] = MYSQL_DB_MAP.get(schema, schema.lower())
    return cfg


def _worker_one_range(
    table: str,
    range_col: str,
    range_start: int,
    range_end: int,
    **context: Any,
) -> None:
    """sample _worker 의 while 루프 1회분만 실행."""
    schema, tbl = _split_schema_table(table)
    t_conn = None
    m_conn = None
    try:
        m_conn = MySQLdb.connect(**_mysql_cfg(schema))
        m_cur = m_conn.cursor()
        url = f"jdbc:tibero:thin:@{TIBERO_CONFIG['host']}:{TIBERO_CONFIG['port']}:{TIBERO_CONFIG['sid']}"
        t_conn = jaydebeapi.connect(
            TIBERO_CONFIG["driver"],
            url,
            [TIBERO_CONFIG["user"], TIBERO_CONFIG["pass"]],
            TIBERO_CONFIG["jdbc_jar"],
        )
        t_cur = t_conn.cursor()

        query = (
            f"SELECT * FROM {schema}.{tbl} "
            f"WHERE {range_col} >= {_sql_literal(range_start)} "
            f"AND {range_col} <= {_sql_literal(range_end)}"
        )
        print(f"[LARGE-TIB-RANGE][{table}] 실행 쿼리: {query}", flush=True)
        t_cur.execute(query)

        placeholders = ", ".join(["%s"] * len(t_cur.description))
        if INSERT_STRATEGY == "REPLACE":
            insert_sql = f"REPLACE INTO {tbl} VALUES ({placeholders})"
        elif INSERT_STRATEGY == "INSERT":
            insert_sql = f"INSERT INTO {tbl} VALUES ({placeholders})"
        elif INSERT_STRATEGY == "INSERT IGNORE":
            insert_sql = f"INSERT IGNORE INTO {tbl} VALUES ({placeholders})"
        else:
            raise ValueError(f"Unsupported INSERT_STRATEGY: {INSERT_STRATEGY}")

        total = 0
        while True:
            rows = t_cur.fetchmany(FETCH_SIZE)
            if not rows:
                break
            m_cur.executemany(insert_sql, rows)
            m_conn.commit()
            total += len(rows)

        print(
            f"[LARGE-TIB-RANGE][{table}] range {range_start}~{range_end} done total={total}",
            flush=True,
        )
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


with DAG(
    dag_id="large_tibero_mysql_copy_range_parallel_v1",
    default_args={"owner": "large", "retries": 1, "retry_delay": timedelta(minutes=5)},
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    tags=["large", "tibero", "mysql", "copy", "range", "parallel"],
    max_active_tasks=4,
) as dag:
    for table_name, (col, start, end) in TABLE_CONFIG.items():
        current = int(start)
        end_i = int(end)
        while current <= end_i:
            current_end = min(current + STEP_SIZE - 1, end_i)
            PythonOperator(
                task_id=f"migrate_{table_name.replace('.', '_')}_{current}_{current_end}",
                python_callable=_worker_one_range,
                pool="large_tibero_mysql_range",
                pool_slots=1,
                op_kwargs={
                    "table": table_name,
                    "range_col": col,
                    "range_start": current,
                    "range_end": current_end,
                },
            )
            current += STEP_SIZE
