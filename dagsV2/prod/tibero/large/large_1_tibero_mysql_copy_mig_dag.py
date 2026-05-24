import jaydebeapi
import MySQLdb

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from typing import Any, Dict, Tuple

# ==========================================================
# LARGE-1: Tibero6 -> MySQL 대용량 (테이블 그룹 1)
# ==========================================================
# sample_tibero_chunk_mig_dag.py: 10만건(STEP_SIZE) 범위 단위 SELECT/INSERT 반복
# Pools: large_tibero_mysql_copy_1 / slots 4

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

MYSQL_DB_MAP: Dict[str, str] = {
    "MEMBER": "member_db",
    "ORDER": "order_db",
    "LOG": "log_db",
}

TABLE_CONFIG: Dict[str, Tuple[str, int, int]] = {
    "MEMBER.BIG_MBR_BASE": ("MBR_NO", 1, 50_000_000),
    "MEMBER.BIG_MBR_HIST": ("HIST_ID", 1, 80_000_000),
    "MEMBER.BIG_MBR_ADDR": ("ADDR_ID", 1, 30_000_000),
}

STEP_SIZE = 100_000
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


def _worker(table: str, range_col: str, start: int, end: int, **context: Any) -> None:
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

        current = int(start)
        total = 0

        while current <= end:
            current_end = min(current + STEP_SIZE - 1, end)
            query = (
                f"SELECT * FROM {schema}.{tbl} "
                f"WHERE {range_col} >= {_sql_literal(current)} "
                f"AND {range_col} <= {_sql_literal(current_end)}"
            )
            print(f"[LARGE-TIB-1][{table}] 실행 쿼리: {query}", flush=True)
            t_cur.execute(query)

            col_count = len(t_cur.description)
            placeholders = ", ".join(["%s"] * col_count)
            if INSERT_STRATEGY == "REPLACE":
                insert_sql = f"REPLACE INTO {tbl} VALUES ({placeholders})"
            elif INSERT_STRATEGY == "INSERT":
                insert_sql = f"INSERT INTO {tbl} VALUES ({placeholders})"
            elif INSERT_STRATEGY == "INSERT IGNORE":
                insert_sql = f"INSERT IGNORE INTO {tbl} VALUES ({placeholders})"
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
                f"[LARGE-TIB-1][{table}] range {current}~{current_end} done "
                f"(rows={chunk_rows}, total={total})",
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


with DAG(
    dag_id="large_tibero_mysql_copy_1_v1",
    default_args={"owner": "large", "retries": 1, "retry_delay": timedelta(minutes=5)},
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    tags=["large", "tibero", "mysql", "copy", "chunk"],
    max_active_tasks=4,
) as dag:
    for table_name, (col, start, end) in TABLE_CONFIG.items():
        PythonOperator(
            task_id=f"migrate_{table_name.replace('.', '_')}",
            python_callable=_worker,
            pool="large_tibero_mysql_copy_1",
            pool_slots=1,
            op_kwargs={
                "table": table_name,
                "range_col": col,
                "start": start,
                "end": end,
            },
        )
