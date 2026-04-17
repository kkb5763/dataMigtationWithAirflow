from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple, Union

import jaydebeapi
import MySQLdb

from airflow import DAG
from airflow.operators.python import PythonOperator


# ==========================================================
# PROD: Tibero(dtg, multi-schema) -> MySQL "청킹 SELECT/INSERT"
# ==========================================================

STEP_SIZE = 100_000  # 10만건 범위 단위로 SELECT 반복 (id 같은 숫자형 기준일 때)
FETCH_SIZE = 5_000   # 내부 적재는 5천건씩 fetchmany
INSERT_STRATEGY = "REPLACE"  # "REPLACE" | "INSERT" | "INSERT IGNORE"


TIBERO_CONFIG: Dict[str, Any] = {
    "host": "10.10.4.10",
    "port": 8629,
    "sid": "tibero",
    "user": "sys",
    "pass": "tibero_src_pwd",
    "jdbc_jar": "/data/airflow/lib/thr_jdbc.jar",
    "driver": "com.tmax.tibero.jdbc.TbDriver",
}


TableConfig = Dict[str, Tuple[str, Optional[Union[int, str]], Optional[Union[int, str]]]]
SCHEMA_TABLE_CONFIGS: Dict[str, TableConfig] = {
    # 스키마별 최소 2개 테이블 예시 (실제 테이블로 교체)
    # sample과 동일하게 "range_col, start(int), end(int)"를 넣으면 STEP_SIZE로 쪼개서 반복합니다.
    "dtg_schema_01": {"dtg_table_01": ("id", 1, 1_000_000), "dtg_table_02": ("id", 1, 1_000_000)},
    "dtg_schema_02": {"dtg_table_01": ("id", 1, 1_000_000), "dtg_table_02": ("id", 1, 1_000_000)},
    "dtg_schema_03": {"dtg_table_01": ("id", 1, 1_000_000), "dtg_table_02": ("id", 1, 1_000_000)},
    "dtg_schema_04": {"dtg_table_01": ("id", 1, 1_000_000), "dtg_table_02": ("id", 1, 1_000_000)},
}


def _jdbc_args() -> Dict[str, Any]:
    url = f"jdbc:tibero:thin:@{TIBERO_CONFIG['host']}:{TIBERO_CONFIG['port']}:{TIBERO_CONFIG['sid']}"
    return {
        "jclassname": TIBERO_CONFIG["driver"],
        "url": url,
        "driver_args": [TIBERO_CONFIG["user"], TIBERO_CONFIG["pass"]],
        "jars": TIBERO_CONFIG["jdbc_jar"],
    }


def _mk_mysql_cfg(schema: str) -> Dict[str, Any]:
    # 기본은 Tibero schema 이름 그대로 MySQL db로 적재
    return {
        "host": "10.10.1.20",
        "user": "root",
        "passwd": "tgt_mysql_pass456@",
        "db": schema,
        "charset": "utf8mb4",
    }


def _migrate_tibero_table(
    schema: str,
    table: str,
    filter_col: str,
    start_val: Optional[Union[int, str]],
    end_val: Optional[Union[int, str]],
    **context: Any,
) -> None:
    t_conn = None
    m_conn = None
    try:
        m_conn = MySQLdb.connect(**_mk_mysql_cfg(schema))
        m_cur = m_conn.cursor()

        t_conn = jaydebeapi.connect(**_jdbc_args())
        t_cur = t_conn.cursor()

        if start_val is not None and end_val is not None and isinstance(start_val, int) and isinstance(end_val, int):
            ranges = []
            current = int(start_val)
            while current <= int(end_val):
                current_end = min(current + STEP_SIZE - 1, int(end_val))
                ranges.append((current, current_end))
                current += STEP_SIZE
        else:
            ranges = [(start_val, end_val)]

        total = 0
        for r_start, r_end in ranges:
            query = f"SELECT * FROM {schema}.{table}"
            params = []
            if r_start is not None and r_end is not None and isinstance(r_start, int) and isinstance(r_end, int):
                query += f" WHERE {filter_col} BETWEEN ? AND ?"
                params = [r_start, r_end]
            elif r_start is not None:
                query += f" WHERE {filter_col} >= ?"
                params = [r_start]
            elif r_end is not None:
                query += f" WHERE {filter_col} <= ?"
                params = [r_end]

            if params:
                t_cur.execute(query, tuple(params))
            else:
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
                f"[MIG] {schema}.{table} range {r_start}~{r_end} done (rows={chunk_rows}, total={total})",
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


default_args = {"owner": "prod", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="prod_tibero_dtg_chunk_mig_v1",
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["prod", "migration", "chunk", "tibero", "dtg"],
    max_active_tasks=2,
) as dag:
    for schema_name, table_cfg in SCHEMA_TABLE_CONFIGS.items():
        for table_name, (col, start, end) in table_cfg.items():
            PythonOperator(
                task_id=f"migrate__{schema_name}__{table_name}",
                python_callable=_migrate_tibero_table,
                op_kwargs={
                    "schema": schema_name,
                    "table": table_name,
                    "filter_col": col,
                    "start_val": start,
                    "end_val": end,
                },
            )

