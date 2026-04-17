from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple, Union

import MySQLdb
import MySQLdb.cursors

from airflow import DAG
from airflow.operators.python import PythonOperator

from common.damo_scp import env_or_var


# ==========================================================
# PROD: MySQL(dip 계열) -> MySQL "청킹 SELECT/INSERT"
# ==========================================================

STEP_SIZE = 100_000  # 10만건 범위 단위로 SELECT 반복
FETCH_SIZE = 5_000   # 내부 적재는 5천건씩 fetchmany
INSERT_STRATEGY = "REPLACE"  # "REPLACE" | "INSERT" | "INSERT IGNORE"


def _mk_src_cfg(db_name: str) -> Dict[str, Any]:
    base: Dict[str, Any] = {
        "host": env_or_var("DIP_SRC_HOST", "10.10.1.10"),
        "user": env_or_var("DIP_SRC_USER", "root"),
        "passwd": env_or_var("DIP_SRC_PASS", "src_password123!"),
        "charset": "utf8mb4",
    }
    base["db"] = db_name
    return base


def _mk_tgt_cfg(db_name: str) -> Dict[str, Any]:
    base: Dict[str, Any] = {
        "host": env_or_var("DIP_TGT_HOST", "10.10.1.20"),
        "user": env_or_var("DIP_TGT_USER", "root"),
        "passwd": env_or_var("DIP_TGT_PASS", "tgt_password456@"),
        "charset": "utf8mb4",
    }
    base["db"] = db_name
    return base


# schema(db)별 테이블 설정: (filter_col, start_val, end_val)
TableConfig = Dict[str, Tuple[str, Optional[Union[int, str]], Optional[Union[int, str]]]]
SCHEMA_TABLE_CONFIGS: Dict[str, TableConfig] = {
    # 스키마별 최소 2개 테이블 예시 (실제 테이블로 교체)
    "dip": {
        "code_definition": ("code", None, None),
        "code_definition_hist": ("code", None, None),
    },
    "sl": {
        "code_definition": ("code", None, None),
        "code_definition_hist": ("code", None, None),
    },
    "mlops": {
        "code_definition": ("code", None, None),
        "code_definition_hist": ("code", None, None),
    },
}


def _quote_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _migrate_mysql_table_chunked(
    schema: str,
    table: str,
    filter_col: str,
    start_val: Optional[Union[int, str]],
    end_val: Optional[Union[int, str]],
    **context: Any,
) -> None:
    src_conn = None
    tgt_conn = None
    try:
        src_conn = MySQLdb.connect(**_mk_src_cfg(schema))
        tgt_conn = MySQLdb.connect(**_mk_tgt_cfg(schema))

        src_cur = src_conn.cursor(MySQLdb.cursors.SSCursor)
        tgt_cur = tgt_conn.cursor()

        # sample_mysql_chunk_mig_dag.py 패턴과 동일:
        # - start/end(int)가 있으면 STEP_SIZE 단위로 BETWEEN 쿼리를 반복 실행
        # - start/end가 없으면 전체를 스트리밍 fetchmany로 적재
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
            query = f"SELECT * FROM {_quote_ident(table)}"
            params = []
            if r_start is not None and r_end is not None and isinstance(r_start, int) and isinstance(r_end, int):
                query += f" WHERE {_quote_ident(filter_col)} BETWEEN %s AND %s"
                params = [r_start, r_end]
            elif r_start is not None:
                query += f" WHERE {_quote_ident(filter_col)} >= %s"
                params = [r_start]
            elif r_end is not None:
                query += f" WHERE {_quote_ident(filter_col)} <= %s"
                params = [r_end]

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
                raise ValueError(f"Unsupported PROD_INSERT_STRATEGY: {INSERT_STRATEGY}")

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
                f"[MIG] {schema}.{table} range {r_start}~{r_end} done (rows={chunk_rows}, total={total})",
                flush=True,
            )
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


default_args = {"owner": "prod", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="prod_mysql_dip_chunk_mig_v1",
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["prod", "migration", "chunk", "mysql", "dip"],
    max_active_tasks=2,
) as dag:
    for schema_name, table_cfg in SCHEMA_TABLE_CONFIGS.items():
        for table_name, (col, start, end) in table_cfg.items():
            PythonOperator(
                task_id=f"migrate__{schema_name}__{table_name}",
                python_callable=_migrate_mysql_table_chunked,
                op_kwargs={
                    "schema": schema_name,
                    "table": table_name,
                    "filter_col": col,
                    "start_val": start,
                    "end_val": end,
                },
            )

