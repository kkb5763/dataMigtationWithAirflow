import jaydebeapi
import MySQLdb

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple, Union

# ==========================================================
# 1. 환경 설정 (DB 접속 정보)
# ==========================================================
TIBERO_CONFIG: Dict[str, Any] = {
    "host": "10.10.4.10",
    "port": 8629,
    "sid": "tibero",
    "user": "sys",
    "pass": "tibero_src_pwd",
    "jdbc_jar": "/data/airflow/lib/thr_jdbc.jar",
    "driver": "com.tmax.tibero.jdbc.TbDriver",
}

# MySQL은 스키마(DB) 4개를 각각 다른 DB로 적재하는 형태를 가정합니다.
# (필요하면 `target_db_map`으로 매핑 변경 가능)
MYSQL_BASE_CONFIG: Dict[str, Any] = {
    "host": "10.10.1.20",
    "user": "root",
    "passwd": "tgt_mysql_pass456@",
}

# Tibero schema -> MySQL db 매핑 (기본: 동일 이름)
TARGET_DB_MAP: Dict[str, str] = {
    # 예시:
    # "DTG_SCHEMA1": "dtg_schema1",
}


# ==========================================================
# 2. 스키마별 TABLE_CONFIG (CXM/CMBS 스타일 유지)
# ==========================================================
TableConfig = Dict[str, Tuple[str, Optional[Union[int, str]], Optional[Union[int, str]]]]

SCHEMA_TABLE_CONFIGS: Dict[str, TableConfig] = {
    # 아래 4개 스키마는 템플릿입니다. 실제 DTG 스키마명으로 변경하세요.
    "dtg_schema_01": {
        "dtg_table_01": ("id", None, None),
    },
    "dtg_schema_02": {
        "dtg_table_01": ("id", None, None),
    },
    "dtg_schema_03": {
        "dtg_table_01": ("id", None, None),
    },
    "dtg_schema_04": {
        "dtg_table_01": ("id", None, None),
    },
}

FETCH_SIZE = 2000
INSERT_STRATEGY = "REPLACE"  # "REPLACE" | "INSERT" | "INSERT IGNORE"


def _sql_literal(v: Union[int, str]) -> str:
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    return str(v)


def _mk_mysql_cfg(schema: str) -> Dict[str, Any]:
    cfg = dict(MYSQL_BASE_CONFIG)
    cfg["db"] = TARGET_DB_MAP.get(schema, schema)
    return cfg


def _etl_worker(
    schema: str,
    table: str,
    filter_col: str,
    start_val: Optional[Union[int, str]],
    end_val: Optional[Union[int, str]],
    **context: Any,
) -> None:
    """
    Tibero(schema.table) -> MySQL(db=schema).table
    - start/end가 None이면 조건 제외
    """
    t_conn = None
    m_conn = None
    try:
        mysql_cfg = _mk_mysql_cfg(schema)
        m_conn = MySQLdb.connect(**mysql_cfg)
        m_cur = m_conn.cursor()

        url = f"jdbc:tibero:thin:@{TIBERO_CONFIG['host']}:{TIBERO_CONFIG['port']}:{TIBERO_CONFIG['sid']}"
        t_conn = jaydebeapi.connect(
            TIBERO_CONFIG["driver"],
            url,
            [TIBERO_CONFIG["user"], TIBERO_CONFIG["pass"]],
            TIBERO_CONFIG["jdbc_jar"],
        )
        t_cur = t_conn.cursor()

        query = f"SELECT * FROM {schema}.{table}"
        conditions = []
        if start_val is not None:
            conditions.append(f"{filter_col} >= {_sql_literal(start_val)}")
        if end_val is not None:
            conditions.append(f"{filter_col} <= {_sql_literal(end_val)}")
        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        print(f"[{schema}.{table}] 실행 쿼리: {query}", flush=True)
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

        total_rows = 0
        while True:
            rows = t_cur.fetchmany(FETCH_SIZE)
            if not rows:
                break
            m_cur.executemany(insert_sql, rows)
            m_conn.commit()
            total_rows += len(rows)
            print(f" >>> {schema}.{table}: {total_rows} rows migrated...", flush=True)

    except Exception as e:
        print(f"!!! [{schema}.{table}] 이관 실패: {str(e)}", flush=True)
        raise
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
    "owner": "data_architect",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dev_tibero_to_dtg_v1",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    tags=["migration", "tibero", "mysql", "dtg"],
    max_active_tasks=1,
) as dag:
    for schema_name, table_cfg in SCHEMA_TABLE_CONFIGS.items():
        for table_name, (col, start, end) in table_cfg.items():
            PythonOperator(
                task_id=f"migrate__{schema_name}__{table_name}",
                python_callable=_etl_worker,
                op_kwargs={
                    "schema": schema_name,
                    "table": table_name,
                    "filter_col": col,
                    "start_val": start,
                    "end_val": end,
                },
            )

