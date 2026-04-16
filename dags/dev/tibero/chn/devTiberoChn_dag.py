import jaydebeapi
import MySQLdb

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from typing import Any, Dict, Tuple, Union

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

# MySQL 타겟은 스키마별 DB로 적재하는 것을 기본으로 가정합니다.
MYSQL_BASE_CONFIG: Dict[str, Any] = {
    "host": "10.10.1.20",
    "user": "root",
    "passwd": "tgt_mysql_pass456@",
}

# Tibero schema -> MySQL db 매핑 (기본: 동일 이름)
TARGET_DB_MAP: Dict[str, str] = {
    # "CHN_SCHEMA1": "chn_schema1",
}


# ==========================================================
# 2. 스키마별 TABLE_CONFIG (범위 지정 필수)
# ==========================================================
# CHN은 데이터가 커서 "전체 조회(None)"는 금지합니다.
# 구조: "테이블명": ("기준컬럼", 시작값, 종료값)
# - 시작/종료값에 None을 넣으면 예외 발생(강제)
TableConfig = Dict[str, Tuple[str, Union[int, str], Union[int, str]]]

SCHEMA_TABLE_CONFIGS: Dict[str, TableConfig] = {
    # 아래 5개 스키마는 템플릿입니다. 실제 CHN 스키마명으로 변경하세요.
    "chn_schema_01": {
        "chn_table_01": ("id", 1, 100000),
    },
    "chn_schema_02": {
        "chn_table_01": ("id", 1, 100000),
    },
    "chn_schema_03": {
        "chn_table_01": ("id", 1, 100000),
    },
    "chn_schema_04": {
        "chn_table_01": ("id", 1, 100000),
    },
    "chn_schema_05": {
        "chn_table_01": ("id", 1, 100000),
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
    start_val: Union[int, str],
    end_val: Union[int, str],
    **context: Any,
) -> None:
    """
    CHN: 범위 지정 필수(start/end). (None 금지)
    Tibero(schema.table) -> MySQL(db=schema).table
    """
    if start_val is None or end_val is None:
        raise ValueError(f"[{schema}.{table}] start/end must be set (None not allowed)")

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

        query = (
            f"SELECT * FROM {schema}.{table} "
            f"WHERE {filter_col} >= {_sql_literal(start_val)} AND {filter_col} <= {_sql_literal(end_val)}"
        )
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
    dag_id="dev_tibero_to_chn_v1",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    tags=["migration", "tibero", "mysql", "chn"],
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

