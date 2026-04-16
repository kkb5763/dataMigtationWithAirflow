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

MYSQL_CONFIG: Dict[str, Any] = {
    "host": "10.10.1.20",
    "user": "root",
    "passwd": "tgt_mysql_pass456@",
    "db": "cxm",
}

# ==========================================================
# 2. 이관 대상 테이블 리스트 및 범위 설정 (CMBS 스타일)
# 구조: "테이블명": ("기준컬럼", 시작값, 종료값)
# - 시작/종료값에 None을 넣으면 해당 조건은 제외됩니다.
# - Airflow 매크로(예: {{ ds }})를 문자열로 넣으면 실행 시 자동 치환됩니다.
# ==========================================================
TABLE_CONFIG: Dict[str, Tuple[str, Optional[Union[int, str]], Optional[Union[int, str]]]] = {
    # 아래 10개는 템플릿입니다. 실제 cxm 대상 테이블/컬럼으로 변경하세요.
    "cxm_table_01": ("id", 1, 100000),
    "cxm_table_02": ("id", 1, 100000),
    "cxm_table_03": ("id", None, None),
    "cxm_table_04": ("updated_dt", "2026-01-01", None),
    "cxm_table_05": ("reg_dt", "{{ ds }}", "{{ next_ds }}"),
    "cxm_table_06": ("id", None, None),
    "cxm_table_07": ("id", 1, 50000),
    "cxm_table_08": ("id", 1, 50000),
    "cxm_table_09": ("id", None, None),
    "cxm_table_10": ("id", None, None),
}

FETCH_SIZE = 2000
INSERT_STRATEGY = "REPLACE"  # "REPLACE" | "INSERT" | "INSERT IGNORE"


def _sql_literal(v: Union[int, str]) -> str:
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    return str(v)


def _etl_worker(
    table: str,
    filter_col: str,
    start_val: Optional[Union[int, str]],
    end_val: Optional[Union[int, str]],
    **context: Any,
) -> None:
    """
    Tibero에서 데이터를 추출하여 MySQL로 (기본) REPLACE INTO 수행.
    - start_val/end_val이 None이면 해당 조건은 제외
    """
    t_conn = None
    m_conn = None
    try:
        # MySQL 연결
        m_conn = MySQLdb.connect(**MYSQL_CONFIG)
        m_cur = m_conn.cursor()

        # Tibero 연결 (JDBC)
        url = f"jdbc:tibero:thin:@{TIBERO_CONFIG['host']}:{TIBERO_CONFIG['port']}:{TIBERO_CONFIG['sid']}"
        t_conn = jaydebeapi.connect(
            TIBERO_CONFIG["driver"],
            url,
            [TIBERO_CONFIG["user"], TIBERO_CONFIG["pass"]],
            TIBERO_CONFIG["jdbc_jar"],
        )
        t_cur = t_conn.cursor()

        # 쿼리 동적 생성
        query = f"SELECT * FROM {table}"
        conditions = []

        if start_val is not None:
            conditions.append(f"{filter_col} >= {_sql_literal(start_val)}")
        if end_val is not None:
            conditions.append(f"{filter_col} <= {_sql_literal(end_val)}")

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        print(f"[{table}] 실행 쿼리: {query}", flush=True)

        # 실행 및 적재
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
            print(f" >>> {table}: {total_rows} rows migrated...", flush=True)

    except Exception as e:
        print(f"!!! [{table}] 이관 실패: {str(e)}", flush=True)
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
    dag_id="dev_tibero_to_cxm_v1",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    tags=["migration", "tibero", "mysql", "cxm"],
    max_active_tasks=1,
) as dag:
    for table_name, (col, start, end) in TABLE_CONFIG.items():
        PythonOperator(
            task_id=f"migrate_{table_name}",
            python_callable=_etl_worker,
            op_kwargs={
                "table": table_name,
                "filter_col": col,
                "start_val": start,
                "end_val": end,
            },
        )

