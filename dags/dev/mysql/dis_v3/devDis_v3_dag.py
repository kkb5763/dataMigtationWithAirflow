import MySQLdb
import MySQLdb.cursors

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple, Union

# ==========================================================
# 1. 환경 설정 (Source & Target 접속 정보)
# ==========================================================
# CMBS 양식과 최대한 동일하게 유지하되, dis는 스키마(DB)가 여러 개라
# "스키마별로 TABLE_CONFIG를 하나씩" 갖는 형태만 추가했습니다.
SRC_MYSQL_BASE_CONFIG: Dict[str, Any] = {
    "host": "10.10.1.10",
    "user": "root",
    "passwd": "src_password123!",
    "charset": "utf8mb4",
}

TGT_MYSQL_BASE_CONFIG: Dict[str, Any] = {
    "host": "10.10.1.20",
    "user": "root",
    "passwd": "tgt_password456@",
    "charset": "utf8mb4",
}


# ==========================================================
# 2. 이관 대상 테이블 설정 (CMBS와 동일한 튜플 형태)
# ==========================================================
# TABLE_CONFIG 규칙:
# (filter_col, start_val, end_val)
# - start_val 또는 end_val이 `None`이면 WHERE 조건에서 해당 부분을 빼서 "전체 조회"합니다.
# - start_val/end_val에 문자열 템플릿(`{{ ds }}` 등)을 넣으면 Airflow에서 런타임 렌더링됩니다.
TableConfig = Dict[str, Tuple[str, Optional[Union[int, str]], Optional[Union[int, str]]]]

SCHEMA_TABLE_CONFIGS: Dict[str, TableConfig] = {
    # 예시) dis 스키마
    "dis": {
        "code_definition": ("code", None, None),
        # "system_log": ("reg_dt", "{{ ds }}", "{{ next_ds }}"),
    },
    # 예시) bbi 스키마
    "bbi": {
        "code_definition": ("code", None, None),
    },
    # 예시) itemdb 스키마
    "itemdb": {
        "code_definition": ("code", None, None),
    },
    # 필요하면 여기에 memberdb/cms/batct/cgidis 등 추가
}


# ==========================================================
# 3. 유틸 함수 (CMBS와 동일)
# ==========================================================
def _quote_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _sql_literal(v: Union[int, str]) -> str:
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    return str(v)


def _mk_cfg(base: Dict[str, Any], db_name: str) -> Dict[str, Any]:
    cfg = dict(base)
    cfg["db"] = db_name
    return cfg


# ==========================================================
# 4. 이관 실행 핵심 함수 (CMBS와 동일 로직)
# ==========================================================
def _mysql_to_mysql_etl(
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
        src_cfg = _mk_cfg(SRC_MYSQL_BASE_CONFIG, schema)
        tgt_cfg = _mk_cfg(TGT_MYSQL_BASE_CONFIG, schema)

        src_conn = MySQLdb.connect(**src_cfg)
        tgt_conn = MySQLdb.connect(**tgt_cfg)

        src_cur = src_conn.cursor(MySQLdb.cursors.SSCursor)
        tgt_cur = tgt_conn.cursor()

        query = f"SELECT * FROM {_quote_ident(table)}"
        conditions = []

        if start_val is not None:
            conditions.append(f"{_quote_ident(filter_col)} >= {_sql_literal(start_val)}")
        if end_val is not None:
            conditions.append(f"{_quote_ident(filter_col)} <= {_sql_literal(end_val)}")

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        print(f"[{schema}.{table}] 소스 쿼리 실행: {query}")
        src_cur.execute(query)

        col_count = len(src_cur.description)
        placeholders = ", ".join(["%s"] * col_count)
        insert_sql = f"REPLACE INTO {_quote_ident(table)} VALUES ({placeholders})"

        total_rows = 0
        while True:
            rows = src_cur.fetchmany(5000)
            if not rows:
                break

            tgt_cur.executemany(insert_sql, rows)
            tgt_conn.commit()
            total_rows += len(rows)
            print(f" >>> {schema}.{table}: {total_rows} rows migrated...", flush=True)

    except Exception as e:
        print(f"!!! [{schema}.{table}] 이관 중 오류 발생: {str(e)}")
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


# ==========================================================
# 5. DAG 정의
# ==========================================================
with DAG(
    dag_id="dev_dis_v3_v1",
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["migration", "mysql", "dis_v3"],
    max_active_tasks=1,
) as dag:
    for schema_name, table_cfg in SCHEMA_TABLE_CONFIGS.items():
        for table_name, (col, start, end) in table_cfg.items():
            PythonOperator(
                task_id=f"migrate__{schema_name}__{table_name}",
                python_callable=_mysql_to_mysql_etl,
                op_kwargs={
                    "schema": schema_name,
                    "table": table_name,
                    "filter_col": col,
                    "start_val": start,
                    "end_val": end,
                },
            )

