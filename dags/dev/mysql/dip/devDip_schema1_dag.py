import MySQLdb
import MySQLdb.cursors

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple, Union

# ==========================================================
# 1. 환경 설정 (Source & Target 접속 정보)
# ==========================================================
# DIP 스키마(=DB) 3개는 "DAG 3개"로 분리 실행합니다.
# 이 파일은 `sl` 스키마용입니다.
SRC_MYSQL_CONFIG: Dict[str, Any] = {
    "host": "10.10.1.10",
    "user": "root",
    "passwd": "src_password123!",
    "db": "sl",
    "charset": "utf8mb4",
}

TGT_MYSQL_CONFIG: Dict[str, Any] = {
    "host": "10.10.1.20",
    "user": "root",
    "passwd": "tgt_password456@",
    "db": "sl",
    "charset": "utf8mb4",
}


# ==========================================================
# 2. 이관 대상 테이블 (Include-list)
# ==========================================================
# (filter_col, start_val, end_val)
# - start_val 또는 end_val이 `None`이면 WHERE 조건에서 해당 부분을 빼서 "전체 조회"합니다.
# - start_val/end_val에 문자열 템플릿(`{{ ds }}` 등)을 넣으면 Airflow에서 런타임 렌더링됩니다.
TABLE_CONFIG: Dict[str, Tuple[str, Optional[Union[int, str]], Optional[Union[int, str]]]] = {
    "code_definition": ("code", None, None),  # 예시: 전체 조회
}


def _quote_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _sql_literal(v: Union[int, str]) -> str:
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    return str(v)


def _mysql_to_mysql_etl(
    table: str,
    filter_col: str,
    start_val: Optional[Union[int, str]],
    end_val: Optional[Union[int, str]],
    **context: Any,
) -> None:
    src_conn = None
    tgt_conn = None
    try:
        src_conn = MySQLdb.connect(**SRC_MYSQL_CONFIG)
        tgt_conn = MySQLdb.connect(**TGT_MYSQL_CONFIG)

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

        print(f"[{table}] 소스 쿼리 실행: {query}")
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
            print(f" >>> {table}: {total_rows} rows migrated...", flush=True)

    except Exception as e:
        print(f"!!! [{table}] 이관 중 오류 발생: {str(e)}")
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


with DAG(
    dag_id="dev_dip_sl_v1",
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["migration", "mysql", "dip", "sl"],
) as dag:
    for table_name, (col, start, end) in TABLE_CONFIG.items():
        PythonOperator(
            task_id=f"migrate_{table_name}",
            python_callable=_mysql_to_mysql_etl,
            op_kwargs={
                "table": table_name,
                "filter_col": col,
                "start_val": start,
                "end_val": end,
            },
        )

