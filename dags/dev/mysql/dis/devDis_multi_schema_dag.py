import MySQLdb
import MySQLdb.cursors

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple, Union

# ==========================================================
# 1. 환경 설정 (Source & Target 접속 정보)
# ==========================================================
# dis는 스키마(DB)가 여러 개이지만, DAG는 1개로 구성합니다.
# 아래 HOST/계정은 공통으로 두고, db 값만 schema별로 바꿔 연결합니다.
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
# 2. 스키마 목록 및 테이블 설정
# ==========================================================
# 요청하신 스키마명:
# - dis / batct / bbi / itemdb / memberdb / cms
#
# TABLE_CONFIG 규칙:
# (filter_col, start_val, end_val)
# - start_val 또는 end_val이 `None`이면 WHERE 조건에서 해당 부분을 빼서 "전체 조회"합니다.
# - start_val/end_val에 문자열 템플릿(`{{ ds }}` 등)을 넣으면 Airflow에서 런타임 렌더링됩니다.
SchemaTableConfig = Dict[str, Tuple[str, Optional[Union[int, str]], Optional[Union[int, str]]]]

SCHEMA_TABLE_CONFIGS: Dict[str, SchemaTableConfig] = {
    # 예시(스키마별로 원하는 테이블/조건을 채우세요)
    "dis": {
        "code_definition": ("code", None, None),
    },
    "batct": {
        "code_definition": ("code", None, None),
    },
    "bbi": {
        "code_definition": ("code", None, None),
    },
    "itemdb": {
        "code_definition": ("code", None, None),
    },
    "memberdb": {
        "code_definition": ("code", None, None),
    },
    "cms": {
        "code_definition": ("code", None, None),
    },
}


# ==========================================================
# 3. 유틸 함수
# ==========================================================
def _quote_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _sql_literal(v: Union[int, str]) -> str:
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    return str(v)


def _mk_src_cfg(db_name: str) -> Dict[str, Any]:
    cfg = dict(SRC_MYSQL_BASE_CONFIG)
    cfg["db"] = db_name
    return cfg


def _mk_tgt_cfg(db_name: str) -> Dict[str, Any]:
    cfg = dict(TGT_MYSQL_BASE_CONFIG)
    cfg["db"] = db_name
    return cfg


# ==========================================================
# 4. 이관 실행 함수
# ==========================================================
def _mysql_to_mysql_etl(
    src_cfg: Dict[str, Any],
    tgt_cfg: Dict[str, Any],
    table: str,
    filter_col: str,
    start_val: Optional[Union[int, str]],
    end_val: Optional[Union[int, str]],
    **context: Any,
) -> None:
    src_conn = None
    tgt_conn = None
    try:
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

        print(f"[{src_cfg['db']}.{table}] 소스 쿼리 실행: {query}")
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
            print(f" >>> {src_cfg['db']}.{table}: {total_rows} rows migrated...", flush=True)

    except Exception as e:
        print(f"!!! [{src_cfg['db']}.{table}] 이관 중 오류 발생: {str(e)}")
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


def _migrate_one_schema(schema_name: str, **context: Any) -> None:
    table_cfg = SCHEMA_TABLE_CONFIGS.get(schema_name)
    if not table_cfg:
        print(f"[SKIP] no TABLE_CONFIG for schema={schema_name}")
        return

    src_cfg = _mk_src_cfg(schema_name)
    tgt_cfg = _mk_tgt_cfg(schema_name)

    for table_name, (col, start, end) in table_cfg.items():
        _mysql_to_mysql_etl(
            src_cfg=src_cfg,
            tgt_cfg=tgt_cfg,
            table=table_name,
            filter_col=col,
            start_val=start,
            end_val=end,
            **context,
        )


# ==========================================================
# 5. DAG 정의 (단일 DAG, 스키마별 task)
# ==========================================================
with DAG(
    dag_id="dev_dis_multi_schema_v1",
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["migration", "mysql", "dis"],
    # 스키마별 task가 동시 실행되면 소스/타겟 부하가 커질 수 있어 1로 두는 것을 권장합니다.
    max_active_tasks=1,
) as dag:
    for schema in SCHEMA_TABLE_CONFIGS.keys():
        PythonOperator(
            task_id=f"migrate_schema__{schema}",
            python_callable=_migrate_one_schema,
            op_kwargs={"schema_name": schema},
        )

