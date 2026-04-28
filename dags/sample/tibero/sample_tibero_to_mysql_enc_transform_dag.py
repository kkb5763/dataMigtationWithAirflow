import subprocess
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

import jaydebeapi
import MySQLdb

from airflow import DAG
from airflow.operators.python import PythonOperator

# ==========================================================
# Tibero -> MySQL (Sample) : 범위 청킹 + 컬럼 변환(HTTP 유틸 API)
# ==========================================================
# 목적:
# - Tibero 데이터를 범위 조건으로 나눠 이행 (대용량 대비)
# - 특정 컬럼에 대해 "변환/암호화/인코딩" 같은 처리를 샘플로 제공
#   (예: 로컬 Java HTTP 유틸 API: /base64/enc/<plain>)
#
# 전제:
# - Airflow 워커에서 Tibero JDBC(jaydebeapi) 사용 가능해야 함
# - MySQL(mysqlclient) 설치되어 있어야 함
# - (선택) HTTP 유틸 API는 Airflow 워커에서 접근 가능해야 함


TIBERO_CONFIG: Dict[str, Any] = {
    "host": "10.10.4.10",
    "port": 8629,
    "sid": "tibero",
    "user": "sys",
    "pass": "tibero_src_pwd",
    "jdbc_jar": "/data/airflow/lib/thr_jdbc.jar",
    "driver": "com.tmax.tibero.jdbc.TbDriver",
    # schema 지정이 필요하면 table을 "SCHEMA.TABLE"로 넣으세요.
}

MYSQL_CONFIG: Dict[str, Any] = {
    "host": "10.10.1.20",
    "port": 3306,
    "user": "root",
    "passwd": "tgt_mysql_pass456@",
    "db": "target_db",
    "charset": "utf8mb4",
}

# ==========================================================
# HTTP 유틸 API (예: SimpleUtilHttpApi)
# ==========================================================
USE_HTTP_UTIL_API = True
HTTP_UTIL_API_BASE = "http://127.0.0.1:8082"
HTTP_UTIL_API_MODE = "base64/enc"
HTTP_UTIL_API_TIMEOUT_SEC = 5.0


def call_http_util_api(value: Optional[str]) -> Optional[str]:
    if not value:
        return value
    encoded = quote(str(value), safe="")
    url = f"{HTTP_UTIL_API_BASE}/{HTTP_UTIL_API_MODE}/{encoded}"
    try:
        req = Request(url, method="GET")
        with urlopen(req, timeout=HTTP_UTIL_API_TIMEOUT_SEC) as resp:
            body = resp.read()
        return body.decode("utf-8").strip()
    except (HTTPError, URLError, TimeoutError, ValueError) as e:
        print(f"HTTP util api fail: url={url} err={e}", flush=True)
        return f"ERR_HTTPUTIL_{value}"


# ==========================================================
# 테이블별 설정
# - columns: SELECT/INSERT 순서 고정
# - transform_cols: 변환 적용 컬럼(문자열 컬럼 기준)
# - range: (filter_col, start, end)  (필수)
# ==========================================================
TABLE_CONFIG: Dict[str, Dict[str, Any]] = {
    # 예시)
    # "SCHEMA.TABLE": {
    #     "columns": ["ID", "NAME", "EMAIL"],
    #     "transform_cols": ["EMAIL"],
    #     "range": ("ID", 1, 1_000_000),
    # },
    "SAMPLE_TABLE": {
        "columns": ["ID", "PLAIN_COL"],
        "transform_cols": ["PLAIN_COL"],
        "range": ("ID", 1, 100000),
    }
}

STEP_SIZE = 100_000
FETCH_SIZE = 2_000
INSERT_STRATEGY = "REPLACE"  # "REPLACE" | "INSERT" | "INSERT IGNORE"


def _sql_literal(v: Union[int, str]) -> str:
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    return str(int(v))


def _transform_row(values: Tuple[Any, ...], col_names: List[str], transform_cols: List[str]) -> Tuple[Any, ...]:
    if not USE_HTTP_UTIL_API or not transform_cols:
        return values

    idxs = [col_names.index(c) for c in transform_cols if c in col_names]
    if not idxs:
        return values

    out: List[Any] = list(values)
    for i in idxs:
        v = out[i]
        if v is None:
            continue
        if isinstance(v, (bytes, bytearray)):
            try:
                v = v.decode("utf-8")
            except Exception:
                v = v.decode("latin1", errors="ignore")
        if isinstance(v, str):
            out[i] = call_http_util_api(v)
    return tuple(out)


def _worker(
    table: str,
    columns: List[str],
    transform_cols: List[str],
    range_col: str,
    start: Union[int, str],
    end: Union[int, str],
    **context: Any,
) -> None:
    t_conn = None
    m_conn = None
    try:
        m_conn = MySQLdb.connect(**MYSQL_CONFIG)
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
        end_i = int(end)
        total = 0

        col_list = ", ".join(columns)
        while current <= end_i:
            current_end = min(current + STEP_SIZE - 1, end_i)
            query = (
                f"SELECT {col_list} FROM {table} "
                f"WHERE {range_col} >= {_sql_literal(current)} AND {range_col} <= {_sql_literal(current_end)}"
            )
            print(f"[{table}] 실행 쿼리: {query}", flush=True)
            t_cur.execute(query)

            placeholders = ", ".join(["%s"] * len(columns))
            if INSERT_STRATEGY == "REPLACE":
                insert_sql = f"REPLACE INTO {table} ({col_list}) VALUES ({placeholders})"
            elif INSERT_STRATEGY == "INSERT":
                insert_sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"
            elif INSERT_STRATEGY == "INSERT IGNORE":
                insert_sql = f"INSERT IGNORE INTO {table} ({col_list}) VALUES ({placeholders})"
            else:
                raise ValueError(f"Unsupported INSERT_STRATEGY: {INSERT_STRATEGY}")

            chunk_rows = 0
            while True:
                rows = t_cur.fetchmany(FETCH_SIZE)
                if not rows:
                    break
                transformed = [_transform_row(tuple(r), columns, transform_cols) for r in rows]
                m_cur.executemany(insert_sql, transformed)
                m_conn.commit()
                chunk_rows += len(transformed)
                total += len(transformed)

            print(
                f"[{table}] range {current}~{current_end} done (rows={chunk_rows}, total={total})",
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


default_args = {
    "owner": "sample",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="sample_tibero_to_mysql_enc_transform_v1",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    tags=["sample", "tibero", "mysql", "chunk", "transform"],
    max_active_tasks=1,
) as dag:
    for table_name, cfg in TABLE_CONFIG.items():
        range_col, start, end = cfg["range"]
        PythonOperator(
            task_id=f"migrate_{table_name.replace('.', '_')}",
            python_callable=_worker,
            op_kwargs={
                "table": table_name,
                "columns": list(cfg["columns"]),
                "transform_cols": list(cfg.get("transform_cols", [])),
                "range_col": range_col,
                "start": start,
                "end": end,
            },
        )

