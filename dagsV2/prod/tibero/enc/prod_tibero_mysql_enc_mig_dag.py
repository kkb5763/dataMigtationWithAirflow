import jaydebeapi
import MySQLdb

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

# ==========================================================
# PROD: Tibero6 -> MySQL (암호화 컬럼 변환)
# ==========================================================
# sample_tibero_chunk_mig_dag.py 구간 루프 + sample_tibero_to_mysql_enc_transform 컬럼 변환
# TABLE_CONFIG: columns, enc_cols, range (col, start, end)

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
}

HTTP_UTIL_API_BASE = "http://127.0.0.1:8082"
HTTP_UTIL_API_MODE = "base64/enc"
HTTP_UTIL_API_TIMEOUT_SEC = 5.0

STEP_SIZE = 100_000
FETCH_SIZE = 2_000
INSERT_STRATEGY = "REPLACE"

TABLE_CONFIG: Dict[str, Dict[str, Any]] = {
    "MEMBER.MBR_BASE": {
        "columns": ["MBR_NO", "MBR_ID", "MBR_NM", "ENC_EMAIL"],
        "enc_cols": ["ENC_EMAIL"],
        "range": ("MBR_NO", 1, 50_000_000),
    },
    "MEMBER.MBR_CONTACT": {
        "columns": ["MBR_NO", "ENC_PHONE", "ENC_ADDR"],
        "enc_cols": ["ENC_PHONE", "ENC_ADDR"],
        "range": ("MBR_NO", 1, 30_000_000),
    },
    "ORDER.ORD_CUST": {
        "columns": ["ORD_NO", "CUST_NM", "ENC_EMAIL", "ENC_TEL"],
        "enc_cols": ["ENC_EMAIL", "ENC_TEL"],
        "range": ("ORD_NO", 1, 80_000_000),
    },
}


def _sql_literal(v: int) -> str:
    return str(int(v))


def _split_schema_table(full_name: str) -> Tuple[str, str]:
    schema, table = full_name.split(".", 1)
    return schema.upper(), table


def _mysql_cfg(schema: str) -> Dict[str, Any]:
    cfg = dict(TGT_MYSQL_BASE)
    cfg["db"] = MYSQL_DB_MAP.get(schema, schema.lower())
    return cfg


def _call_http_util_api(value: Optional[str]) -> Optional[str]:
    if not value:
        return value
    encoded = quote(str(value), safe="")
    url = f"{HTTP_UTIL_API_BASE.rstrip('/')}/{HTTP_UTIL_API_MODE.strip('/')}/{encoded}"
    try:
        req = Request(url, method="GET")
        with urlopen(req, timeout=HTTP_UTIL_API_TIMEOUT_SEC) as resp:
            body = resp.read()
        return body.decode("utf-8").strip()
    except (HTTPError, URLError, TimeoutError, ValueError) as e:
        print(f"HTTP util api fail: url={url} err={e}", flush=True)
        return f"ERR_HTTPUTIL_{value}"


def _transform_row(
    values: Tuple[Any, ...],
    col_names: List[str],
    enc_cols: List[str],
) -> Tuple[Any, ...]:
    if not enc_cols:
        return values
    out: List[Any] = list(values)
    for col in enc_cols:
        if col not in col_names:
            continue
        i = col_names.index(col)
        v = out[i]
        if v is None:
            continue
        if isinstance(v, (bytes, bytearray)):
            try:
                v = v.decode("utf-8")
            except Exception:
                v = v.decode("latin1", errors="ignore")
        if isinstance(v, str):
            out[i] = _call_http_util_api(v)
    return tuple(out)


def _worker(
    table: str,
    columns: List[str],
    enc_cols: List[str],
    range_col: str,
    start: int,
    end: int,
    **context: Any,
) -> None:
    schema, tbl = _split_schema_table(table)
    col_list = ", ".join(columns)
    mysql_cols = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))

    if INSERT_STRATEGY == "REPLACE":
        insert_sql = f"REPLACE INTO {tbl} ({mysql_cols}) VALUES ({placeholders})"
    elif INSERT_STRATEGY == "INSERT":
        insert_sql = f"INSERT INTO {tbl} ({mysql_cols}) VALUES ({placeholders})"
    elif INSERT_STRATEGY == "INSERT IGNORE":
        insert_sql = f"INSERT IGNORE INTO {tbl} ({mysql_cols}) VALUES ({placeholders})"
    else:
        raise ValueError(f"Unsupported INSERT_STRATEGY: {INSERT_STRATEGY}")

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
                f"SELECT {col_list} FROM {schema}.{tbl} "
                f"WHERE {range_col} >= {_sql_literal(current)} "
                f"AND {range_col} <= {_sql_literal(current_end)}"
            )
            print(f"[ENC-TIB][{table}] 실행 쿼리: {query}", flush=True)
            t_cur.execute(query)

            chunk_rows = 0
            while True:
                rows = t_cur.fetchmany(FETCH_SIZE)
                if not rows:
                    break
                batch = [_transform_row(tuple(r), columns, enc_cols) for r in rows]
                m_cur.executemany(insert_sql, batch)
                m_conn.commit()
                chunk_rows += len(batch)
                total += len(batch)

            print(
                f"[ENC-TIB][{table}] range {current}~{current_end} done "
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
    dag_id="prod_tibero_mysql_enc_mig_v1",
    default_args={"owner": "prod", "retries": 1, "retry_delay": timedelta(minutes=5)},
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    tags=["prod", "tibero", "mysql", "enc", "chunk"],
    max_active_tasks=4,
) as dag:
    for table_name, cfg in TABLE_CONFIG.items():
        range_col, start, end = cfg["range"]
        PythonOperator(
            task_id=f"migrate_{table_name.replace('.', '_')}",
            python_callable=_worker,
            pool="prod_tibero_enc_mig",
            pool_slots=1,
            op_kwargs={
                "table": table_name,
                "columns": list(cfg["columns"]),
                "enc_cols": list(cfg.get("enc_cols", [])),
                "range_col": range_col,
                "start": start,
                "end": end,
            },
        )
