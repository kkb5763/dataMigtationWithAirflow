import MySQLdb
import MySQLdb.cursors

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from typing import Any, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

# ==========================================================
# PROD: MySQL a1(암호문) -> a2(평문) -> a3(SAFEDB) UPDATE
# ==========================================================
# 테이블: rowid, a1, a2, a3
#   rowid : 진행 확인용 번호
#   a1    : 미리 INSERT 한 암호문 (읽기만)
#   a2    : 8082 복호화 API 결과
#   a3    : 8084 SAFEDB 암호화 API 결과 (a2 평문 사용)
#
# 태스크 1개 = rowid 1000개 구간 (dec_rowid_1_1000, dec_rowid_1001_2000, ...)
# Pool (Admin): prod_dec_data / slots=1

TABLE_NAME = "enc_target"

ROWID_START = 1
ROWID_END = 1000
BATCH_SIZE = 1000

MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "your_password"
MYSQL_DB = "test_db"

API_DECRYPT = "http://127.0.0.1:8082/dec"
API_SAFEDB_ENC = "http://127.0.0.1:8084/enc"
HTTP_TIMEOUT_SEC = 5.0


def _to_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="ignore")
    return str(value)


def _http_get(base_url: str, text: str) -> str:
    encoded = quote(text, safe="")
    if base_url.endswith("/"):
        url = f"{base_url}{encoded}"
    else:
        url = f"{base_url}/{encoded}"
    req = Request(url, method="GET")
    with urlopen(req, timeout=HTTP_TIMEOUT_SEC) as resp:
        return resp.read().decode("utf-8").strip()


def _api_decrypt(a1: str) -> str:
    try:
        return _http_get(API_DECRYPT, a1)
    except (HTTPError, URLError, TimeoutError, ValueError) as e:
        print(f"  [복호화 실패] {e}", flush=True)
        return f"ERR_DEC_{a1[:20]}"


def _api_safedb_enc(plain: str) -> str:
    try:
        return _http_get(API_SAFEDB_ENC, plain)
    except (HTTPError, URLError, TimeoutError, ValueError) as e:
        print(f"  [SAFEDB 암호화 실패] {e}", flush=True)
        return f"ERR_ENC_{plain[:20]}"


def _dec_batch(batch_start: int, batch_end: int, **context: Any) -> None:
    print(f"\n=== rowid {batch_start} ~ {batch_end} ===", flush=True)

    conn = MySQLdb.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        passwd=MYSQL_PASSWORD,
        db=MYSQL_DB,
        charset="utf8mb4",
        cursorclass=MySQLdb.cursors.DictCursor,
    )
    cur = conn.cursor()

    select_sql = (
        f"SELECT `rowid`, `a1` FROM `{TABLE_NAME}` "
        f"WHERE `rowid` >= %s AND `rowid` <= %s "
        f"AND `a1` IS NOT NULL AND `a1` != ''"
    )
    cur.execute(select_sql, (batch_start, batch_end))
    rows = cur.fetchall()

    update_sql = f"UPDATE `{TABLE_NAME}` SET `a2` = %s, `a3` = %s WHERE `rowid` = %s"
    done = 0

    for row in rows:
        rowid = row["rowid"]
        a1 = _to_str(row["a1"])
        if not a1:
            continue

        a2 = _api_decrypt(a1)
        a3 = _api_safedb_enc(a2)

        if done < 3:
            print(
                f"  rowid={rowid}  a1={a1[:20]}...  ->  a2={str(a2)[:30]}  ->  a3={str(a3)[:30]}",
                flush=True,
            )

        cur.execute(update_sql, (a2, a3, rowid))
        done += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"  완료: {done}건 (rowid {batch_start}~{batch_end})", flush=True)


def _iter_rowid_batches() -> List[tuple]:
    batches: List[tuple] = []
    batch_start = ROWID_START
    while batch_start <= ROWID_END:
        batch_end = min(batch_start + BATCH_SIZE - 1, ROWID_END)
        batches.append((batch_start, batch_end))
        batch_start += BATCH_SIZE
    return batches


with DAG(
    dag_id="prod_dec_data_v1",
    default_args={"owner": "prod", "retries": 1, "retry_delay": timedelta(minutes=5)},
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    tags=["prod", "mysql", "dec"],
    max_active_runs=1,
    max_active_tasks=1,
) as dag:
    prev_task = None
    for batch_start, batch_end in _iter_rowid_batches():
        task = PythonOperator(
            task_id=f"dec_rowid_{batch_start}_{batch_end}",
            python_callable=_dec_batch,
            op_kwargs={"batch_start": batch_start, "batch_end": batch_end},
            pool="prod_dec_data",
            pool_slots=1,
        )
        if prev_task is not None:
            prev_task >> task
        prev_task = task
