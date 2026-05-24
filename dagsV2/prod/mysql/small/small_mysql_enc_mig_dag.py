import MySQLdb
import MySQLdb.cursors

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from typing import Any, Dict, List

# ==========================================================
# SMALL: MySQL -> MySQL (동일 스키마·동일 테이블 구조) 단순 적재
# ==========================================================
# - 소스/타겟 DB·테이블 구조가 같다고 가정 → SELECT * 그대로 타겟에 적재
# - 설정은 TABLE_LIST(테이블명)만 추가하면 됨
# - 암호화 API·컬럼 지정·PK 구간 분할 없음 (전체 테이블 스캔)

SRC_MYSQL: Dict[str, Any] = {
    "host": "10.10.1.10",
    "port": 3306,
    "user": "root",
    "passwd": "src_mysql_pass123!",
    "db": "member_db",
    "charset": "utf8mb4",
}

TGT_MYSQL: Dict[str, Any] = {
    "host": "10.10.1.20",
    "port": 3306,
    "user": "root",
    "passwd": "tgt_mysql_pass456@",
    "db": "member_db",
    "charset": "utf8mb4",
}

CHUNK_SIZE = 5000
INSERT_STRATEGY = "REPLACE"  # REPLACE | INSERT | INSERT IGNORE

# 이관할 테이블명만 나열 (소스·타겟 동일 DB 내 동일 이름·동일 컬럼 구조)
TABLE_LIST: List[str] = [
    "mbr_base",
    "mbr_sample",
    # "table_03",
    # "table_04",
    # "table_05",
]


def _quote_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _mysql_copy_table(table: str, **context: Any) -> None:
    src_conn = None
    tgt_conn = None
    try:
        src_conn = MySQLdb.connect(**SRC_MYSQL)
        tgt_conn = MySQLdb.connect(**TGT_MYSQL)

        src_cur = src_conn.cursor(MySQLdb.cursors.SSCursor)
        tgt_cur = tgt_conn.cursor()

        query = f"SELECT * FROM {_quote_ident(table)}"
        print(f"[SMALL][{table}] {query}", flush=True)
        src_cur.execute(query)

        col_count = len(src_cur.description)
        placeholders = ", ".join(["%s"] * col_count)

        if INSERT_STRATEGY == "REPLACE":
            insert_sql = f"REPLACE INTO {_quote_ident(table)} VALUES ({placeholders})"
        elif INSERT_STRATEGY == "INSERT":
            insert_sql = f"INSERT INTO {_quote_ident(table)} VALUES ({placeholders})"
        elif INSERT_STRATEGY == "INSERT IGNORE":
            insert_sql = f"INSERT IGNORE INTO {_quote_ident(table)} VALUES ({placeholders})"
        else:
            raise ValueError(f"Unsupported INSERT_STRATEGY: {INSERT_STRATEGY}")

        total = 0
        while True:
            rows = src_cur.fetchmany(CHUNK_SIZE)
            if not rows:
                break
            tgt_cur.executemany(insert_sql, rows)
            tgt_conn.commit()
            total += len(rows)
            print(f" >>> [SMALL][{table}] {total} rows...", flush=True)

        print(f"[SMALL][{table}] finished total={total}", flush=True)

    except Exception as e:
        print(f"!!! [SMALL][{table}] failed: {e}", flush=True)
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


default_args = {
    "owner": "small",
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="small_mysql_copy_mig_v1",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    tags=["small", "mysql", "copy", "migration"],
    max_active_tasks=4,
) as dag:
    for table_name in TABLE_LIST:
        PythonOperator(
            task_id=f"migrate_{table_name}",
            python_callable=_mysql_copy_table,
            pool="small_mysql_copy",
            pool_slots=1,
            op_kwargs={"table": table_name},
        )
