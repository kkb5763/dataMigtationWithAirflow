import MySQLdb
import MySQLdb.cursors

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from typing import Any, Dict, List

# ==========================================================
# SMALL-2: MySQL -> MySQL 동일 구조 단순 적재 (테이블 그룹 2)
# ==========================================================

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
INSERT_STRATEGY = "REPLACE"

TABLE_LIST: List[str] = [
    "mbr_sample",
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
        print(f"[SMALL-2][{table}] {query}", flush=True)
        src_cur.execute(query)

        placeholders = ", ".join(["%s"] * len(src_cur.description))
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
            print(f" >>> [SMALL-2][{table}] {total} rows...", flush=True)
        print(f"[SMALL-2][{table}] finished total={total}", flush=True)
    except Exception as e:
        print(f"!!! [SMALL-2][{table}] failed: {e}", flush=True)
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
    dag_id="small_mysql_copy_2_v1",
    default_args={"owner": "small", "retries": 0, "retry_delay": timedelta(minutes=2)},
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    tags=["small", "small_2", "mysql", "copy"],
    max_active_tasks=4,
) as dag:
    for table_name in TABLE_LIST:
        PythonOperator(
            task_id=f"migrate_{table_name}",
            python_callable=_mysql_copy_table,
            pool="small_mysql_copy_2",
            pool_slots=1,
            op_kwargs={"table": table_name},
        )
