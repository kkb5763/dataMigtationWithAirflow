import MySQLdb
import MySQLdb.cursors

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

# ==========================================================
# LARGE-7: MySQL -> MySQL recid BIGINT 구간 병렬 이행
# ==========================================================
# - recid 컬럼이 BIGINT 이고 1 ~ 400,000,000 범위가 형성된 테이블용
# - 한 DAG 안에서 recid 구간별 태스크를 생성
# - max_active_tasks=10 + Pool slots=10 → 동시 10개 SELECT/INSERT
#
# 예: STEP_SIZE=10,000,000 이면 1~4억 → 약 40개 태스크, 동시에 10개 실행
# Pool (Admin): large_mysql_recid_parallel / slots 10

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

STEP_SIZE = 10_000_000
CHUNK_SIZE = 10_000
INSERT_STRATEGY = "REPLACE"  # REPLACE | INSERT | INSERT IGNORE

TABLE_CONFIG: Dict[str, Dict[str, Any]] = {
    "big_mbr_addr": {
        "source_table": "big_mbr_addr",
        "target_table": "big_mbr_addr",
        "recid_col": "recid",
        "start": 1,
        "end": 400_000_000,
        "columns": None,  # None 이면 SELECT * / INSERT VALUES
        "where": None,  # 예: "use_yn = 'Y'"
        "index_hint": "FORCE INDEX (idx_big_mbr_addr_recid)",  # 없으면 None
        "order_by": None,  # recid 정렬이 필요하면 "`recid`" 등으로 지정
    },
}


def _quote_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _iter_bigint_ranges(start_id: int, end_id: int, step: int) -> List[Tuple[int, int]]:
    ranges: List[Tuple[int, int]] = []
    current = int(start_id)
    end_i = int(end_id)
    step_i = int(step)
    if step_i <= 0:
        raise ValueError("STEP_SIZE must be > 0")

    while current <= end_i:
        current_end = min(current + step_i - 1, end_i)
        ranges.append((current, current_end))
        current += step_i
    return ranges


def _build_insert_sql(target_table: str, columns: Optional[List[str]], col_count: int) -> str:
    placeholders = ", ".join(["%s"] * col_count)
    if columns:
        cols_str = ", ".join(_quote_ident(c) for c in columns)
        target = f"{_quote_ident(target_table)} ({cols_str})"
    else:
        target = _quote_ident(target_table)

    if INSERT_STRATEGY == "REPLACE":
        return f"REPLACE INTO {target} VALUES ({placeholders})"
    if INSERT_STRATEGY == "INSERT":
        return f"INSERT INTO {target} VALUES ({placeholders})"
    if INSERT_STRATEGY == "INSERT IGNORE":
        return f"INSERT IGNORE INTO {target} VALUES ({placeholders})"
    raise ValueError(f"Unsupported INSERT_STRATEGY: {INSERT_STRATEGY}")


def _build_select_sql(
    source_table: str,
    recid_col: str,
    columns: Optional[List[str]],
    where_sql: Optional[str],
    index_hint: Optional[str],
    order_by: Optional[str],
) -> str:
    cols_str = ", ".join(_quote_ident(c) for c in columns) if columns else "*"
    query = f"SELECT {cols_str} FROM {_quote_ident(source_table)}"
    if index_hint:
        query += f" {index_hint}"

    conditions = [f"{_quote_ident(recid_col)} BETWEEN %s AND %s"]
    if where_sql:
        conditions.append(f"({where_sql})")
    query += " WHERE " + " AND ".join(conditions)

    if order_by:
        query += f" ORDER BY {order_by}"
    return query


def _mysql_copy_recid_range(
    table_key: str,
    range_start: int,
    range_end: int,
    **context: Any,
) -> None:
    cfg = TABLE_CONFIG[table_key]
    source_table = str(cfg.get("source_table", table_key))
    target_table = str(cfg.get("target_table", table_key))
    recid_col = str(cfg.get("recid_col", "recid"))
    columns = cfg.get("columns")
    columns = list(columns) if columns else None
    where_sql = cfg.get("where")
    index_hint = cfg.get("index_hint")
    order_by = cfg.get("order_by")

    src_cfg = dict(SRC_MYSQL)
    src_cfg["cursorclass"] = MySQLdb.cursors.SSCursor
    tgt_cfg = dict(TGT_MYSQL)

    src_conn = None
    tgt_conn = None
    try:
        src_conn = MySQLdb.connect(**src_cfg)
        tgt_conn = MySQLdb.connect(**tgt_cfg)
        src_cur = src_conn.cursor()
        tgt_cur = tgt_conn.cursor()

        query = _build_select_sql(
            source_table=source_table,
            recid_col=recid_col,
            columns=columns,
            where_sql=where_sql,
            index_hint=index_hint,
            order_by=order_by,
        )
        params = (range_start, range_end)
        print(
            f"[LARGE-7][{table_key}] recid {range_start}~{range_end} query={query}",
            flush=True,
        )
        src_cur.execute(query, params)

        col_count = len(src_cur.description)
        insert_sql = _build_insert_sql(target_table, columns, col_count)

        total = 0
        while True:
            rows = src_cur.fetchmany(CHUNK_SIZE)
            if not rows:
                break
            tgt_cur.executemany(insert_sql, rows)
            tgt_conn.commit()
            total += len(rows)
            print(
                f" >>> [LARGE-7][{table_key}] recid {range_start}~{range_end} total={total}",
                flush=True,
            )

        print(f"[LARGE-7][{table_key}] recid {range_start}~{range_end} done total={total}", flush=True)
    except Exception as e:
        print(f"!!! [LARGE-7][{table_key}] recid {range_start}~{range_end} failed: {e}", flush=True)
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
    "owner": "large",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="large_7_mysql_recid_parallel_v1",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    tags=["large", "large_7", "mysql", "recid", "parallel"],
    max_active_tasks=10,
) as dag:
    for table_name, cfg in TABLE_CONFIG.items():
        start_id = cfg.get("start")
        end_id = cfg.get("end")
        if not isinstance(start_id, int) or not isinstance(end_id, int):
            raise ValueError(f"{table_name}: start/end must be int")

        for r_start, r_end in _iter_bigint_ranges(start_id, end_id, STEP_SIZE):
            PythonOperator(
                task_id=f"migrate_{table_name}_{r_start}_{r_end}",
                python_callable=_mysql_copy_recid_range,
                pool="large_mysql_recid_parallel",
                pool_slots=1,
                op_kwargs={
                    "table_key": table_name,
                    "range_start": r_start,
                    "range_end": r_end,
                },
            )
