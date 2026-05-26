import MySQLdb
import MySQLdb.cursors

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

# ==========================================================
# LARGE-4: MySQL -> MySQL 대용량 단일 테이블 특수 이행
# ==========================================================
# - PK/range key 가 없거나, range select 가 너무 느린 테이블용
# - 기본 모드(row_id): 소스 테이블에 이행용 row id 컬럼을 만들어 BETWEEN 분할
# - 이미 사용할 key 가 있으면 mode="range" 로 바꿔 기존 LARGE 방식처럼 STEP_SIZE 분할 가능
#
# 주의:
# - row_id 준비는 소스 테이블 DDL/UPDATE 를 수행하므로 운영 승인 후 prepare_row_id=True 로 변경.
# - 타겟을 비우고 재적재해야 하는 경우에만 truncate_target=True 로 변경.
# - row_id 컬럼은 INSERT 대상 columns 에서 자동 제외됨.
#
# Pool (Admin): large_mysql_copy_4 / slots 1

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

STEP_SIZE = 100_000
CHUNK_SIZE = 5000
INSERT_STRATEGY = "REPLACE"  # REPLACE | INSERT | INSERT IGNORE

# 단일 특수 테이블 설정.
# columns=None 이면 소스 컬럼을 조회해서 row_id_col 을 제외한 컬럼으로 SELECT/INSERT.
# columns 를 명시하면 SELECT/INSERT 컬럼 순서를 고정할 수 있음.
TABLE_CONFIG: Dict[str, Dict[str, Any]] = {
    "big_mbr_addr": {
        "source_table": "big_mbr_addr",
        "target_table": "big_mbr_addr",
        "mode": "row_id",  # row_id | range
        "row_id_col": "_mig_row_id",
        "row_id_index": "idx_big_mbr_addr_mig_row_id",
        "prepare_row_id": False,  # True 면 소스에 row_id 컬럼 생성/채움/인덱스 생성
        "columns": None,  # 예: ["addr_id", "mbr_no", "addr"]
        "where": None,  # 예: "use_yn = 'Y'" (하드코딩 조건만 사용)
        "order_by": None,  # key 없으면 ORDER BY 금지 권장 (filesort 방지)
        "source_index_hint": None,  # 예: "FORCE INDEX (idx_mbr_no)"
        "truncate_target": False,  # True 면 시작 전 TRUNCATE TABLE target_table
        # mode="range" 로 바꿀 때만 사용
        "range": {"col": "addr_id", "start": 1, "end": 30_000_000},
    },
}


def _quote_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _iter_bigint_ranges(start_id: int, end_id: int, step: int) -> List[Tuple[int, int]]:
    ranges: List[Tuple[int, int]] = []
    current = int(start_id)
    end_i = int(end_id)
    step_i = int(step)
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
    columns: Optional[List[str]],
    where_sql: Optional[str],
    order_by: Optional[str],
    index_hint: Optional[str],
    range_col: Optional[str] = None,
) -> str:
    cols_str = ", ".join(_quote_ident(c) for c in columns) if columns else "*"
    query = f"SELECT {cols_str} FROM {_quote_ident(source_table)}"
    if index_hint:
        query += f" {index_hint}"

    conditions: List[str] = []
    if where_sql:
        conditions.append(f"({where_sql})")
    if range_col:
        conditions.append(f"{_quote_ident(range_col)} BETWEEN %s AND %s")
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    if order_by:
        query += f" ORDER BY {order_by}"
    return query


def _fetch_source_columns(src_conn: Any, source_table: str, exclude_col: Optional[str]) -> List[str]:
    cur = src_conn.cursor()
    try:
        cur.execute(f"SHOW COLUMNS FROM {_quote_ident(source_table)}")
        columns = [str(row[0]) for row in cur.fetchall()]
        if exclude_col:
            columns = [c for c in columns if c.lower() != exclude_col.lower()]
        if not columns:
            raise ValueError(f"{source_table}: no columns found")
        return columns
    finally:
        try:
            cur.close()
        except Exception:
            pass


def _source_column_exists(src_conn: Any, source_table: str, column_name: str) -> bool:
    cur = src_conn.cursor()
    try:
        cur.execute(f"SHOW COLUMNS FROM {_quote_ident(source_table)} LIKE %s", (column_name,))
        return cur.fetchone() is not None
    finally:
        try:
            cur.close()
        except Exception:
            pass


def _ensure_row_id_column(
    src_conn: Any,
    source_table: str,
    row_id_col: str,
    row_id_index: str,
) -> None:
    """소스 테이블에 이행용 row id 컬럼을 만들고 값/인덱스를 준비."""
    cur = src_conn.cursor()
    try:
        if not _source_column_exists(src_conn, source_table, row_id_col):
            print(f"[LARGE-4][{source_table}] add helper column {row_id_col}", flush=True)
            cur.execute(
                f"ALTER TABLE {_quote_ident(source_table)} "
                f"ADD COLUMN {_quote_ident(row_id_col)} BIGINT NULL"
            )
            src_conn.commit()

        print(f"[LARGE-4][{source_table}] populate helper row id {row_id_col}", flush=True)
        cur.execute("SET @mig_row_id := 0")
        cur.execute(
            f"UPDATE {_quote_ident(source_table)} "
            f"SET {_quote_ident(row_id_col)} = (@mig_row_id := @mig_row_id + 1) "
            f"WHERE {_quote_ident(row_id_col)} IS NULL"
        )
        src_conn.commit()

        print(f"[LARGE-4][{source_table}] add helper index {row_id_index}", flush=True)
        try:
            cur.execute(
                f"ALTER TABLE {_quote_ident(source_table)} "
                f"ADD INDEX {_quote_ident(row_id_index)} ({_quote_ident(row_id_col)})"
            )
            src_conn.commit()
        except Exception as e:
            # 이미 인덱스가 있으면 계속 진행. 다른 오류는 아래 bounds/select 에서 드러남.
            print(f"[LARGE-4][{source_table}] add index skipped or failed: {e}", flush=True)
    finally:
        try:
            cur.close()
        except Exception:
            pass


def _fetch_row_id_bounds(src_conn: Any, source_table: str, row_id_col: str) -> Tuple[int, int]:
    cur = src_conn.cursor()
    try:
        cur.execute(
            f"SELECT MIN({_quote_ident(row_id_col)}), MAX({_quote_ident(row_id_col)}) "
            f"FROM {_quote_ident(source_table)}"
        )
        min_id, max_id = cur.fetchone()
        if min_id is None or max_id is None:
            raise ValueError(f"{source_table}: no row_id values in {row_id_col}")
        return int(min_id), int(max_id)
    finally:
        try:
            cur.close()
        except Exception:
            pass


def _mysql_copy_special_table(table_key: str, **context: Any) -> None:
    cfg = TABLE_CONFIG[table_key]
    source_table = str(cfg.get("source_table", table_key))
    target_table = str(cfg.get("target_table", table_key))
    mode = str(cfg.get("mode", "row_id"))
    row_id_col = str(cfg.get("row_id_col", "_mig_row_id"))
    row_id_index = str(cfg.get("row_id_index", f"idx_{source_table}_mig_row_id"))
    prepare_row_id = bool(cfg.get("prepare_row_id", False))
    columns = cfg.get("columns")
    columns = list(columns) if columns else None
    where_sql = cfg.get("where")
    order_by = cfg.get("order_by")
    index_hint = cfg.get("source_index_hint")
    truncate_target = bool(cfg.get("truncate_target", False))

    src_cfg = dict(SRC_MYSQL)
    src_cfg["cursorclass"] = MySQLdb.cursors.SSCursor
    tgt_cfg = dict(TGT_MYSQL)

    if mode == "range":
        range_cfg: Dict[str, Any] = dict(cfg.get("range", {}) or {})
        range_col = range_cfg.get("col")
        start_id = range_cfg.get("start")
        end_id = range_cfg.get("end")
        if not range_col or not isinstance(start_id, int) or not isinstance(end_id, int):
            raise ValueError(f"{table_key}: mode=range requires range.col/start/end(int)")
        id_ranges: List[Tuple[Optional[int], Optional[int]]] = _iter_bigint_ranges(
            int(start_id),
            int(end_id),
            STEP_SIZE,
        )
    elif mode == "row_id":
        range_col = row_id_col
        id_ranges = []
    else:
        raise ValueError(f"{table_key}: unsupported mode={mode}")

    src_conn = None
    tgt_conn = None
    try:
        src_conn = MySQLdb.connect(**src_cfg)
        tgt_conn = MySQLdb.connect(**tgt_cfg)
        tgt_cur = tgt_conn.cursor()

        if mode == "row_id":
            if prepare_row_id:
                _ensure_row_id_column(src_conn, source_table, row_id_col, row_id_index)
            elif not _source_column_exists(src_conn, source_table, row_id_col):
                raise ValueError(
                    f"{table_key}: row_id column {row_id_col} not found. "
                    f"Set prepare_row_id=True once, or create/index the column manually."
                )

            start_id, end_id = _fetch_row_id_bounds(src_conn, source_table, row_id_col)
            id_ranges = _iter_bigint_ranges(start_id, end_id, STEP_SIZE)

        if columns is None:
            columns = _fetch_source_columns(
                src_conn,
                source_table,
                row_id_col if mode == "row_id" else None,
            )

        if truncate_target:
            print(f"[LARGE-4][{table_key}] truncate target {_quote_ident(target_table)}", flush=True)
            tgt_cur.execute(f"TRUNCATE TABLE {_quote_ident(target_table)}")
            tgt_conn.commit()

        total = 0
        for r_start, r_end in id_ranges:
            src_cur = src_conn.cursor()
            query = _build_select_sql(
                source_table=source_table,
                columns=columns,
                where_sql=where_sql,
                order_by=order_by,
                index_hint=index_hint,
                range_col=range_col if mode in ("range", "row_id") else None,
            )
            params: List[Any] = []
            if mode in ("range", "row_id"):
                params = [r_start, r_end]

            print(
                f"[LARGE-4][{table_key}] mode={mode} range={r_start}~{r_end} query={query}",
                flush=True,
            )
            src_cur.execute(query, tuple(params))

            col_count = len(src_cur.description)
            insert_sql = _build_insert_sql(target_table, columns, col_count)

            chunk_in_range = 0
            while True:
                rows = src_cur.fetchmany(CHUNK_SIZE)
                if not rows:
                    break
                tgt_cur.executemany(insert_sql, rows)
                tgt_conn.commit()
                chunk_in_range += len(rows)
                total += len(rows)
                print(f" >>> [LARGE-4][{table_key}] total={total}", flush=True)

            try:
                src_cur.close()
            except Exception:
                pass
            print(
                f"[LARGE-4][{table_key}] range {r_start}~{r_end} done rows={chunk_in_range} total={total}",
                flush=True,
            )

        print(f"[LARGE-4][{table_key}] finished total={total}", flush=True)
    except Exception as e:
        print(f"!!! [LARGE-4][{table_key}] failed: {e}", flush=True)
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
    dag_id="large_4_mysql_copy_single_table_v1",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    tags=["large", "large_4", "mysql", "copy", "single-table"],
    max_active_tasks=1,
) as dag:
    for table_name in TABLE_CONFIG:
        PythonOperator(
            task_id=f"migrate_{table_name}",
            python_callable=_mysql_copy_special_table,
            pool="large_mysql_copy_4",
            pool_slots=1,
            op_kwargs={"table_key": table_name},
        )
