import re
import MySQLdb
import MySQLdb.cursors

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

# ==========================================================
# LARGE-6: MySQL -> MySQL 복합 인덱스 조건 기반 분할 이행
# ==========================================================
# - 한 테이블에 복합 인덱스가 2개 이상 있고, 날짜/BIGINT 조건으로 분할 가능한 경우
# - plan 별로 FORCE INDEX / 날짜 컬럼 / BIGINT 컬럼 / 추가 조건을 다르게 설정
# - Airflow 태스크 1개 = plan 1개 + 날짜 구간 1개 + 선택적 BIGINT 구간 1개
#
# 예)
#   idx_big_mbr_addr_reg_dt: (region_cd, base_dt)
#   idx_big_mbr_addr_type_dt: (addr_type, base_dt)
#   idx_big_mbr_addr_mbr_dt: (mbr_no, base_dt)  # BIGINT + 날짜
#
# 주의:
# - split_plans 간 조건이 겹치면 target 에 중복 적재될 수 있음.
# - 날짜 구간은 [start, end) 방식: date_col >= start AND date_col < end
# - BIGINT 구간은 BETWEEN start AND end 방식
# - Pool (Admin): large_mysql_composite_split / slots 4

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

TABLE_CONFIG: Dict[str, Dict[str, Any]] = {
    "big_mbr_addr": {
        "source_table": "big_mbr_addr",
        "target_table": "big_mbr_addr",
        "columns": None,  # None 이면 SELECT * / INSERT VALUES
        # 조건이 서로 겹치지 않게 설계해야 함.
        "split_plans": [
            {
                "name": "region_recent",
                "index_hint": "FORCE INDEX (idx_big_mbr_addr_region_dt)",
                "date_col": "base_dt",
                "date_start": "2024-01-01",
                "date_end": "2024-07-01",  # exclusive
                "step_days": 7,
                "where": "region_cd IS NOT NULL",
                "order_by": None,
            },
            {
                "name": "type_recent",
                "index_hint": "FORCE INDEX (idx_big_mbr_addr_type_dt)",
                "date_col": "base_dt",
                "date_start": "2024-07-01",
                "date_end": "2025-01-01",  # exclusive
                "step_days": 7,
                "where": "addr_type IN ('HOME', 'WORK')",
                "order_by": None,
            },
            {
                "name": "mbr_no_date_combo",
                "index_hint": "FORCE INDEX (idx_big_mbr_addr_mbr_no_dt)",
                "date_col": "base_dt",
                "date_start": "2025-01-01",
                "date_end": "2025-04-01",  # exclusive
                "step_days": 7,
                "bigint_col": "mbr_no",
                "bigint_start": 1,
                "bigint_end": 50_000_000,
                "bigint_step": 1_000_000,
                "where": None,
                "order_by": None,
            },
        ],
    },
}


def _quote_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _safe_task_part(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", value).strip("_").lower()


def _parse_date(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d")


def _iter_date_ranges(start: str, end: str, step_days: int) -> List[Tuple[str, str]]:
    ranges: List[Tuple[str, str]] = []
    current = _parse_date(start)
    end_dt = _parse_date(end)
    step = timedelta(days=int(step_days))
    if step.days <= 0:
        raise ValueError("step_days must be > 0")

    while current < end_dt:
        current_end = min(current + step, end_dt)
        ranges.append(
            (
                current.strftime("%Y-%m-%d"),
                current_end.strftime("%Y-%m-%d"),
            )
        )
        current = current_end
    return ranges


def _iter_bigint_ranges(start_id: int, end_id: int, step: int) -> List[Tuple[int, int]]:
    ranges: List[Tuple[int, int]] = []
    current = int(start_id)
    end_i = int(end_id)
    step_i = int(step)
    if step_i <= 0:
        raise ValueError("bigint_step must be > 0")

    while current <= end_i:
        current_end = min(current + step_i - 1, end_i)
        ranges.append((current, current_end))
        current += step_i
    return ranges


def _plan_bigint_ranges(plan: Dict[str, Any]) -> List[Tuple[Optional[int], Optional[int]]]:
    if not plan.get("bigint_col"):
        return [(None, None)]
    return _iter_bigint_ranges(
        int(plan["bigint_start"]),
        int(plan["bigint_end"]),
        int(plan["bigint_step"]),
    )


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
    index_hint: Optional[str],
    date_col: str,
    bigint_col: Optional[str],
    where_sql: Optional[str],
    order_by: Optional[str],
) -> str:
    cols_str = ", ".join(_quote_ident(c) for c in columns) if columns else "*"
    query = f"SELECT {cols_str} FROM {_quote_ident(source_table)}"
    if index_hint:
        query += f" {index_hint}"

    conditions = [f"{_quote_ident(date_col)} >= %s", f"{_quote_ident(date_col)} < %s"]
    if bigint_col:
        conditions.append(f"{_quote_ident(bigint_col)} BETWEEN %s AND %s")
    if where_sql:
        conditions.append(f"({where_sql})")

    query += " WHERE " + " AND ".join(conditions)
    if order_by:
        query += f" ORDER BY {order_by}"
    return query


def _mysql_copy_date_split(
    table_key: str,
    plan: Dict[str, Any],
    date_start: str,
    date_end: str,
    bigint_start: Optional[int] = None,
    bigint_end: Optional[int] = None,
    **context: Any,
) -> None:
    table_cfg = TABLE_CONFIG[table_key]
    source_table = str(table_cfg.get("source_table", table_key))
    target_table = str(table_cfg.get("target_table", table_key))
    columns = table_cfg.get("columns")
    columns = list(columns) if columns else None

    plan_name = str(plan["name"])
    date_col = str(plan["date_col"])
    bigint_col = plan.get("bigint_col")
    bigint_col = str(bigint_col) if bigint_col else None
    index_hint = plan.get("index_hint")
    where_sql = plan.get("where")
    order_by = plan.get("order_by")

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
            columns=columns,
            index_hint=index_hint,
            date_col=date_col,
            bigint_col=bigint_col,
            where_sql=where_sql,
            order_by=order_by,
        )
        params: Tuple[Any, ...]
        if bigint_col:
            if bigint_start is None or bigint_end is None:
                raise ValueError(f"{plan_name}: bigint_start/end required for bigint_col={bigint_col}")
            params = (date_start, date_end, bigint_start, bigint_end)
        else:
            params = (date_start, date_end)
        print(
            f"[LARGE-6][{table_key}][{plan_name}] "
            f"date={date_start}~{date_end} bigint={bigint_start}~{bigint_end} query={query}",
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
                f" >>> [LARGE-6][{table_key}][{plan_name}] "
                f"date={date_start}~{date_end} bigint={bigint_start}~{bigint_end} total={total}",
                flush=True,
            )

        print(
            f"[LARGE-6][{table_key}][{plan_name}] "
            f"date={date_start}~{date_end} bigint={bigint_start}~{bigint_end} done total={total}",
            flush=True,
        )
    except Exception as e:
        print(
            f"!!! [LARGE-6][{table_key}][{plan_name}] "
            f"date={date_start}~{date_end} bigint={bigint_start}~{bigint_end} failed: {e}",
            flush=True,
        )
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
    dag_id="large_6_mysql_composite_index_split_v1",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    tags=["large", "large_6", "mysql", "composite-index", "date-split"],
    max_active_tasks=4,
) as dag:
    for table_name, cfg in TABLE_CONFIG.items():
        for split_plan in cfg["split_plans"]:
            for start_dt, end_dt in _iter_date_ranges(
                split_plan["date_start"],
                split_plan["date_end"],
                int(split_plan["step_days"]),
            ):
                for big_start, big_end in _plan_bigint_ranges(split_plan):
                    task_parts = [
                        "migrate",
                        _safe_task_part(table_name),
                        _safe_task_part(str(split_plan["name"])),
                        start_dt.replace("-", ""),
                        end_dt.replace("-", ""),
                    ]
                    if big_start is not None and big_end is not None:
                        task_parts.extend([str(big_start), str(big_end)])

                    PythonOperator(
                        task_id="_".join(task_parts),
                        python_callable=_mysql_copy_date_split,
                        pool="large_mysql_composite_split",
                        pool_slots=1,
                        op_kwargs={
                            "table_key": table_name,
                            "plan": dict(split_plan),
                            "date_start": start_dt,
                            "date_end": end_dt,
                            "bigint_start": big_start,
                            "bigint_end": big_end,
                        },
                    )
