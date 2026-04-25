import os
import subprocess
from datetime import datetime, timedelta
from typing import Any, Dict, Tuple

from airflow import DAG
from airflow.operators.python import PythonOperator

# ==========================================================
# PostgreSQL -> PostgreSQL (DEV/CMS) : 범위(청크) 단위 \copy 파이프라인
# ==========================================================
# - Python DB 드라이버(psycopg2 등) 설치 없이, psql 클라이언트만으로 이행
# - Airflow 워커가 소스/타겟 모두에 outbound 접속 가능해야 합니다.
# - 소스:  \copy (SELECT ...) TO STDOUT WITH (FORMAT csv)
# - 타겟:  \copy table FROM STDIN WITH (FORMAT csv)


SRC_PG_CONFIG: Dict[str, Any] = {
    "host": os.environ.get("PG_SRC_HOST", "10.10.2.10"),
    "port": int(os.environ.get("PG_SRC_PORT", "5432")),
    "user": os.environ.get("PG_SRC_USER", "postgres"),
    "pass": os.environ.get("PG_SRC_PASS", "src_pg_pass123!"),
    "database": os.environ.get("PG_SRC_DB", "postgres"),
}

TGT_PG_CONFIG: Dict[str, Any] = {
    "host": os.environ.get("PG_TGT_HOST", "10.10.2.20"),
    "port": int(os.environ.get("PG_TGT_PORT", "5432")),
    "user": os.environ.get("PG_TGT_USER", "postgres"),
    "pass": os.environ.get("PG_TGT_PASS", "tgt_pg_pass456@"),
    "database": os.environ.get("PG_TGT_DB", "postgres"),
}

# ==========================================================
# 테이블별 범위 설정 (필수)
# 구조: "schema.table" 또는 "table": (range_col, start, end)
# ==========================================================
TABLE_CONFIG: Dict[str, Tuple[str, int, int]] = {
    "public.sample_table": ("id", 1, 1_000_000),
}

STEP_SIZE = 100_000

# ==========================================================
# COPY 옵션
# ==========================================================
COPY_WITH_OPTIONS = "FORMAT csv"


def _pg_quote_ident(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def _pg_qualified_table(name: str) -> str:
    if "." in name:
        schema, table = name.split(".", 1)
        return f"{_pg_quote_ident(schema)}.{_pg_quote_ident(table)}"
    return _pg_quote_ident(name)


def _build_psql_cmd(cfg: Dict[str, Any]) -> Tuple[list, Dict[str, str]]:
    cmd = [
        "psql",
        "-h",
        str(cfg["host"]),
        "-p",
        str(int(cfg.get("port", 5432))),
        "-U",
        str(cfg["user"]),
        "-d",
        str(cfg["database"]),
        "--no-password",
        "-v",
        "ON_ERROR_STOP=1",
        "-q",
        "-tA",
    ]
    env = os.environ.copy()
    env["PGPASSWORD"] = str(cfg.get("pass", ""))
    env.setdefault("PGCONNECT_TIMEOUT", "10")
    return cmd, env


def _pipe_copy_range(table: str, range_col: str, start: int, end: int) -> None:
    q_table = _pg_qualified_table(table)
    q_col = _pg_quote_ident(range_col)

    src_copy = (
        f"\\copy (SELECT * FROM {q_table} "
        f"WHERE {q_col} BETWEEN {int(start)} AND {int(end)}) "
        f"TO STDOUT WITH ({COPY_WITH_OPTIONS})"
    )
    tgt_copy = f"\\copy {q_table} FROM STDIN WITH ({COPY_WITH_OPTIONS})"

    src_cmd, src_env = _build_psql_cmd(SRC_PG_CONFIG)
    tgt_cmd, tgt_env = _build_psql_cmd(TGT_PG_CONFIG)

    src_proc = subprocess.Popen(
        [*src_cmd, "-c", src_copy],
        env=src_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=False,
    )
    assert src_proc.stdout is not None

    tgt_proc = subprocess.Popen(
        [*tgt_cmd, "-c", tgt_copy],
        env=tgt_env,
        stdin=src_proc.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=False,
    )

    src_proc.stdout.close()
    tgt_out, tgt_err = tgt_proc.communicate()
    src_out, src_err = src_proc.communicate()

    if src_proc.returncode != 0:
        raise RuntimeError(
            f"source psql failed (code={src_proc.returncode}) "
            f"err={(src_err or b'').decode('utf-8', errors='ignore').strip()} "
            f"out={(src_out or b'').decode('utf-8', errors='ignore').strip()}"
        )
    if tgt_proc.returncode != 0:
        raise RuntimeError(
            f"target psql failed (code={tgt_proc.returncode}) "
            f"err={(tgt_err or b'').decode('utf-8', errors='ignore').strip()} "
            f"out={(tgt_out or b'').decode('utf-8', errors='ignore').strip()}"
        )


def _worker(table: str, range_col: str, start: int, end: int, **context: Any) -> None:
    current = int(start)
    total_ranges = 0

    while current <= end:
        current_end = min(current + STEP_SIZE - 1, end)
        _pipe_copy_range(table, range_col, current, current_end)
        total_ranges += 1

        print(
            f"[{table}] range {current}~{current_end} copy done (ranges={total_ranges})",
            flush=True,
        )
        current += STEP_SIZE


default_args = {
    "owner": "dev",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dev_pgsql_cms_chunk_mig_v1",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dev", "pgsql", "cms", "chunk"],
    max_active_tasks=1,
) as dag:
    for table_name, (col, start, end) in TABLE_CONFIG.items():
        PythonOperator(
            task_id=f"migrate_{table_name.replace('.', '_')}",
            python_callable=_worker,
            op_kwargs={
                "table": table_name,
                "range_col": col,
                "start": start,
                "end": end,
            },
        )

