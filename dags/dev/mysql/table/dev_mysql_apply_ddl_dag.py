import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from airflow import DAG
from airflow.operators.python import PythonOperator

# ==========================================================
# DEV MySQL DDL Apply DAG
# ==========================================================
# 목적:
# - dags/dev/mysql/table/* 아래에 있는 .sql 파일을 "정렬된 순서"로 실행하여
#   타겟 MySQL에 테이블/인덱스/뷰 등을 생성합니다.
#
# 전제:
# - Airflow 워커(실행 노드)에서 `mysql` CLI가 PATH로 접근 가능해야 합니다.
# - 비밀번호는 커맨드라인(-p...) 대신 env(MYSQL_PWD)로 전달합니다.
#
# 디렉토리 규칙:
# - 하위 폴더를 어떤 DB에 적용할지는 아래 `DDL_DIR_TO_DB`에 명시합니다.


DDL_ROOT_DIR = Path(__file__).resolve().parent  # dags/dev/mysql/table

MYSQL_TGT: Dict[str, Any] = {
    "host": os.environ.get("MYSQL_DDL_HOST", "10.10.1.20"),
    "port": int(os.environ.get("MYSQL_DDL_PORT", "3306")),
    "user": os.environ.get("MYSQL_DDL_USER", "root"),
    "pass": os.environ.get("MYSQL_DDL_PASS", "tgt_mysql_pass456@"),
}

# 특정 디렉토리만 실행하고 싶으면 "comma-separated"로 지정 (예: "_dis,_dip")
DDL_ONLY_DIRS = os.environ.get("MYSQL_DDL_ONLY_DIRS", "").strip()


DDL_DIR_TO_DB: Dict[str, str] = {
    "_dis": "dis",
    "_dip": "dip",
}


def _list_sql_files(folder: Path) -> List[Path]:
    return sorted([p for p in folder.iterdir() if p.is_file() and p.suffix.lower() == ".sql"])


def _run_mysql_sql_file(db: str, sql_file: Path) -> None:
    cmd = [
        "mysql",
        "--protocol=tcp",
        "-h",
        str(MYSQL_TGT["host"]),
        "-P",
        str(int(MYSQL_TGT["port"])),
        "-u",
        str(MYSQL_TGT["user"]),
        db,
    ]

    env = os.environ.copy()
    env["MYSQL_PWD"] = str(MYSQL_TGT.get("pass", ""))

    try:
        with sql_file.open("rb") as f:
            proc = subprocess.run(
                cmd,
                stdin=f,
                env=env,
                capture_output=True,
                check=False,
            )
    except FileNotFoundError as e:
        raise RuntimeError("mysql CLI를 찾을 수 없습니다. (Airflow 워커에 mysql client 설치/PATH 필요)") from e

    if proc.returncode != 0:
        stderr = (proc.stderr or b"").decode("utf-8", errors="ignore").strip()
        stdout = (proc.stdout or b"").decode("utf-8", errors="ignore").strip()
        msg = stderr or stdout or f"mysql exited with {proc.returncode}"
        raise RuntimeError(f"[db={db}] apply failed: {sql_file.name} -> {msg}")

    print(f"[db={db}] applied {sql_file.name}", flush=True)


def apply_all_ddl(**context: Any) -> None:
    only: Optional[List[str]] = None
    if DDL_ONLY_DIRS:
        only = [s.strip() for s in DDL_ONLY_DIRS.split(",") if s.strip()]

    selected_dirs = list(DDL_DIR_TO_DB.keys())
    if only is not None:
        selected_dirs = [d for d in selected_dirs if d in set(only)]

    if not selected_dirs:
        raise RuntimeError("No DDL directories selected. Check MYSQL_DDL_ONLY_DIRS or DDL_DIR_TO_DB.")

    for dir_name in selected_dirs:
        if dir_name not in DDL_DIR_TO_DB:
            raise RuntimeError(f"DDL_DIR_TO_DB has no mapping for: {dir_name}")

        d = DDL_ROOT_DIR / dir_name
        if not d.exists() or not d.is_dir():
            raise RuntimeError(f"DDL directory not found: {d}")

        db = DDL_DIR_TO_DB[dir_name]
        sql_files = _list_sql_files(d)
        if not sql_files:
            print(f"[skip] {dir_name} has no .sql files", flush=True)
            continue

        print(f"[start] apply DDL dir={dir_name} db={db} files={len(sql_files)}", flush=True)
        for f in sql_files:
            _run_mysql_sql_file(db, f)
        print(f"[finish] apply DDL dir={dir_name} db={db}", flush=True)


default_args = {
    "owner": "dev",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dev_mysql_apply_ddl_v1",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dev", "mysql", "ddl", "table"],
    max_active_tasks=1,
) as dag:
    PythonOperator(
        task_id="apply_mysql_ddl",
        python_callable=apply_all_ddl,
    )

