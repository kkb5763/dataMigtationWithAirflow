import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

from airflow import DAG
from airflow.operators.python import PythonOperator

# ==========================================================
# MySQL DDL Apply (dags/table)
# ==========================================================
# - 관리자 계정 + SSL 로 mysql CLI 접속
# - 스키마별 폴더의 .sql 을 정렬 순서대로 실행 (001 스키마 → 010 테이블 …)
#
# DDL 배포 경로 (Airflow 서버, git 과 무관)
DDL_ROOT_DIR = Path("/data/app/airflow/dataMigtationWithAirflow/table/mysql")

# ==========================================================
# 1) 타겟 MySQL (관리자 계정)
# ==========================================================
MYSQL_ADMIN: Dict[str, Any] = {
    "host": "10.10.1.20",
    "port": 3306,
    "user": "admin_user",
    "pass": "admin_password_here",
}

# ==========================================================
# 2) SSL (mysql CLI 옵션)
# ==========================================================
# enabled=False 이면 SSL 옵션 미사용
MYSQL_SSL: Dict[str, Any] = {
    "enabled": True,
    "mode": "VERIFY_IDENTITY",  # DISABLED | PREFERRED | REQUIRED | VERIFY_CA | VERIFY_IDENTITY
    "ca": "/data/app/airflow/dataMigtationWithAirflow/certs/ca.pem",
    "cert": None,  # 클라이언트 인증서 (없으면 None)
    "key": None,
}

# ==========================================================
# 3) 스키마별 DDL (명시)
# ==========================================================
# - schema: 생성할 MySQL 스키마(데이터베이스) 이름
# - ddl_dir: DDL_ROOT_DIR 아래 SQL 폴더명
# - connect_database: mysql -d 접속 DB (보통 mysql; SQL 에 CREATE DATABASE / USE 포함)
#
# 폴더 내 파일 예:
#   001_<schema>_schema.sql  → CREATE DATABASE …
#   010_<schema>_tables.sql  → USE `<schema>`; CREATE TABLE …
DDL_SCHEMA_PLANS: Dict[str, Dict[str, str]] = {
    "dis": {
        "ddl_dir": "_dis",
        "connect_database": "mysql",
    },
    "dip": {
        "ddl_dir": "_dip",
        "connect_database": "mysql",
    },
}

# 폴더(스키마) 동시 실행 수 — Pools: table_mysql_ddl, slots=4
MAX_PARALLEL_SCHEMAS = 4
DDL_POOL = "table_mysql_ddl"


def _list_sql_files(folder: Path) -> List[Path]:
    return sorted(p for p in folder.iterdir() if p.is_file() and p.suffix.lower() == ".sql")


def _build_mysql_cmd(connect_database: str) -> List[str]:
    cmd = [
        "mysql",
        "--protocol=tcp",
        "-h",
        str(MYSQL_ADMIN["host"]),
        "-P",
        str(int(MYSQL_ADMIN["port"])),
        "-u",
        str(MYSQL_ADMIN["user"]),
        connect_database,
    ]

    if MYSQL_SSL.get("enabled"):
        cmd.append(f"--ssl-mode={MYSQL_SSL.get('mode', 'VERIFY_IDENTITY')}")
        ca = MYSQL_SSL.get("ca")
        if ca:
            cmd.extend(["--ssl-ca", str(ca)])
        cert = MYSQL_SSL.get("cert")
        key = MYSQL_SSL.get("key")
        if cert:
            cmd.extend(["--ssl-cert", str(cert)])
        if key:
            cmd.extend(["--ssl-key", str(key)])

    return cmd


def _run_mysql_sql_file(connect_database: str, sql_file: Path, schema: str) -> None:
    cmd = _build_mysql_cmd(connect_database)
    env = os.environ.copy()
    env["MYSQL_PWD"] = str(MYSQL_ADMIN.get("pass", ""))

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
        raise RuntimeError("mysql CLI를 찾을 수 없습니다. (워커에 mysql client 설치/PATH 필요)") from e

    if proc.returncode != 0:
        stderr = (proc.stderr or b"").decode("utf-8", errors="ignore").strip()
        stdout = (proc.stdout or b"").decode("utf-8", errors="ignore").strip()
        msg = stderr or stdout or f"mysql exited with {proc.returncode}"
        raise RuntimeError(
            f"[schema={schema}] connect_db={connect_database} "
            f"file={sql_file.name} -> {msg}"
        )

    print(
        f"[schema={schema}] connect_db={connect_database} applied {sql_file.name}",
        flush=True,
    )


def _apply_schema_ddl(schema: str, ddl_dir: str, connect_database: str, **context: Any) -> None:
    folder = DDL_ROOT_DIR / ddl_dir
    if not folder.is_dir():
        raise RuntimeError(f"DDL directory not found: {folder}")

    sql_files = _list_sql_files(folder)
    if not sql_files:
        print(f"[skip] schema={schema} dir={ddl_dir} has no .sql files", flush=True)
        return

    print(
        f"[start] schema={schema} dir={ddl_dir} connect_db={connect_database} "
        f"files={len(sql_files)} ssl={MYSQL_SSL.get('enabled')}",
        flush=True,
    )
    for sql_file in sql_files:
        _run_mysql_sql_file(connect_database, sql_file, schema)
    print(f"[finish] schema={schema}", flush=True)


default_args = {
    "owner": "migration",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="table_mysql_apply_ddl_v1",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    tags=["table", "mysql", "ddl"],
    max_active_tasks=MAX_PARALLEL_SCHEMAS,
) as dag:
    for schema_name, plan in DDL_SCHEMA_PLANS.items():
        PythonOperator(
            task_id=f"ddl__{schema_name}",
            python_callable=_apply_schema_ddl,
            pool=DDL_POOL,
            pool_slots=1,
            op_kwargs={
                "schema": schema_name,
                "ddl_dir": plan["ddl_dir"],
                "connect_database": plan["connect_database"],
            },
        )
