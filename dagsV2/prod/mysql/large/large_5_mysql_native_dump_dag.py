import shlex

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from typing import Any, Dict

# ==========================================================
# LARGE-5: MySQL -> MySQL key 없는 대용량 테이블 네이티브 이행
# ==========================================================
# - 소스 테이블에 row_id 컬럼/인덱스를 만들지 않음
# - Python fetchmany/executemany 대신 MySQL 네이티브 스트리밍 사용
# - mysqldump --quick --no-create-info | mysql
#
# 추천 상황:
# - PK/range key 가 없고, 소스 테이블 변경(DDL/UPDATE)이 어려운 경우
# - Airflow 워커가 row-by-row 로 처리하는 오버헤드를 피하고 싶은 경우
#
# 주의:
# - 결국 full scan 이므로 DB I/O 부하는 있음. 저부하 시간대 실행 권장.
# - key 없는 테이블은 재실행 시 중복 방지 기준이 없으므로 truncate_target=True 권장.
# - mysqldump/mysql client 가 Airflow 워커에 설치되어 있어야 함.
# - Pool (Admin): large_mysql_native_dump / slots 1

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

# 단순하고 강한 선택지:
# - target 을 먼저 비운 뒤 dump stream 을 INSERT/REPLACE 로 적재
# - where 조건이 필요하면 "where" 에 mysqldump --where 조건을 하드코딩
TABLE_CONFIG: Dict[str, Dict[str, Any]] = {
    "big_mbr_addr": {
        "source_table": "big_mbr_addr",
        "target_table": "big_mbr_addr",
        "truncate_target": True,
        "where": None,  # 예: "use_yn = 'Y'"
        "use_replace": False,  # target truncate 후에는 INSERT 가 더 가벼움
    },
}


def _q(value: Any) -> str:
    return shlex.quote(str(value))


def _quote_mysql_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _mysql_client_args(cfg: Dict[str, Any], include_db: bool = True) -> str:
    args = [
        "-h",
        str(cfg["host"]),
        "-P",
        str(cfg["port"]),
        "-u",
        str(cfg["user"]),
        f"--default-character-set={cfg.get('charset', 'utf8mb4')}",
    ]
    if include_db:
        args.append(str(cfg["db"]))
    return " ".join(_q(a) for a in args)


def _build_native_dump_command(table_key: str, cfg: Dict[str, Any]) -> str:
    source_table = str(cfg.get("source_table", table_key))
    target_table = str(cfg.get("target_table", table_key))
    truncate_target = bool(cfg.get("truncate_target", False))
    where_sql = cfg.get("where")
    use_replace = bool(cfg.get("use_replace", False))

    if source_table != target_table:
        raise ValueError("large_5 native dump requires source_table == target_table")

    dump_opts = [
        "--single-transaction",
        "--quick",
        "--skip-lock-tables",
        "--skip-add-locks",
        "--no-create-info",
        "--skip-triggers",
        "--hex-blob",
        f"--default-character-set={SRC_MYSQL.get('charset', 'utf8mb4')}",
    ]
    if use_replace:
        dump_opts.append("--replace")
    if where_sql:
        dump_opts.append(f"--where={where_sql}")

    dump_cmd = " ".join(
        [
            f"MYSQL_PWD={_q(SRC_MYSQL['passwd'])}",
            "mysqldump",
            _mysql_client_args(SRC_MYSQL, include_db=False),
            *(_q(opt) for opt in dump_opts),
            _q(SRC_MYSQL["db"]),
            _q(source_table),
        ]
    )

    load_cmd = " ".join(
        [
            f"MYSQL_PWD={_q(TGT_MYSQL['passwd'])}",
            "mysql",
            _mysql_client_args(TGT_MYSQL, include_db=True),
        ]
    )

    commands = ["set -euo pipefail"]
    if truncate_target:
        truncate_sql = f"TRUNCATE TABLE {_quote_mysql_ident(target_table)}"
        commands.append(
            " ".join(
                [
                    f"MYSQL_PWD={_q(TGT_MYSQL['passwd'])}",
                    "mysql",
                    _mysql_client_args(TGT_MYSQL, include_db=True),
                    "-e",
                    _q(truncate_sql),
                ]
            )
        )

    commands.append(f"{dump_cmd} | {load_cmd}")
    return "\n".join(commands)


default_args = {
    "owner": "large",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="large_5_mysql_native_dump_v1",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    tags=["large", "large_5", "mysql", "native-dump"],
    max_active_tasks=1,
) as dag:
    for table_name, table_cfg in TABLE_CONFIG.items():
        BashOperator(
            task_id=f"native_dump_{table_name}",
            bash_command=_build_native_dump_command(table_name, table_cfg),
            pool="large_mysql_native_dump",
            pool_slots=1,
        )
