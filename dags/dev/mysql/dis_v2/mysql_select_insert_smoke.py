"""
MySQL select+insert smoke test.

목적:
- 운영/테스트 전에 "SELECT 가능" + "INSERT 가능"을 최소 수준으로 확인
- Python DB driver 없이 mysql client만 사용

동작:
1) target DB에 스모크 테이블 생성
2) target DB에 1 row INSERT
3) target DB에서 SELECT count(*) 수행
"""

import os
import subprocess
import sys
from datetime import datetime


TGT = {
    "label": "mysql_target",
    "host": os.environ.get("MYSQL_TGT_HOST", "10.10.1.20"),
    "port": int(os.environ.get("MYSQL_TGT_PORT", "3306")),
    "user": os.environ.get("MYSQL_TGT_USER", "root"),
    "pass": os.environ.get("MYSQL_TGT_PASS", "tgt_mysql_pass456@"),
    "database": os.environ.get("MYSQL_TGT_DB", "dis"),
}

SMOKE_TABLE = os.environ.get("MYSQL_SMOKE_TABLE", "__mig_smoke_test")


def _run_mysql(ep: dict, sql: str) -> str:
    cmd = [
        "mysql",
        "-h",
        ep["host"],
        "-P",
        str(int(ep.get("port", 3306))),
        "-u",
        ep["user"],
        f"-p{ep['pass']}",
        "-D",
        ep["database"],
        "-Nse",
        sql,
    ]

    env = os.environ.copy()
    env.setdefault("MYSQL_PWD", ep["pass"])

    proc = subprocess.run(
        cmd,
        env=env,
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )
    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        stdout = (proc.stdout or "").strip()
        raise RuntimeError(stderr or stdout or f"mysql exited with {proc.returncode}")
    return (proc.stdout or "").strip()


def main() -> int:
    try:
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{SMOKE_TABLE}` (
            id BIGINT NOT NULL AUTO_INCREMENT,
            run_id VARCHAR(64) NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """.strip()
        _run_mysql(TGT, create_sql)

        run_id = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        insert_sql = f"INSERT INTO `{SMOKE_TABLE}` (run_id) VALUES ('{run_id}');"
        _run_mysql(TGT, insert_sql)

        out = _run_mysql(TGT, f"SELECT COUNT(*) FROM `{SMOKE_TABLE}`;")
        print(f"[OK]   MySQL smoke: insert+select ok (count={out})", flush=True)
        return 0
    except FileNotFoundError as e:
        print("[FAIL] mysql 실행 파일을 찾을 수 없습니다 (PATH 확인 필요)", flush=True)
        return 1
    except Exception as e:
        print(f"[FAIL] MySQL smoke failed: {type(e).__name__}: {e}", flush=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

