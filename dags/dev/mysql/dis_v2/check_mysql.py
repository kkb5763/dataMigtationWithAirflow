"""
MySQL connection check (simple).

- No Python DB driver usage
- Uses mysql client to run: SELECT 1;
"""

import os
import subprocess
import sys


MYSQL_ENDPOINTS = [
    {
        "label": "mysql_source",
        "host": os.environ.get("MYSQL_SRC_HOST", "10.10.1.10"),
        "port": int(os.environ.get("MYSQL_SRC_PORT", "3306")),
        "user": os.environ.get("MYSQL_SRC_USER", "root"),
        "pass": os.environ.get("MYSQL_SRC_PASS", "src_mysql_pass123!"),
        "database": os.environ.get("MYSQL_SRC_DB", "dis"),
    },
    {
        "label": "mysql_target",
        "host": os.environ.get("MYSQL_TGT_HOST", "10.10.1.20"),
        "port": int(os.environ.get("MYSQL_TGT_PORT", "3306")),
        "user": os.environ.get("MYSQL_TGT_USER", "root"),
        "pass": os.environ.get("MYSQL_TGT_PASS", "tgt_mysql_pass456@"),
        "database": os.environ.get("MYSQL_TGT_DB", "dis"),
    },
]


def run_select_1(ep: dict) -> str:
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
        "SELECT 1;",
    ]

    env = os.environ.copy()
    env.setdefault("MYSQL_PWD", ep["pass"])  # 일부 환경 호환용

    try:
        proc = subprocess.run(
            cmd,
            env=env,
            check=False,
            capture_output=True,
            text=True,
            timeout=15,
        )
    except FileNotFoundError as e:
        raise RuntimeError("mysql 실행 파일을 찾을 수 없습니다 (PATH 확인 필요)") from e
    except subprocess.TimeoutExpired as e:
        raise RuntimeError("mysql 실행이 timeout 되었습니다 (네트워크/방화벽/host/port 확인)") from e

    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        stdout = (proc.stdout or "").strip()
        msg = stderr or stdout or f"mysql exited with {proc.returncode}"
        raise RuntimeError(msg)

    return (proc.stdout or "").strip()


def main() -> int:
    failed = False
    for ep in MYSQL_ENDPOINTS:
        try:
            out = run_select_1(ep)
            print(f"[OK]   MySQL {ep['label']}: {ep['host']}:{ep['port']} -> {out}", flush=True)
        except Exception as e:
            failed = True
            print(
                f"[FAIL] MySQL {ep['label']}: {ep['host']}:{ep['port']} ({type(e).__name__}: {e})",
                flush=True,
            )

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

