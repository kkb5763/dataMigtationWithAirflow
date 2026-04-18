import base64
import os
import subprocess
import sys
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import URLError, HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen

import MySQLdb
import MySQLdb.cursors

# ==========================================================
# 1) 환경 설정 (기본은 "코드 값"이 기준)
# ==========================================================
# Airflow에서 env가 안 실려오는 경우가 많아서, 이 스크립트는
# "코드 상단 설정"만 바꿔도 동작하도록 구성합니다.
#
# 필요하면 아래 값을 env로 덮어쓸 수는 있지만(선택), env만 믿지는 않습니다.

JAVA_BIN = "java"
JAR_PATH = "/path/to/your/decryptor.jar"  # JAR 절대경로 권장

# subprocess로 "메인 클래스" 실행할 때 사용 (라이브러리 JAR + main class 조합)
# - JAVA_MAIN_CLASS가 비어있지 않으면 `-jar` 대신 `-cp ... <mainClass>`로 실행합니다.
# - classpath는 OS에 맞게 `os.pathsep`로 join 됩니다. (Windows=';', Linux=':')
JAVA_MAIN_CLASS = ""  # 예: tools.damo.DamoDecryptCli
JAVA_CLASSPATH: List[str] = []  # 예: ["/path/to/damo.jar", "/path/to/other-deps.jar", "/path/to/compiled/classes_or_cli.jar"]

# JPype는 선택 기능입니다. (jpype1 설치 + 호출 가능한 클래스/메서드가 있을 때만)
USE_JPYPE = False
JPYPE_CLASS = ""  # 예: com.company.crypto.Decryptor
JPYPE_METHOD = "decrypt"


# D'amo (ScpDbAgent) 샘플 시그니처 지원
# - Java 예: new ScpDbAgent(); ag.scpDecrypt(confPath, "KEY_GROUP_NAME", encryptedData);
# - 아래 값을 실제 환경에 맞게 지정하세요.
DAMO_CONF_PATH = "/path/to/your/damo_api.conf"
DAMO_KEY_GROUP = "KEY_GROUP_NAME"

# ==========================================================
# 1-1) HTTP 유틸 API (JDK 내장 HttpServer) 연동 옵션
# ==========================================================
# 예: SimpleUtilHttpApi 기동 후
#   - http://127.0.0.1:8082/base64/enc/<plain>
#   - http://127.0.0.1:8082/base64/dec/<b64>
#   - http://127.0.0.1:8082/mock/enc/<plain>
#
# 값은 URL path segment 로 들어가므로 UTF-8 URL 인코딩이 필요합니다.
USE_HTTP_UTIL_API = True
HTTP_UTIL_API_BASE = os.getenv("HTTP_UTIL_API_BASE", "http://127.0.0.1:8082").rstrip("/")
# "base64/enc", "base64/dec", "mock/enc" 중 하나를 넣으면 됩니다.
HTTP_UTIL_API_MODE = os.getenv("HTTP_UTIL_API_MODE", "base64/enc").strip("/")
HTTP_UTIL_API_TIMEOUT_SEC = float(os.getenv("HTTP_UTIL_API_TIMEOUT_SEC", "5"))


SRC = {
    "host": "10.10.1.10",
    "port": 3306,
    "user": "root",
    "passwd": "src_mysql_pass123!",
    "db": "member_db",
    "cursorclass": MySQLdb.cursors.DictCursor,
    "charset": "utf8mb4",
}

TGT = {
    "host": "10.10.1.20",
    "port": 3306,
    "user": "root",
    "passwd": "tgt_mysql_pass456@",
    "db": "member_db",
    "charset": "utf8mb4",
}

CHUNK_SIZE = 1000


# ==========================================================
# 2) 테이블별 설정 (여기에 여러 테이블을 추가 가능)
# ==========================================================
# - columns: SELECT 및 INSERT 대상 컬럼 (순서 중요)
# - decrypt_cols: 복호화(또는 암/복호화) 적용할 컬럼들
# - range: (선택) 범위 조건. start/end가 None이면 조건 생략
#
# 예시처럼 여러 테이블을 동시에 넣을 수 있습니다.
TABLE_CONFIG: Dict[str, Dict[str, Any]] = {
    "mbr_base": {
        "columns": ["mbr_no", "mbr_id", "mbr_nm", "enc_email"],
        "decrypt_cols": ["enc_email"],
        "range": {
            "col": "mbr_no",
            "start": 1,
            "end": 50000,
        },
    },
    # "mbr_detail": {
    #     "columns": ["mbr_no", "enc_phone"],
    #     "decrypt_cols": ["enc_phone"],
    #     "range": {"col": "mbr_no", "start": None, "end": None},  # 전체
    # },
}


# ==========================================================
# 3) Base64 유틸 (필요 시)
# ==========================================================
def handle_base64(data: Optional[str], mode: str = "decode") -> Optional[str]:
    if not data:
        return data
    try:
        if mode == "decode":
            return base64.b64decode(data.encode("utf-8")).decode("utf-8")
        return base64.b64encode(data.encode("utf-8")).decode("utf-8")
    except Exception as e:
        print(f"Base64 {mode} Error: {e}", flush=True)
        return data


# ==========================================================
# 4) Java 복호화 구현 (subprocess / jpype)
# ==========================================================
_jpype_ready = False
_jpype_class_ref = None
_jpype_instance_ref = None

# JPype 인스턴스 호출 옵션
# - Java 쪽이 `new Decryptor(...).decrypt(String)` 같은 구조라면 아래를 사용합니다.
# - JPYPE_CONSTRUCTOR_ARGS는 생성자 파라미터(문자열 등)를 순서대로 넣습니다.
JPYPE_USE_INSTANCE = False
JPYPE_CONSTRUCTOR_ARGS: Tuple[Any, ...] = ()


def _init_jpype_if_needed() -> None:
    global _jpype_ready, _jpype_class_ref, _jpype_instance_ref
    if _jpype_ready:
        return

    if not USE_JPYPE:
        return

    if not JPYPE_CLASS:
        raise RuntimeError("USE_JPYPE=1 이지만 JPYPE_CLASS가 비어있습니다.")

    try:
        import jpype  # type: ignore
        import jpype.imports  # type: ignore  # noqa: F401
    except ModuleNotFoundError as e:
        raise RuntimeError("JPype가 설치되어 있지 않습니다. (pip install jpype1)") from e

    if not os.path.isabs(JAR_PATH):
        raise RuntimeError("JPype 사용 시 JAR_PATH는 절대경로여야 합니다.")

    if not os.path.exists(JAR_PATH):
        raise RuntimeError(f"JAR 파일을 찾을 수 없습니다: {JAR_PATH}")

    if not jpype.isJVMStarted():
        jpype.startJVM(classpath=[JAR_PATH])

    _jpype_class_ref = jpype.JClass(JPYPE_CLASS)

    if JPYPE_USE_INSTANCE:
        try:
            _jpype_instance_ref = _jpype_class_ref(*JPYPE_CONSTRUCTOR_ARGS)
        except Exception as e:
            raise RuntimeError(f"JPype 인스턴스 생성 실패: {JPYPE_CLASS} args={JPYPE_CONSTRUCTOR_ARGS} err={e}") from e
    _jpype_ready = True


def call_java_decrypt(encrypted_value: Optional[str]) -> Optional[str]:
    if not encrypted_value:
        return encrypted_value

    if USE_JPYPE:
        _init_jpype_if_needed()
        try:
            # 기본은 static method 호출: Decryptor.decrypt(String)
            # Java가 생성자를 통해 인스턴스를 만든 뒤 인스턴스 메서드를 호출하는 구조라면
            # JPYPE_USE_INSTANCE=True 로 바꿔서 instance.decrypt(String) 경로를 사용합니다.
            target = _jpype_instance_ref if JPYPE_USE_INSTANCE else _jpype_class_ref
            fn = getattr(target, JPYPE_METHOD)

            # D'amo ScpDbAgent.scpDecrypt(confPath, keyGroup, encrypted) 형태 지원
            if JPYPE_METHOD == "scpDecrypt":
                out = fn(DAMO_CONF_PATH, DAMO_KEY_GROUP, encrypted_value)
            else:
                out = fn(encrypted_value)
            return str(out).strip() if out is not None else None
        except Exception as e:
            print(f"JPype Decrypt Fail: {e}", flush=True)
            return f"ERR_DECRYPT_{encrypted_value}"

    # 기본: subprocess로 jar 실행
    try:
        if JAVA_MAIN_CLASS:
            if not JAVA_CLASSPATH:
                raise RuntimeError("JAVA_MAIN_CLASS가 설정됐지만 JAVA_CLASSPATH가 비어있습니다.")
            missing = [p for p in JAVA_CLASSPATH if p and not os.path.exists(p)]
            if missing:
                raise RuntimeError(f"CLASSPATH 파일을 찾을 수 없습니다: {missing}")
            cp = os.pathsep.join(JAVA_CLASSPATH)
            cmd = [JAVA_BIN, "-cp", cp, JAVA_MAIN_CLASS, DAMO_CONF_PATH, DAMO_KEY_GROUP, encrypted_value]
        else:
            if not os.path.exists(JAR_PATH):
                raise RuntimeError(f"JAR 파일을 찾을 수 없습니다: {JAR_PATH}")
            cmd = [JAVA_BIN, "-jar", JAR_PATH, encrypted_value]

        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return (result.stdout or "").strip()
    except Exception as e:
        print(f"Java Decrypt Fail: {e}", flush=True)
        return f"ERR_DECRYPT_{encrypted_value}"


def call_http_util_api(value: Optional[str]) -> Optional[str]:
    if not value:
        return value

    # path segment 인코딩: '/' 등도 안전하게 인코딩되도록 safe=""
    encoded = quote(str(value), safe="")
    url = f"{HTTP_UTIL_API_BASE}/{HTTP_UTIL_API_MODE}/{encoded}"

    try:
        req = Request(url, method="GET")
        with urlopen(req, timeout=HTTP_UTIL_API_TIMEOUT_SEC) as resp:
            body = resp.read()
        return body.decode("utf-8").strip()
    except (HTTPError, URLError, TimeoutError, ValueError) as e:
        print(f"HTTP util api fail: url={url} err={e}", flush=True)
        return f"ERR_HTTPUTIL_{value}"


# ==========================================================
# 5) Transform
# ==========================================================
def transform_row(row: Dict[str, Any], decrypt_cols: List[str]) -> tuple:
    """
    사용성을 위해 "row dict를 직접 변형"하고 `tuple(row.values())`로 반환합니다.

    주의:
    - DictCursor는 SELECT에서 지정한 컬럼 순서를 유지하는 dict를 반환하는 것이 일반적입니다.
      그래서 `SELECT col1,col2,...` 로 컬럼을 명시하면 `row.values()` 순서도 맞게 떨어집니다.
    """
    for col in decrypt_cols:
        if col in row and row[col]:
            v = row[col]
            if isinstance(v, (bytes, bytearray)):
                try:
                    v = v.decode("utf-8")
                except Exception:
                    v = v.decode("latin1", errors="ignore")

            if isinstance(v, str):
                # 필요 시 base64 decode 후 복호화하도록 확장 가능
                # v = handle_base64(v, "decode")
                v = call_http_util_api(v) if USE_HTTP_UTIL_API else call_java_decrypt(v)
                v = v.strip() if v else v

            row[col] = v

            #row[col] = row[col].strip() if row[col] else row[col]

    return tuple(row.values())


# ==========================================================
# 6) ETL 실행
# ==========================================================
def _etl_process(table_name: str, cfg: Dict[str, Any]) -> None:
    src_conn = None
    tgt_conn = None

    columns: List[str] = list(cfg["columns"])
    decrypt_cols: List[str] = list(cfg.get("decrypt_cols", []))
    range_cfg: Dict[str, Any] = dict(cfg.get("range", {}) or {})

    range_col = range_cfg.get("col")
    start_id = range_cfg.get("start")
    end_id = range_cfg.get("end")

    cols_str = ", ".join(f"`{c}`" for c in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    insert_sql = f"INSERT INTO `{table_name}` ({cols_str}) VALUES ({placeholders})"

    try:
        src_conn = MySQLdb.connect(**SRC)
        tgt_conn = MySQLdb.connect(**TGT)

        src_cur = src_conn.cursor()  # DictCursor
        tgt_cur = tgt_conn.cursor()

        query = f"SELECT {cols_str} FROM `{table_name}`"
        params: List[Any] = []

        if range_col and (start_id is not None or end_id is not None):
            conds = []
            if start_id is not None:
                conds.append(f"`{range_col}` >= %s")
                params.append(start_id)
            if end_id is not None:
                conds.append(f"`{range_col}` <= %s")
                params.append(end_id)
            if conds:
                query += " WHERE " + " AND ".join(conds)

        print(f"[{table_name}] query={query} params={params}", flush=True)
        src_cur.execute(query, tuple(params))

        processed_count = 0
        while True:
            rows = src_cur.fetchmany(CHUNK_SIZE)
            if not rows:
                break

            transformed_rows = [transform_row(r, decrypt_cols) for r in rows]
            try:
                tgt_cur.executemany(insert_sql, transformed_rows)
                tgt_conn.commit()
            except Exception as e:
                tgt_conn.rollback()
                raise RuntimeError(f"[{table_name}] batch insert failed at row {processed_count}: {e}")

            processed_count += len(transformed_rows)
            print(f"   - {table_name}: {processed_count} rows migrated...", flush=True)

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


def main() -> int:
    failed = False
    for table_name, cfg in TABLE_CONFIG.items():
        print(f"\n[START] {table_name}", flush=True)
        try:
            _etl_process(table_name, cfg)
            print(f"[FINISH] {table_name} - Success", flush=True)
        except Exception as e:
            failed = True
            print(f"[ABORT] {table_name} - Error: {e}", flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

