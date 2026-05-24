import MySQLdb  # MySQL 클라이언트 (mysqlclient)
import MySQLdb.cursors  # DictCursor 등 커서 타입

from airflow import DAG  # DAG 정의
from airflow.operators.python import PythonOperator  # Python 함수 실행 태스크
from datetime import datetime, timedelta  # DAG 시작일·재시도 지연
from typing import Any, Dict, List, Optional  # 설정 dict 타입 힌트
from urllib.error import HTTPError, URLError  # HTTP API 호출 예외
from urllib.parse import quote  # URL path 인코딩(평문 값)
from urllib.request import Request, urlopen  # HTTP GET 호출

# ==========================================================
# PROD: MySQL -> MySQL (암호화 컬럼 변환 이행)
# ==========================================================
# - 소스 행의 enc_cols 를 HTTP 유틸 API 로 변환한 뒤 타겟에 INSERT/REPLACE
# - API 예: GET http://127.0.0.1:8082/base64/enc/<plain>
# - 테이블 추가: TABLE_CONFIG 에 항목 추가 → migrate_<table> 태스크 자동 생성

# --- MySQL 소스 (읽기) ---
SRC_MYSQL: Dict[str, Any] = {
    "host": "10.10.1.10",  # 소스 DB 호스트
    "port": 3306,  # 소스 DB 포트
    "user": "root",  # 소스 DB 계정
    "passwd": "src_mysql_pass123!",  # 소스 DB 비밀번호
    "db": "member_db",  # 소스 스키마(데이터베이스)명
    "charset": "utf8mb4",  # 문자셋
}

# --- MySQL 타겟 (쓰기) ---
TGT_MYSQL: Dict[str, Any] = {
    "host": "10.10.1.20",  # 타겟 DB 호스트
    "port": 3306,  # 타겟 DB 포트
    "user": "root",  # 타겟 DB 계정
    "passwd": "tgt_mysql_pass456@",  # 타겟 DB 비밀번호
    "db": "member_db",  # 타겟 스키마명
    "charset": "utf8mb4",  # 문자셋
}

# --- HTTP 유틸 (암호화/인코딩 변환 서버) ---
HTTP_UTIL_API_BASE = "http://127.0.0.1:8082"  # API 베이스 URL (워커에서 접근 가능해야 함)
HTTP_UTIL_API_MODE = "base64/enc"  # 경로 모드: base64/enc | base64/dec | mock/enc
HTTP_UTIL_API_TIMEOUT_SEC = 5  # API 호출 타임아웃(초)

# --- 이관 배치 옵션 ---
STEP_SIZE = 100_000  # BIGINT PK 구간 폭(한 번의 SELECT 가 커버하는 id 범위, 4억 건 → 약 4000회)
CHUNK_SIZE = 5000  # fetchmany 한 번에 읽을 행 수(메모리·커밋 단위)
INSERT_STRATEGY = "REPLACE"  # REPLACE | INSERT | INSERT IGNORE

# --- 테이블(태스크) 동시 실행: 5개 테이블이면 최대 4개까지 병렬 (Pools: prod_enc_mig, slots=4) ---

# --- 테이블별 이관 정의 ---
# columns: SELECT·INSERT 컬럼 목록(순서 = row.values() 순서)
# enc_cols: API 변환을 적용할 컬럼명 목록
# range.col: BIGINT PK 컬럼명 / start·end: 전체 이관 id 범위 (정수일 때 STEP_SIZE 로 자동 분할)
TABLE_CONFIG: Dict[str, Dict[str, Any]] = {
    "mbr_base": {
        "columns": ["mbr_no", "mbr_id", "mbr_nm", "enc_email"],
        "enc_cols": ["enc_email"],
        "range": {"col": "mbr_no", "start": 1, "end": 400_000_000},  # 예: 4억 건 PK 범위
    },
}


def _quote_ident(name: str) -> str:
    """MySQL 식별자(테이블/컬럼명)를 백틱으로 감싸 SQL 인젝션·예약어 충돌 방지."""
    return "`" + name.replace("`", "``") + "`"


def _iter_bigint_ranges(
    start_id: int,
    end_id: int,
    step: int,
) -> List[tuple]:
    """BIGINT PK 를 [start..end] 구간으로 step 크기만큼 잘라 (current, current_end) 리스트 반환."""
    ranges: List[tuple] = []
    current = int(start_id)
    end_i = int(end_id)
    step_i = int(step)
    while current <= end_i:
        current_end = min(current + step_i - 1, end_i)
        ranges.append((current, current_end))
        current += step_i
    return ranges


def _call_http_util_api(value: Optional[str]) -> Optional[str]:
    """컬럼 값 1개를 HTTP GET 으로 변환. 실패 시 ERR_HTTPUTIL_<원본> 반환."""
    if not value:
        return value
    encoded = quote(str(value), safe="")  # URL path segment 로 안전하게 인코딩
    url = f"{HTTP_UTIL_API_BASE.rstrip('/')}/{HTTP_UTIL_API_MODE.strip('/')}/{encoded}"
    try:
        req = Request(url, method="GET")
        with urlopen(req, timeout=HTTP_UTIL_API_TIMEOUT_SEC) as resp:
            body = resp.read()
        return body.decode("utf-8").strip()  # API 응답 본문(변환된 문자열)
    except (HTTPError, URLError, TimeoutError, ValueError) as e:
        print(f"HTTP util api fail: url={url} err={e}", flush=True)
        return f"ERR_HTTPUTIL_{value}"  # 실패해도 행 전체는 계속 적재(값으로 표시)


def _transform_row(row: Dict[str, Any], enc_cols: List[str]) -> tuple:
    """DictCursor 행에서 enc_cols 만 API 변환 후 INSERT용 tuple 반환."""
    for col in enc_cols:
        if col not in row or not row[col]:
            continue  # NULL/빈 값은 스킵
        v = row[col]
        if isinstance(v, (bytes, bytearray)):
            try:
                v = v.decode("utf-8")
            except Exception:
                v = v.decode("latin1", errors="ignore")  # 바이너리 fallback
        if isinstance(v, str):
            row[col] = _call_http_util_api(v)  # 변환 결과로 컬럼 덮어쓰기
    return tuple(row.values())  # SELECT 컬럼 순서와 동일하게 tuple


def _mysql_enc_etl(table: str, **context: Any) -> None:
    """테이블 1개에 대해 소스 SELECT → 변환 → 타겟 적재 (Airflow PythonOperator 진입점)."""
    cfg = TABLE_CONFIG[table]
    columns: List[str] = list(cfg["columns"])
    enc_cols: List[str] = list(cfg.get("enc_cols", []))
    range_cfg: Dict[str, Any] = dict(cfg.get("range", {}) or {})

    range_col = range_cfg.get("col")  # 범위 조건 컬럼명
    start_id = range_cfg.get("start")  # 범위 시작값 (None 가능)
    end_id = range_cfg.get("end")  # 범위 끝값 (None 가능)

    src_cfg = dict(SRC_MYSQL)
    # 서버 사이드 커서 + dict (대용량에서 클라이언트 메모리 폭주 방지)
    src_cfg["cursorclass"] = MySQLdb.cursors.SSDictCursor
    tgt_cfg = dict(TGT_MYSQL)

    cols_str = ", ".join(_quote_ident(c) for c in columns)
    placeholders = ", ".join(["%s"] * len(columns))  # executemany 바인딩

    if INSERT_STRATEGY == "REPLACE":
        insert_sql = f"REPLACE INTO {_quote_ident(table)} ({cols_str}) VALUES ({placeholders})"
    elif INSERT_STRATEGY == "INSERT":
        insert_sql = f"INSERT INTO {_quote_ident(table)} ({cols_str}) VALUES ({placeholders})"
    elif INSERT_STRATEGY == "INSERT IGNORE":
        insert_sql = f"INSERT IGNORE INTO {_quote_ident(table)} ({cols_str}) VALUES ({placeholders})"
    else:
        raise ValueError(f"Unsupported INSERT_STRATEGY: {INSERT_STRATEGY}")

    # BIGINT start/end 가 있으면 STEP_SIZE 단위로 쿼리 분할 (4억 건 대응)
    if (
        range_col
        and start_id is not None
        and end_id is not None
        and isinstance(start_id, int)
        and isinstance(end_id, int)
    ):
        id_ranges = _iter_bigint_ranges(int(start_id), int(end_id), STEP_SIZE)
    else:
        id_ranges = [(start_id, end_id)]  # 비정수·부분 범위는 1회 쿼리

    src_conn = None
    tgt_conn = None
    try:
        src_conn = MySQLdb.connect(**src_cfg)
        tgt_conn = MySQLdb.connect(**tgt_cfg)
        tgt_cur = tgt_conn.cursor()

        total = 0
        for r_start, r_end in id_ranges:
            src_cur = src_conn.cursor()  # 구간마다 커서 새로 (SSCursor 특성)
            query = f"SELECT {cols_str} FROM {_quote_ident(table)}"
            params: List[Any] = []

            if range_col and r_start is not None and r_end is not None and isinstance(r_start, int) and isinstance(r_end, int):
                query += f" WHERE {_quote_ident(range_col)} BETWEEN %s AND %s"
                params = [r_start, r_end]
            elif range_col and r_start is not None:
                query += f" WHERE {_quote_ident(range_col)} >= %s"
                params = [r_start]
            elif range_col and r_end is not None:
                query += f" WHERE {_quote_ident(range_col)} <= %s"
                params = [r_end]

            print(f"[{table}] range {r_start}~{r_end} query={query} params={params}", flush=True)
            src_cur.execute(query, tuple(params))

            chunk_in_range = 0
            while True:
                rows = src_cur.fetchmany(CHUNK_SIZE)
                if not rows:
                    break
                batch = [_transform_row(r, enc_cols) for r in rows]
                tgt_cur.executemany(insert_sql, batch)
                tgt_conn.commit()
                chunk_in_range += len(batch)
                total += len(batch)

            try:
                src_cur.close()
            except Exception:
                pass
            print(
                f" >>> [{table}] range {r_start}~{r_end} done (rows={chunk_in_range}, total={total})",
                flush=True,
            )

    except Exception as e:
        print(f"!!! [{table}] 이관 실패: {e}", flush=True)
        raise  # 태스크 실패로 표시
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
    "owner": "prod",  # DAG 소유자
    "retries": 1,  # 태스크 실패 시 1회 재시도
    "retry_delay": timedelta(minutes=5),  # 재시도 전 대기 5분
}

with DAG(
    dag_id="prod_mysql_enc_mig_v1",  # UI 에 표시되는 DAG ID
    default_args=default_args,  # 위 기본값을 모든 태스크에 적용
    start_date=datetime(2026, 4, 1),  # DAG Run 논리 시작일
    schedule_interval=None,  # 자동 스케줄 없음(수동 Trigger)
    catchup=False,  # 과거 catchup 비활성
    tags=["prod", "mysql", "enc", "migration"],  # UI 필터 태그
    max_active_tasks=4,  # TABLE_CONFIG 5개일 때 최대 4개 migrate_* 태스크 동시 실행
) as dag:
    for table_name in TABLE_CONFIG:  # TABLE_CONFIG 키마다 태스크 1개(서로 의존성 없음 → 병렬 가능)
        PythonOperator(
            task_id=f"migrate_{table_name}",  # 그래프 노드 이름
            python_callable=_mysql_enc_etl,  # 실행할 함수
            pool="prod_enc_mig",  # 동시 실행 상한(Pool slots=4 와 맞출 것)
            pool_slots=1,  # 이 태스크가 Pool 에서 차지하는 슬롯
            op_kwargs={"table": table_name},  # 함수 인자: 어떤 테이블을 이관할지
        )
