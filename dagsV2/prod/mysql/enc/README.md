## dagsV2 / prod / mysql / enc

운영(MySQL → MySQL) 암호화 컬럼 이행 DAG.

| 항목 | 값 |
|------|-----|
| DAG id | `prod_mysql_enc_mig_v1` |
| 파일 | `prod_mysql_enc_mig_dag.py` |

### 설정 (파일 상단 하드코딩)

- `SRC_MYSQL` / `TGT_MYSQL` — 접속 정보
- `HTTP_UTIL_API_BASE` / `HTTP_UTIL_API_MODE` — 변환 API (워커에서 접근 가능해야 함)
- `STEP_SIZE` — BIGINT PK 구간 폭 (기본 10만)
- `TABLE_CONFIG` — 테이블·컬럼·`enc_cols`·`range` (`start`/`end` 정수)

### BIGINT 대용량 나누기

| 방법 | 설명 |
|------|------|
| **STEP_SIZE 루프** (현재) | `id BETWEEN n AND n+STEP-1` 반복 |
| **구간별 Airflow 태스크** | 구간마다 태스크 + `max_active_tasks` 병렬 |
| **paraller** | 스키마/테이블별 DAG 를 pool 로 동시 실행 |

행 단위 HTTP 변환은 4억 건에서 API 병목 — 배치/JAR 검토 필요.

### 테이블 병렬 (5개 중 4개)

- `max_active_tasks=4`, `pool=prod_enc_mig`, `pool_slots=1`
- Admin → Pools: **prod_enc_mig**, **slots=4**

### Airflow

`dagsV2` 가 스캔 경로에 포함되어 있어야 UI 에 DAG 가 보입니다 (`airflow.cfg` 의 `dags_folder` 또는 심볼릭 링크).
