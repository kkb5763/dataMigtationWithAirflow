## dagsV2 / prod / mysql / large

**수천만 ~ 수억 건** MySQL → MySQL 이행. SMALL 과 달리 **전체 `SELECT *` 금지**, BIGINT PK 구간 분할 필수.

### SMALL vs LARGE

| | SMALL | LARGE |
|---|--------|--------|
| 규모 | 소량 (전체 스캔 가능) | 수천만 ~ 수억 |
| 읽기 | `SELECT *` 한 번 | `WHERE pk BETWEEN …` 반복 (`STEP_SIZE`) |
| 커서 | `SSCursor` | `SSCursor` |
| 설정 | `TABLE_LIST` (테이블명만) | `TABLE_CONFIG` + `range.col/start/end` |
| 재시도 | 0 | 1 (5분 후) |

### Child DAG (테이블 그룹 1·2·3)

| DAG id | 파일 | Pool |
|--------|------|------|
| `large_mysql_copy_1_v1` | `large_1_mysql_copy_mig_dag.py` | `large_mysql_copy_1` |
| `large_mysql_copy_2_v1` | `large_2_mysql_copy_mig_dag.py` | `large_mysql_copy_2` |
| `large_mysql_copy_3_v1` | `large_3_mysql_copy_mig_dag.py` | `large_mysql_copy_3` |

각 child DAG 에 **샘플 테이블 3개**씩 들어 있음 (실제명·PK·end 는 환경에 맞게 수정).

| DAG | 샘플 테이블 (건수 예) |
|-----|------------------------|
| `large_mysql_copy_1_v1` | `big_mbr_base` ~5천만, `big_mbr_hist` ~8천만, `big_mbr_addr` ~3천만 |
| `large_mysql_copy_2_v1` | `big_ord_mst` ~1.2억, `big_ord_dtl` ~2억, `big_ord_pay` ~6천만 |
| `large_mysql_copy_3_v1` | `big_act_log` ~3억, `big_audit_trail` ~1.5억, `big_msg_queue` ~4천만 |

- `STEP_SIZE` 기본 **100_000** → 4억 건·테이블 1개면 migrate 태스크 1개 안에서 약 4000회 SELECT
- 테이블 3개 → `max_active_tasks=4` 이므로 **3개 테이블 동시** 이행 가능

### 초대형 테이블 1개 (구간별 Airflow 태스크)

| DAG id | 파일 | Pool |
|--------|------|------|
| `large_mysql_copy_range_parallel_v1` | `large_mysql_copy_range_parallel_dag.py` | `large_mysql_range` |

- PK 구간마다 `migrate_<table>_<start>_<end>` 태스크 생성 → **구간 최대 4개 동시**
- `STEP_SIZE` 기본 **10_000_000** (태스크 수 줄이기). 4억 건 → 약 40 태스크

### 오케스트레이터 (1·2·3)

| DAG id | 패턴 |
|--------|------|
| `large_mysql_copy_parallel_runner_v1` | 1 ∥ 2 ∥ 3 |
| `large_mysql_copy_sequential_runner_v1` | 1 → 2 → 3 |
| `large_mysql_copy_parallel_12_then_3_runner_v1` | (1 ∥ 2) → 3 |

Pool: `large_mysql_runner` (병렬 3 / 12→3 은 slots 2)

### 암호화 컬럼

동일 PK 구간·HTTP 변환 로직: `../enc/prod_mysql_enc_mig_v1`  
수억 건 + 행 단위 HTTP 는 병목 → 배치 API·JAR 검토.

### Pools (Admin)

| Pool | slots | 용도 |
|------|-------|------|
| `large_mysql_copy_1` ~ `_3` | 4 | child 내부 테이블 병렬 |
| `large_mysql_range` | 4 | 구간 태스크 병렬 |
| `large_mysql_runner` | 3 or 2 | 오케스트레이터 |
