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
| `large_4_mysql_copy_single_table_v1` | `large_4_mysql_copy_single_table_dag.py` | `large_mysql_copy_4` |
| `large_5_mysql_native_dump_v1` | `large_5_mysql_native_dump_dag.py` | `large_mysql_native_dump` |
| `large_6_mysql_composite_index_split_v1` | `large_6_mysql_composite_index_split_dag.py` | `large_mysql_composite_split` |
| `large_7_mysql_recid_parallel_v1` | `large_7_mysql_recid_parallel_dag.py` | `large_mysql_recid_parallel` |

각 child DAG 에 **샘플 테이블 3개**씩 들어 있음 (실제명·PK·end 는 환경에 맞게 수정).

| DAG | 샘플 테이블 (건수 예) |
|-----|------------------------|
| `large_mysql_copy_1_v1` | `big_mbr_base` ~5천만, `big_mbr_hist` ~8천만 |
| `large_mysql_copy_2_v1` | `big_ord_mst` ~1.2억, `big_ord_dtl` ~2억, `big_ord_pay` ~6천만 |
| `large_mysql_copy_3_v1` | `big_act_log` ~3억, `big_audit_trail` ~1.5억, `big_msg_queue` ~4천만 |
| `large_4_mysql_copy_single_table_v1` | `big_mbr_addr` 처럼 key/range select 이슈가 있는 단일 테이블 |
| `large_5_mysql_native_dump_v1` | key 없는 테이블을 MySQL 네이티브 dump stream 으로 이행 |
| `large_6_mysql_composite_index_split_v1` | 복합 인덱스 + 날짜 조건으로 plan 별 날짜 구간 이행 |
| `large_7_mysql_recid_parallel_v1` | recid BIGINT 1~4억 구간을 한 DAG 안에서 동시 10개 이행 |

- `STEP_SIZE` 기본 **100_000** → 4억 건·테이블 1개면 migrate 태스크 1개 안에서 약 4000회 SELECT
- 테이블 3개 → `max_active_tasks=4` 이므로 **3개 테이블 동시** 이행 가능

### key 없거나 range select 가 느린 단일 테이블

| DAG id | 파일 | 특징 |
|--------|------|------|
| `large_4_mysql_copy_single_table_v1` | `large_4_mysql_copy_single_table_dag.py` | 기본 `row_id` 모드: 보조 row id 컬럼으로 `BETWEEN` 분할 |
| `large_5_mysql_native_dump_v1` | `large_5_mysql_native_dump_dag.py` | `mysqldump --quick --no-create-info | mysql` 네이티브 스트리밍 |

- `mode="row_id"`: 소스 테이블에 `_mig_row_id` 같은 보조 컬럼/인덱스를 준비한 뒤 `BETWEEN` 분할.
- `prepare_row_id=True`: 소스에 보조 컬럼을 만들고 값을 채운 뒤 인덱스를 생성. DDL/UPDATE가 들어가므로 운영 승인 후 1회만 사용.
- `prepare_row_id=False`: 보조 컬럼이 이미 준비되어 있다고 보고 이관만 수행.
- `mode="range"`: 나중에 사용할 수 있는 기존 key 가 생기면 `range.col/start/end` 로 분할 가능.
- row_id 컬럼은 자동으로 SELECT/INSERT 컬럼에서 제외됨.

`large_5` 는 소스 테이블을 변경하지 않는 가장 단순한 대안:

- Airflow/Python 이 row 를 직접 fetch/insert 하지 않고 MySQL client 가 스트리밍.
- 기본은 `truncate_target=True` 후 INSERT dump stream 적재.
- 여전히 full scan 이므로 DB I/O 부하는 있음. 저부하 시간대 실행 권장.
- `source_table == target_table` 인 동일 구조 이행용.

`large_6` 는 복합 인덱스가 있는 단일 테이블용:

- `split_plans` 에 인덱스별 `FORCE INDEX`, `date_col`, 날짜 범위, 추가 `where` 조건 지정.
- BIGINT + 날짜 복합 인덱스는 `bigint_col/start/end/step` 을 추가해 날짜 구간 안에서 BIGINT 구간까지 분할.
- Airflow 태스크 1개 = plan 1개 + 날짜 구간 1개 + 선택적 BIGINT 구간 1개.
- 날짜 조건은 `[start, end)` 방식 (`date_col >= start AND date_col < end`)이라 구간 중복을 피함.
- BIGINT 조건은 `bigint_col BETWEEN start AND end` 방식.
- plan 조건이 서로 겹치면 target 중복 적재 가능. `where`/날짜 범위를 겹치지 않게 설계.

`large_7` 은 `recid` BIGINT 가 1~4억까지 잡힌 테이블용:

- `STEP_SIZE=10_000_000` 기준 약 40개 태스크 생성.
- `max_active_tasks=10`, Pool `large_mysql_recid_parallel` slots=10 으로 동시 10개 구간 실행.
- 각 태스크는 `WHERE recid BETWEEN start AND end` 로 독립 SELECT/INSERT.
- 소스/타겟 DB 부하가 크면 Pool slots 또는 `max_active_tasks` 를 낮춰 조절.

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
| `large_mysql_copy_4` | 1 | key/range 이슈 단일 테이블 |
| `large_mysql_native_dump` | 1 | mysqldump pipe 기반 네이티브 이행 |
| `large_mysql_composite_split` | 4 | 복합 인덱스 + 날짜 조건 분할 |
| `large_mysql_recid_parallel` | 10 | recid 구간 동시 10개 이행 |
| `large_mysql_range` | 4 | 구간 태스크 병렬 |
| `large_mysql_runner` | 3 or 2 | 오케스트레이터 |
