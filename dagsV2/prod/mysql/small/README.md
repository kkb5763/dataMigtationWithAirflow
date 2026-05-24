## dagsV2 / prod / mysql / small

소스·타겟 **동일 DB·동일 테이블 구조** → `SELECT *` 적재. **TABLE_LIST** 만 수정.

| DAG id | 파일 | Pool (slots=4) |
|--------|------|----------------|
| `small_mysql_copy_1_v1` | `small_1_mysql_copy_mig_dag.py` | `small_mysql_copy_1` |
| `small_mysql_copy_2_v1` | `small_2_mysql_copy_mig_dag.py` | `small_mysql_copy_2` |
| `small_mysql_copy_3_v1` | `small_3_mysql_copy_mig_dag.py` | `small_mysql_copy_3` |
| `small_mysql_copy_mig_v1` | `small_mysql_enc_mig_dag.py` | `small_mysql_copy` (통합용, 선택) |

5개 테이블을 3개 DAG로 나눌 때 예: 1번 DAG에 2개, 2번에 2개, 3번에 1개.

### 오케스트레이터 (1·2·3 실행 패턴)

| DAG id | 파일 | 실행 순서 |
|--------|------|-----------|
| `small_mysql_copy_parallel_runner_v1` | `small_mysql_copy_parallel_runner_dag.py` | 1 ∥ 2 ∥ 3 |
| `small_mysql_copy_sequential_runner_v1` | `small_mysql_copy_sequential_runner_dag.py` | 1 → 2 → 3 |
| `small_mysql_copy_parallel_12_then_3_runner_v1` | `small_mysql_copy_parallel_12_then_3_runner_dag.py` | (1 ∥ 2) → 3 |

Pool (Admin): `small_mysql_runner` — 병렬·12→3 패턴에서 slots **3**(전체 병렬) 또는 **2**(1·2만 동시).

암호화 컬럼 이행은 `../enc/prod_mysql_enc_mig_v1`.
