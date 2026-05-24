## tibero / large

`sample_tibero_chunk_mig_dag.py` — `STEP_SIZE=100_000`, 테이블당 `_worker` 1개가 전 구간 루프.

| DAG | 샘플 |
|-----|------|
| `large_tibero_mysql_copy_1_v1` | MEMBER.BIG_* (~3천만~8천만) |
| `large_tibero_mysql_copy_2_v1` | ORDER.BIG_* |
| `large_tibero_mysql_copy_3_v1` | LOG.BIG_* |

`large_tibero_mysql_copy_range_parallel_v1`: sample 루프 **1구간 = 태스크 1개**, `STEP_SIZE=10_000_000`
