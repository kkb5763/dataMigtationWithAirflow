## tibero / small

`sample_tibero_chunk_mig_dag.py` 패턴. 소량은 **end 를 작게** 잡음.

```python
TABLE_CONFIG: Dict[str, Tuple[str, int, int]] = {
    "MEMBER.MBR_CFG": ("CFG_ID", 1, 50_000),
}
```

| DAG | 샘플 3테이블 |
|-----|----------------|
| `small_tibero_mysql_copy_1_v1` | MEMBER.MBR_CFG / MBR_CODE / MBR_EXT |
| `small_tibero_mysql_copy_2_v1` | ORDER.ORD_* |
| `small_tibero_mysql_copy_3_v1` | LOG.* |

Runner: `small_tibero_mysql_copy_*_runner_v1`
