## tibero / enc

`sample_tibero_chunk_mig_dag.py` 구간 루프 + `sample_tibero_to_mysql_enc_transform_dag.py` HTTP 변환.

| 항목 | 값 |
|------|-----|
| DAG id | `prod_tibero_mysql_enc_mig_v1` |

```python
"MEMBER.MBR_BASE": {
    "columns": [...],
    "enc_cols": ["ENC_EMAIL"],
    "range": ("MBR_NO", 1, 50_000_000),  # (col, start, end)
},
```

Pool: **prod_tibero_enc_mig** slots=4
