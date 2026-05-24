## dagsV2 / prod / tibero

**소스: Tibero 6** (JDBC) → **타겟: MySQL**

구현 기준: `dags/sample/tibero/sample_tibero_chunk_mig_dag.py`

- `TABLE_CONFIG`: `"SCHEMA.TABLE": (range_col, start, end)`
- `while current <= end:` + `STEP_SIZE` 구간마다 **새 SELECT** 실행
- `FETCH_SIZE` 단위 `fetchmany` → MySQL `executemany`

| 구분 | 경로 | 비고 |
|------|------|------|
| SMALL | `small/` | end 값 작게 (소량) · 동일 chunk 패턴 |
| LARGE | `large/` | 수천만~수억 end · `large_tibero_mysql_copy_range_parallel_v1` = 구간당 태스크 |
| ENC | `enc/` | `columns` + `enc_cols` + `range` 튜플 · HTTP API |

### 공통

- `TIBERO_CONFIG`: `thr_jdbc.jar`, `TbDriver`
- `MYSQL_DB_MAP`: Tibero 스키마 → MySQL DB
- 테이블 키: `"SCHEMA.TABLE"`

MySQL→MySQL: `../mysql/`
