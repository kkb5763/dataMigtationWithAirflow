## dags/table — MySQL DDL

관리자 계정 + SSL 로 타겟 MySQL에 **스키마·테이블**을 생성합니다.

### 서버 DDL 경로

`/data/app/airflow/dataMigtationWithAirflow/table/mysql/<ddl_dir>/`

### DAG 설정 (`apply_mysql_ddl_dag.py`)

| 블록 | 내용 |
|------|------|
| `MYSQL_ADMIN` | host, port, user, pass (관리자) |
| `MYSQL_SSL` | enabled, mode, ca, cert, key |
| `DDL_SCHEMA_PLANS` | 스키마명 → `ddl_dir`, `connect_database` |

스키마 추가 예:

```python
DDL_SCHEMA_PLANS = {
    "dis": {"ddl_dir": "_dis", "connect_database": "mysql"},
    "bbi": {"ddl_dir": "_bbi", "connect_database": "mysql"},
}
```

### SQL 폴더 예 (`_dis`)

- `001_dis_schema.sql` — `CREATE DATABASE IF NOT EXISTS dis …`
- `010_dis_tables.sql` — `USE dis;` + `CREATE TABLE …`

### 실행

- DAG: `table_mysql_apply_ddl_v1`
- 스키마당 태스크 1개, 최대 4개 동시 (`MAX_PARALLEL_SCHEMAS`)
- Pool: `table_mysql_ddl`, slots `4`
