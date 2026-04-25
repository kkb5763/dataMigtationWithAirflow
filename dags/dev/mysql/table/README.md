## MySQL 테이블 생성 스크립트

이 디렉토리는 **타겟 MySQL에 테이블을 생성(DDL 적용)** 하기 위한 SQL 파일 모음입니다.

### 디렉토리 규칙
- `dags/dev/mysql/table/_dis/`: DIS 계열 테이블 DDL
- `dags/dev/mysql/table/_dip/`: DIP 계열 테이블 DDL

### 파일 네이밍 권장
스크립트가 많으면 실행 순서를 위해 **접두어 숫자**를 붙이는 것을 권장합니다.

- 예: `001_schema.sql`, `010_tables.sql`, `020_indexes.sql`, `900_views.sql`

### 실행 방식(예시)
Airflow 워커에서 `mysql` CLI로 폴더 내 `.sql`을 정렬해서 순차 실행하는 형태를 권장합니다.

