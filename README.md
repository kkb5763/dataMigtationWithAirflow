# dataMigtationWithAirflow
Airflow with Python으로 수행하는 데이터 이행

## 목표
- Airflow(2.1) + Python으로 통합 데이터 마이그레이션 오케스트레이션
- `full` / `incremental` 로드 지원
- `MySQL -> MySQL` (버전 차이로 인한 `SELECT -> INSERT` 방식) 구현
- 테이블별 증분 기준 컬럼/커서 정의 가능
- 샘플데이터 이행은 별도 DAG로 구성

## 프로젝트 구조(스캐폴딩)
- `dags/`: 마이그레이션 DAG
- `configs/`: 소스/타겟 DB 및 테이블별 규칙(코드 내부 파일로 분리)
- `src/migration/`: 공통 로더/커넥터/워터마크/스키마 동기화
