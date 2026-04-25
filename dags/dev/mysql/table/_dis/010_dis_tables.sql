-- DIS (예시) 테이블 생성 DDL 템플릿
-- 실제 컬럼/PK/인덱스/엔진을 업무 스키마에 맞게 채우세요.

CREATE TABLE IF NOT EXISTS `code_definition` (
  `code` varchar(64) NOT NULL,
  `code_name` varchar(255) NULL,
  `use_yn` char(1) NULL,
  `reg_dt` datetime NULL,
  PRIMARY KEY (`code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

