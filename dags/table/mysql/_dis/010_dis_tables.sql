-- DIS 테이블 생성 (실제 DDL로 교체)
USE `dis`;

CREATE TABLE IF NOT EXISTS `code_definition` (
  `code` varchar(64) NOT NULL,
  `code_name` varchar(255) NULL,
  `use_yn` char(1) NULL,
  `reg_dt` datetime NULL,
  PRIMARY KEY (`code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
