-- DIP (예시) 테이블 생성 DDL 템플릿
-- 실제 테이블 정의로 교체하세요.

CREATE TABLE IF NOT EXISTS `sample_dip_table` (
  `id` bigint NOT NULL,
  `payload` text NULL,
  `reg_dt` datetime NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

