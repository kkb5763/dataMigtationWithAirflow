-- DIP 테이블 생성 (실제 DDL로 교체)
USE `dip`;

CREATE TABLE IF NOT EXISTS `sample_dip_table` (
  `id` bigint NOT NULL,
  `payload` text NULL,
  `reg_dt` datetime NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
