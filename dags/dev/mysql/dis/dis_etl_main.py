import MySQLdb  # mysqlclient 라이브러리 (기존 환경에 설치됨)
import MySQLdb.cursors
import sys
from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass(frozen=True)
class EtlConfig:
    src: Dict[str, Any]
    tgt: Dict[str, Any]
    schema: str
    tables: List[str]
    chunk_size: int = 1000
    use_sscursor: bool = True
    insert_strategy: str = "INSERT"  # "INSERT" | "REPLACE" | "INSERT IGNORE"


def transform_data(row: tuple) -> tuple:
    """
    추출된 행(row) 데이터를 가공하는 함수입니다.
    이곳에 복호화/변환 로직을 넣으시면 됩니다.
    """
    row_list = list(row)
    # row_list[2] = decrypt_function(row_list[2])  # 예시
    return tuple(row_list)


def _quote_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _build_insert_sql(table: str, col_count: int, strategy: str) -> str:
    placeholders = ", ".join(["%s"] * col_count)
    if strategy == "INSERT":
        return f"INSERT INTO {_quote_ident(table)} VALUES ({placeholders})"
    if strategy == "REPLACE":
        return f"REPLACE INTO {_quote_ident(table)} VALUES ({placeholders})"
    if strategy == "INSERT IGNORE":
        return f"INSERT IGNORE INTO {_quote_ident(table)} VALUES ({placeholders})"
    raise ValueError(f"Unsupported insert_strategy: {strategy}")


def _etl_process(cfg: EtlConfig, table: str) -> None:
    src_conn = None
    tgt_conn = None

    try:
        src = dict(cfg.src)
        tgt = dict(cfg.tgt)
        src["db"] = cfg.schema
        tgt["db"] = cfg.schema

        src_conn = MySQLdb.connect(**src)
        tgt_conn = MySQLdb.connect(**tgt)

        if cfg.use_sscursor:
            src_cur = src_conn.cursor(MySQLdb.cursors.SSCursor)
        else:
            src_cur = src_conn.cursor()
        tgt_cur = tgt_conn.cursor()

        src_cur.execute(f"SELECT * FROM {_quote_ident(table)}")

        col_count = len(src_cur.description)
        insert_sql = _build_insert_sql(table=table, col_count=col_count, strategy=cfg.insert_strategy)

        processed_count = 0
        while True:
            rows = src_cur.fetchmany(cfg.chunk_size)
            if not rows:
                break

            transformed_rows = [transform_data(r) for r in rows]

            try:
                tgt_cur.executemany(insert_sql, transformed_rows)
                tgt_conn.commit()
            except Exception as e:
                tgt_conn.rollback()
                raise RuntimeError(f"[{cfg.schema}.{table}] insert failed at row {processed_count}: {e}")

            processed_count += len(transformed_rows)
            print(f"   - {cfg.schema}.{table}: {processed_count} rows processed...", flush=True)

    finally:
        if src_conn:
            try:
                src_conn.close()
            except Exception:
                pass
        if tgt_conn:
            try:
                tgt_conn.close()
            except Exception:
                pass


def run(cfg: EtlConfig) -> int:
    failed = False
    for table in cfg.tables:
        try:
            print(f"[START ETL] {cfg.schema}.{table}", flush=True)
            _etl_process(cfg, table)
            print(f"[OK]        {cfg.schema}.{table}", flush=True)
        except Exception as e:
            failed = True
            print(f"[FAIL]      {cfg.schema}.{table} -> Error: {e}", flush=True)
    return 1 if failed else 0


def main() -> int:
    # 로컬 테스트용 기본 값 (필요 시 수정)
    SRC = {
        "host": "10.10.1.10",
        "port": 3306,
        "user": "root",
        "passwd": "src_mysql_pass123!",
        "charset": "utf8mb4",
    }

    TGT = {
        "host": "10.10.1.20",
        "port": 3306,
        "user": "root",
        "passwd": "tgt_mysql_pass456@",
        "charset": "utf8mb4",
    }

    cfg = EtlConfig(
        src=SRC,
        tgt=TGT,
        schema="memberdb",
        tables=["mbr_base", "mbr_detail"],
        chunk_size=1000,
        use_sscursor=True,
        insert_strategy="INSERT",
    )
    return run(cfg)


if __name__ == "__main__":
    sys.exit(main())

