import MySQLdb  # mysqlclient (기존 환경 설치 가정)
import MySQLdb.cursors
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class ConnBase:
    host: str
    port: int
    user: str
    passwd: str
    charset: str = "utf8mb4"

    def as_dict_with_db(self, db: str) -> Dict[str, Any]:
        return {
            "host": self.host,
            "port": int(self.port),
            "user": self.user,
            "passwd": self.passwd,
            "db": db,
            "charset": self.charset,
        }


@dataclass(frozen=True)
class EtlJob:
    schema: str
    tables: List[str]
    chunk_size: int = 5000
    use_sscursor: bool = True
    insert_strategy: str = "REPLACE"  # "INSERT" | "REPLACE" | "INSERT IGNORE"


def transform_data(schema: str, table: str, row: tuple) -> tuple:
    """
    row 변환/복호화 필요 시 여기서 처리하세요.
    """
    return row


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


def run_job(src_base: ConnBase, tgt_base: ConnBase, job: EtlJob) -> int:
    """
    스키마 1개에 대해 지정 테이블들을 select->insert로 적재합니다.
    - 성공: 0
    - 실패: 1
    """
    failed = False
    for table in job.tables:
        src_conn = None
        tgt_conn = None
        try:
            src_conn = MySQLdb.connect(**src_base.as_dict_with_db(job.schema))
            tgt_conn = MySQLdb.connect(**tgt_base.as_dict_with_db(job.schema))

            if job.use_sscursor:
                src_cur = src_conn.cursor(MySQLdb.cursors.SSCursor)
            else:
                src_cur = src_conn.cursor()
            tgt_cur = tgt_conn.cursor()

            src_cur.execute(f"SELECT * FROM {_quote_ident(table)}")

            col_count = len(src_cur.description)
            insert_sql = _build_insert_sql(table=table, col_count=col_count, strategy=job.insert_strategy)

            processed = 0
            while True:
                rows = src_cur.fetchmany(job.chunk_size)
                if not rows:
                    break

                out_rows = [transform_data(job.schema, table, r) for r in rows]
                try:
                    tgt_cur.executemany(insert_sql, out_rows)
                    tgt_conn.commit()
                except Exception as e:
                    tgt_conn.rollback()
                    raise RuntimeError(f"[{job.schema}.{table}] insert failed at row {processed}: {e}")

                processed += len(out_rows)
                print(f"   - {job.schema}.{table}: {processed} rows processed...", flush=True)

            print(f"[OK]   {job.schema}.{table} done (rows={processed})", flush=True)

        except Exception as e:
            failed = True
            print(f"[FAIL] {job.schema}.{table} -> {type(e).__name__}: {e}", flush=True)
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

    return 1 if failed else 0

