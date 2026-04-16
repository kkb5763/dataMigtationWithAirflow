import sys

from dags.dev.mysql.dis_v2.dis_common_etl import ConnBase, EtlJob, run_job


SRC = ConnBase(
    host="10.10.1.10",
    port=3306,
    user="root",
    passwd="src_mysql_pass123!",
)

TGT = ConnBase(
    host="10.10.1.20",
    port=3306,
    user="root",
    passwd="tgt_mysql_pass456@",
)


JOB = EtlJob(
    schema="cgidis",
    tables=[
        # 예시) 여기에 cgidis 스키마 테이블을 넣으세요
        "code_definition",
    ],
    chunk_size=5000,
    use_sscursor=True,
    insert_strategy="REPLACE",
)


def main() -> int:
    return run_job(SRC, TGT, JOB)


if __name__ == "__main__":
    sys.exit(main())

