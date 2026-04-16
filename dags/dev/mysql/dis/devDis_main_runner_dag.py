from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from dags.dev.mysql.dis.dis_etl_main import EtlConfig, run


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


SCHEMA_TABLES = {
    "dis": ["code_definition"],
    "batct": ["code_definition"],
    "bbi": ["code_definition"],
    "itemdb": ["code_definition"],
    "memberdb": ["mbr_base", "mbr_detail"],
    "cms": ["code_definition"],
}


def _run_schema(schema: str) -> None:
    cfg = EtlConfig(
        src=SRC,
        tgt=TGT,
        schema=schema,
        tables=SCHEMA_TABLES[schema],
        chunk_size=1000,
        use_sscursor=True,
        insert_strategy="INSERT",
    )
    rc = run(cfg)
    if rc != 0:
        raise RuntimeError(f"Schema migration failed: {schema}")


with DAG(
    dag_id="dev_dis_main_runner_v1",
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["migration", "mysql", "dis"],
    max_active_tasks=1,
) as dag:
    for schema in SCHEMA_TABLES.keys():
        PythonOperator(
            task_id=f"run__{schema}",
            python_callable=_run_schema,
            op_kwargs={"schema": schema},
        )

