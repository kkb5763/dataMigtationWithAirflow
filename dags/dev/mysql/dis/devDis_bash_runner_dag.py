import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


# `dis_etl_main.py`를 BashOperator로 호출하는 DAG
DIS_DIR = os.path.dirname(os.path.abspath(__file__))
ETL_SCRIPT = os.path.join(DIS_DIR, "dis_etl_main.py")


with DAG(
    dag_id="dev_dis_bash_runner_v1",
    start_date=datetime(2026, 4, 2),
    schedule_interval=None,
    catchup=False,
    tags=["MIG", "DIS", "MYSQL"],
) as dag:
    start = EmptyOperator(task_id="start")

    run_dis_etl = BashOperator(
        task_id="run_dis_etl_main",
        bash_command=f'python "{ETL_SCRIPT}"',
    )

    start >> run_dis_etl

