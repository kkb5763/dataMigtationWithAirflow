import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


with DAG(
    dag_id="dev_dis_v2_runner_v1",
    start_date=datetime(2026, 4, 2),
    schedule_interval=None,
    catchup=False,
    tags=["MIG", "MYSQL", "DIS_V2"],
) as dag:
    start = EmptyOperator(task_id="start")

    run_bbi = BashOperator(
        task_id="run_bbi",
        bash_command=f'python "{BASE_DIR}/bbi_etl.py"',
    )

    run_cgidis = BashOperator(
        task_id="run_cgidis",
        bash_command=f'python "{BASE_DIR}/cgidis_etl.py"',
    )

    run_itemdb = BashOperator(
        task_id="run_itemdb",
        bash_command=f'python "{BASE_DIR}/itemdb_etl.py"',
    )

    end = EmptyOperator(task_id="end")

    start >> run_bbi >> run_cgidis >> run_itemdb >> end

