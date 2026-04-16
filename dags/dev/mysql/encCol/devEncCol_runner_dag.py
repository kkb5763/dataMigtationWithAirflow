import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


ENC_DIR = os.path.dirname(os.path.abspath(__file__))
ETL_SCRIPT = os.path.join(ENC_DIR, "enccol_etl.py")


with DAG(
    dag_id="dev_encCol_runner_v1",
    start_date=datetime(2026, 4, 2),
    schedule_interval=None,
    catchup=False,
    tags=["MIG", "MYSQL", "ENCCOL"],
) as dag:
    start = EmptyOperator(task_id="start")

    run_etl = BashOperator(
        task_id="run_enccol_etl",
        bash_command=f'python "{ETL_SCRIPT}"',
    )

    end = EmptyOperator(task_id="end")

    start >> run_etl >> end

