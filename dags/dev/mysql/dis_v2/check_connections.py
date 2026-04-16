from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import os


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


with DAG(
    dag_id="dis_v2_check_connections",
    start_date=datetime(2026, 4, 2),
    schedule_interval=None,
    catchup=False,
    tags=["CHECK", "DB", "DIS_V2"],
) as dag:
    start = EmptyOperator(task_id="start")

    check_mysql = BashOperator(
        task_id="check_mysql",
        bash_command=f'python "{BASE_DIR}/check_mysql.py"',
    )

    check_postgresql = BashOperator(
        task_id="check_postgresql",
        bash_command=f'python "{BASE_DIR}/check_postgresql.py"',
    )

    # "select + insert" 스모크 테스트(권장: 운영 전에 반드시 1회 확인)
    smoke_mysql_select_insert = BashOperator(
        task_id="smoke_mysql_select_insert",
        bash_command=f'python "{BASE_DIR}/mysql_select_insert_smoke.py"',
    )

    end = EmptyOperator(task_id="end")

    start >> check_mysql >> check_postgresql >> smoke_mysql_select_insert >> end

