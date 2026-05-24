from datetime import datetime

from airflow import DAG

try:
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator
except Exception:  # pragma: no cover
    from airflow.operators.dagrun_operator import TriggerDagRunOperator  # type: ignore

# SMALL Tibero 1 → 2 → 3 순차 오케스트레이터

SMALL_1_DAG_ID = "small_tibero_mysql_copy_1_v1"
SMALL_2_DAG_ID = "small_tibero_mysql_copy_2_v1"
SMALL_3_DAG_ID = "small_tibero_mysql_copy_3_v1"

_TRIGGER_KW = {
    "execution_date": "{{ execution_date }}",
    "reset_dag_run": True,
    "wait_for_completion": True,
    "poke_interval": 60,
}


def _trigger(task_id: str, trigger_dag_id: str) -> TriggerDagRunOperator:
    return TriggerDagRunOperator(
        task_id=task_id,
        trigger_dag_id=trigger_dag_id,
        **_TRIGGER_KW,
    )


with DAG(
    dag_id="small_tibero_mysql_copy_sequential_runner_v1",
    default_args={"owner": "small", "retries": 0},
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    tags=["small", "tibero", "orchestrator", "sequential"],
    max_active_runs=1,
) as dag:
    run_1 = _trigger("run__small_tibero_mysql_copy_1_v1", SMALL_1_DAG_ID)
    run_2 = _trigger("run__small_tibero_mysql_copy_2_v1", SMALL_2_DAG_ID)
    run_3 = _trigger("run__small_tibero_mysql_copy_3_v1", SMALL_3_DAG_ID)
    run_1 >> run_2 >> run_3
