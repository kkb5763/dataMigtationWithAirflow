from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor

# Airflow 2.x TriggerDagRunOperator location
try:
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator
except Exception:  # pragma: no cover
    # Fallback for older Airflow installs
    from airflow.operators.dagrun_operator import TriggerDagRunOperator  # type: ignore


DEV_DIS_V3_DAG_ID = "dev_dis_v3_v1"
DEV_TIBERO_CHN_DAG_ID = "dev_tibero_to_chn_v1"


default_args = {
    "owner": "dev",
    "retries": 0,
}


with DAG(
    dag_id="dev_sequential_runner_v1",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dev", "orchestrator", "sequential"],
    max_active_runs=1,
) as dag:
    # 1) dev_dis_v3_v1 트리거
    trigger_dis_v3 = TriggerDagRunOperator(
        task_id="trigger_dev_dis_v3",
        trigger_dag_id=DEV_DIS_V3_DAG_ID,
        execution_date="{{ execution_date }}",
        reset_dag_run=True,
    )

    # 2) dev_dis_v3_v1 완료 대기 (동일 execution_date 기준)
    wait_dis_v3 = ExternalTaskSensor(
        task_id="wait_dev_dis_v3_success",
        external_dag_id=DEV_DIS_V3_DAG_ID,
        external_task_id=None,  # wait for DAG run
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 60 * 3,
    )

    # 3) dev_tibero_to_chn_v1 트리거
    trigger_tibero_chn = TriggerDagRunOperator(
        task_id="trigger_dev_tibero_chn",
        trigger_dag_id=DEV_TIBERO_CHN_DAG_ID,
        execution_date="{{ execution_date }}",
        reset_dag_run=True,
    )

    # 4) dev_tibero_to_chn_v1 완료 대기
    wait_tibero_chn = ExternalTaskSensor(
        task_id="wait_dev_tibero_chn_success",
        external_dag_id=DEV_TIBERO_CHN_DAG_ID,
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 60 * 3,
    )

    trigger_dis_v3 >> wait_dis_v3 >> trigger_tibero_chn >> wait_tibero_chn

