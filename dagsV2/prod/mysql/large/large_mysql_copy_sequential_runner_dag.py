from datetime import datetime

from airflow import DAG

try:
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator
except Exception:  # pragma: no cover
    from airflow.operators.dagrun_operator import TriggerDagRunOperator  # type: ignore

# ==========================================================
# LARGE 1 → 2 → 3 순차 오케스트레이터 (하드코딩)
# ==========================================================
# 한 번에 child 1개만 실행 — DB 부하·락 완화
# wait_for_completion=True + run_1 >> run_2 >> run_3

LARGE_1_DAG_ID = "large_mysql_copy_1_v1"
LARGE_2_DAG_ID = "large_mysql_copy_2_v1"
LARGE_3_DAG_ID = "large_mysql_copy_3_v1"

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
    dag_id="large_mysql_copy_sequential_runner_v1",
    default_args={"owner": "large", "retries": 0},
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    tags=["large", "orchestrator", "sequential"],
    max_active_runs=1,
) as dag:
    run_1 = _trigger("run__large_mysql_copy_1_v1", LARGE_1_DAG_ID)
    run_2 = _trigger("run__large_mysql_copy_2_v1", LARGE_2_DAG_ID)
    run_3 = _trigger("run__large_mysql_copy_3_v1", LARGE_3_DAG_ID)

    run_1 >> run_2 >> run_3
