from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

try:
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator
except Exception:  # pragma: no cover
    from airflow.operators.dagrun_operator import TriggerDagRunOperator  # type: ignore

# ==========================================================
# LARGE 1·2·3 병렬 오케스트레이터 (하드코딩)
# ==========================================================
# 실행 패턴: large_mysql_copy_1_v1 ∥ _2_v1 ∥ _3_v1 (동시 시작)
#
# Pools (Admin):
#   - large_mysql_runner / slots 3  ← Trigger 3개 child 동시 상한
#   - large_mysql_copy_1~3 / slots 4  ← 각 child 내부 BIGINT 구간·테이블 병렬
#
# 주의: child 는 수억 건 이행 — 3개 동시 실행 시 소스·타겟 DB 부하 확인

LARGE_1_DAG_ID = "large_mysql_copy_1_v1"
LARGE_2_DAG_ID = "large_mysql_copy_2_v1"
LARGE_3_DAG_ID = "large_mysql_copy_3_v1"

_TRIGGER_KW = {
    "execution_date": "{{ execution_date }}",
    "reset_dag_run": True,
    "wait_for_completion": True,
    "poke_interval": 60,
    "pool": "large_mysql_runner",
    "pool_slots": 1,
}


def _trigger(task_id: str, trigger_dag_id: str) -> TriggerDagRunOperator:
    return TriggerDagRunOperator(
        task_id=task_id,
        trigger_dag_id=trigger_dag_id,
        **_TRIGGER_KW,
    )


default_args = {"owner": "large", "retries": 0}

with DAG(
    dag_id="large_mysql_copy_parallel_runner_v1",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    tags=["large", "orchestrator", "parallel"],
    max_active_runs=1,
    max_active_tasks=8,
) as dag:
    run_1 = _trigger("run__large_mysql_copy_1_v1", LARGE_1_DAG_ID)
    run_2 = _trigger("run__large_mysql_copy_2_v1", LARGE_2_DAG_ID)
    run_3 = _trigger("run__large_mysql_copy_3_v1", LARGE_3_DAG_ID)

    all_done = EmptyOperator(task_id="all_parallel_done")
    [run_1, run_2, run_3] >> all_done
