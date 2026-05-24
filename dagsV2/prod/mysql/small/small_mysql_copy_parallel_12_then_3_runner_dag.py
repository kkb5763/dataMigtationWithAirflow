from datetime import datetime
from typing import Optional

from airflow import DAG

try:
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator
except Exception:  # pragma: no cover
    from airflow.operators.dagrun_operator import TriggerDagRunOperator  # type: ignore

# ==========================================================
# SMALL (1·2 병렬) → 3 순차 오케스트레이터 (하드코딩)
# ==========================================================
# 실행 패턴:
#   [1 ∥ 2]  — small_mysql_copy_1_v1 과 _2_v1 동시 시작
#      ↓
#     3      — 1·2 둘 다 success 후 small_mysql_copy_3_v1 시작
#
# Pools (Admin):
#   - small_mysql_runner / slots 2  ← run_1·run_2 Trigger 만 (1·2 동시 상한)
#   - run_3 은 Pool 없음 (1·2 완료 후 단독 실행)
#
# Airflow 의존: [run_1, run_2] >> run_3
#   → run_3 은 upstream 인 run_1, run_2 가 모두 success 일 때만 스케줄됨
#
# 사용: 1·2 그룹은 병렬로 빠르게, 3 그룹은 앞 그룹 완료 후 실행할 때

# --- 트리거 대상 child DAG id ---
SMALL_1_DAG_ID = "small_mysql_copy_1_v1"
SMALL_2_DAG_ID = "small_mysql_copy_2_v1"
SMALL_3_DAG_ID = "small_mysql_copy_3_v1"

# TriggerDagRunOperator 기본 옵션 (pool 은 태스크별로 덮어씀)
_TRIGGER_KW = {
    "execution_date": "{{ execution_date }}",
    "reset_dag_run": True,
    "wait_for_completion": True,  # 1·2 완료 대기 후에야 run_3 이 success 로 간주 가능
    "poke_interval": 60,
}


def _trigger(
    task_id: str,
    trigger_dag_id: str,
    *,
    pool: Optional[str] = None,
) -> TriggerDagRunOperator:
    """
    TriggerDagRunOperator 생성.
    pool 지정 시: 해당 Pool 슬롯으로 동시 child 수 제한 (1·2 병렬용).
    pool 미지정: 순차 단계(3) 또는 Pool 제한 없이 실행.
    """
    kw = dict(_TRIGGER_KW)
    if pool:
        kw["pool"] = pool
        kw["pool_slots"] = 1
    return TriggerDagRunOperator(
        task_id=task_id,
        trigger_dag_id=trigger_dag_id,
        **kw,
    )


default_args = {
    "owner": "small",
    "retries": 0,
}

with DAG(
    dag_id="small_mysql_copy_parallel_12_then_3_runner_v1",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    tags=["small", "orchestrator", "parallel", "sequential"],
    max_active_runs=1,
) as dag:
    # 1단계(병렬): child 1 — small_mysql_runner Pool 로 1·2 동시 실행 상한
    run_1 = _trigger(
        task_id="run__small_mysql_copy_1_v1",
        trigger_dag_id=SMALL_1_DAG_ID,
        pool="small_mysql_runner",  # slots=2 권장
    )

    # 1단계(병렬): child 2 — run_1 과 동시에 시작 가능 (Pool 2슬롯)
    run_2 = _trigger(
        task_id="run__small_mysql_copy_2_v1",
        trigger_dag_id=SMALL_2_DAG_ID,
        pool="small_mysql_runner",
    )

    # 2단계(순차): child 3 — run_1·run_2 모두 success 후에만 실행
    run_3 = _trigger(
        task_id="run__small_mysql_copy_3_v1",
        trigger_dag_id=SMALL_3_DAG_ID,
        # pool 없음: 1·2 완료 뒤 단일 child 만 실행
    )

    # 1·2 병렬 → 3 순차 (run_3 은 upstream 2개 모두 성공 필요)
    [run_1, run_2] >> run_3
