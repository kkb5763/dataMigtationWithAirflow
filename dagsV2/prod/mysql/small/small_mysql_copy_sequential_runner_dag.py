from datetime import datetime

from airflow import DAG

try:
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator
except Exception:  # pragma: no cover
    from airflow.operators.dagrun_operator import TriggerDagRunOperator  # type: ignore

# ==========================================================
# SMALL 1 → 2 → 3 순차 오케스트레이터 (하드코딩)
# ==========================================================
# 실행 패턴: 1 완료 → 2 완료 → 3 (한 번에 하나의 child DAG 만 실행)
#
# wait_for_completion=True 이므로 Trigger 태스크가 child 종료까지 블로킹.
# 별도 ExternalTaskSensor 없이 run_1 >> run_2 >> run_3 체인으로 순서 보장.
#
# Pool: 순차 실행이므로 오케스트레이터 Trigger 에 Pool 미지정
#       (동시에 child 1개만 RUNNING — 그래프 의존성으로 제한)
#
# 사용: 부하·락을 줄이거나 테이블 그룹 간 순서가 필요할 때 이 DAG Trigger

# --- 트리거 대상 child DAG id ---
SMALL_1_DAG_ID = "small_mysql_copy_1_v1"
SMALL_2_DAG_ID = "small_mysql_copy_2_v1"
SMALL_3_DAG_ID = "small_mysql_copy_3_v1"

# TriggerDagRunOperator 공통 옵션
_TRIGGER_KW = {
    "execution_date": "{{ execution_date }}",  # 부모·자식 DagRun execution_date 맞춤
    "reset_dag_run": True,  # 동일 execution_date 기존 child DagRun 있으면 재실행
    "wait_for_completion": True,  # child 완료까지 대기 → 다음 태스크(다음 child) 시작
    "poke_interval": 60,  # 완료 대기 중 상태 확인 주기(초)
    # pool 미설정: 순차이므로 그래프상 동시 child 는 최대 1개
}


def _trigger(task_id: str, trigger_dag_id: str) -> TriggerDagRunOperator:
    """TriggerDagRunOperator 생성 헬퍼."""
    return TriggerDagRunOperator(
        task_id=task_id,
        trigger_dag_id=trigger_dag_id,
        **_TRIGGER_KW,
    )


default_args = {
    "owner": "small",
    "retries": 0,
}

with DAG(
    dag_id="small_mysql_copy_sequential_runner_v1",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,  # 수동 Trigger 만
    catchup=False,
    tags=["small", "orchestrator", "sequential"],
    max_active_runs=1,  # 오케스트레이터 중복 DagRun 방지
) as dag:
    # 1단계: small_mysql_copy_1_v1 실행 후 완료 대기
    run_1 = _trigger(
        task_id="run__small_mysql_copy_1_v1",
        trigger_dag_id=SMALL_1_DAG_ID,
    )

    # 2단계: 1 성공 후 small_mysql_copy_2_v1
    run_2 = _trigger(
        task_id="run__small_mysql_copy_2_v1",
        trigger_dag_id=SMALL_2_DAG_ID,
    )

    # 3단계: 2 성공 후 small_mysql_copy_3_v1
    run_3 = _trigger(
        task_id="run__small_mysql_copy_3_v1",
        trigger_dag_id=SMALL_3_DAG_ID,
    )

    # 순차 의존: 1 → 2 → 3 (이전 child 가 success 여야 다음 Trigger 시작)
    run_1 >> run_2 >> run_3
