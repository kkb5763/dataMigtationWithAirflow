from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

try:
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator
except Exception:  # pragma: no cover
    from airflow.operators.dagrun_operator import TriggerDagRunOperator  # type: ignore

# ==========================================================
# SMALL 1·2·3 병렬 오케스트레이터 (하드코딩)
# ==========================================================
# 실행 패턴: small_mysql_copy_1_v1 ∥ _2_v1 ∥ _3_v1 (선후관계 없음, 동시 시작)
#
# Pools (Admin):
#   - small_mysql_runner / slots 3  ← 이 DAG 의 Trigger 가 동시에 child 3개까지
#   - small_mysql_copy_1~3 / slots 4  ← 각 child DAG 내부 테이블 병렬 (별도 설정)
#
# 사용: UI 에서 이 DAG 만 Trigger → 1·2·3 child 가 한 번에 돌아감
# child DAG: small_1_mysql_copy_mig_dag.py 등 (TABLE_LIST 만 다름)

# --- 트리거 대상 child DAG id (dag_id 와 동일 문자열) ---
SMALL_1_DAG_ID = "small_mysql_copy_1_v1"
SMALL_2_DAG_ID = "small_mysql_copy_2_v1"
SMALL_3_DAG_ID = "small_mysql_copy_3_v1"

# TriggerDagRunOperator 공통 옵션 (3개 child 에 동일 적용)
_TRIGGER_KW = {
    "execution_date": "{{ execution_date }}",  # 부모·자식 DagRun 의 execution_date 맞춤
    "reset_dag_run": True,  # 같은 execution_date 의 기존 child DagRun 이 있으면 지우고 재실행
    "wait_for_completion": True,  # True = child DAG 가 끝날 때까지 이 태스크가 대기
    "poke_interval": 60,  # wait_for_completion 시 child 상태 확인 주기(초)
    "pool": "small_mysql_runner",  # child 3개 동시 실행 상한 Pool
    "pool_slots": 1,  # 이 태스크가 Pool 에서 차지하는 슬롯 (slots=3 이면 최대 3 child 동시)
}


def _trigger(task_id: str, trigger_dag_id: str) -> TriggerDagRunOperator:
    """TriggerDagRunOperator 생성 헬퍼 (task_id·trigger_dag_id 만 바꿔 재사용)."""
    return TriggerDagRunOperator(
        task_id=task_id,
        trigger_dag_id=trigger_dag_id,
        **_TRIGGER_KW,
    )


default_args = {
    "owner": "small",  # DAG 소유자 (UI 표시)
    "retries": 0,  # 오케스트레이터 태스크 실패 시 재시도 없음
}

with DAG(
    # --- DAG 메타 ---
    dag_id="small_mysql_copy_parallel_runner_v1",  # Airflow UI 고유 ID
    default_args=default_args,
    start_date=datetime(2026, 4, 1),  # 스케줄/DagRun 논리 시작 시각
    schedule_interval=None,  # None = 자동 스케줄 없음 (수동 Trigger 만)
    catchup=False,  # 과거 구간 소급 실행 비활성
    tags=["small", "orchestrator", "parallel"],  # UI 필터용 태그
    # --- 동시성 ---
    max_active_runs=1,  # 이 오케스트레이터의 활성 DagRun 1개로 제한 (중복 실행 방지)
    max_active_tasks=8,  # 이 DAG 안 동시 RUNNING 태스크 상한 (child 3 + 여유)
) as dag:
    # --- child DAG: small_mysql_copy_1_v1 ---
    run_1 = _trigger(
        task_id="run__small_mysql_copy_1_v1",  # 그래프·로그에 표시되는 태스크 이름
        trigger_dag_id=SMALL_1_DAG_ID,  # 실행할 대상 DAG 의 dag_id
    )

    # --- child DAG: small_mysql_copy_2_v1 ---
    run_2 = _trigger(
        task_id="run__small_mysql_copy_2_v1",
        trigger_dag_id=SMALL_2_DAG_ID,
    )

    # --- child DAG: small_mysql_copy_3_v1 ---
    run_3 = _trigger(
        task_id="run__small_mysql_copy_3_v1",
        trigger_dag_id=SMALL_3_DAG_ID,
    )

    # 3개 child Trigger 가 모두 성공한 뒤 합류하는 마커 (본 DAG 성공 조건)
    all_done = EmptyOperator(task_id="all_parallel_done")

    # 선후관계 없음 → 1·2·3 병렬 실행 후 all_done 으로 수렴
    [run_1, run_2, run_3] >> all_done
