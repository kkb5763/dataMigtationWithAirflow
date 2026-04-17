from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from common.damo_scp import env_or_var, scp_call


def _require(name: str, value: Optional[str]) -> str:
    if value is None or value == "":
        raise RuntimeError(f"Missing required setting: {name}")
    return value


def _run_damo(**context: Any) -> None:
    """
    실행 시 값 우선순위:
      - dag_run.conf (수동 트리거 시 입력)
      - Airflow Variable
      - ENV

    dag_run.conf 예시:
      {
        "mode": "decrypt",
        "data": "....",
        "classpath": "C:\\path\\damo.jar;C:\\path\\deps\\*",
        "confPath": "C:\\path\\damo_api.conf",
        "keyGroup": "KEY_GROUP_NAME"
      }
    """
    dag_run = context.get("dag_run")
    conf: Dict[str, Any] = (dag_run.conf or {}) if dag_run else {}

    mode = (conf.get("mode") or env_or_var("DAMO_MODE", "decrypt") or "decrypt").strip()
    data = conf.get("data") or env_or_var("DAMO_DATA") or ""
    classpath = conf.get("classpath") or env_or_var("DAMO_CLASSPATH") or ""
    conf_path = conf.get("confPath") or env_or_var("DAMO_CONF_PATH") or ""
    key_group = conf.get("keyGroup") or env_or_var("DAMO_KEY_GROUP") or ""
    agent_class = conf.get("agentClass") or env_or_var("DAMO_CLASS", "com.penta.scpdb.ScpDbAgent")

    out = scp_call(
        mode=_require("mode", mode),
        classpath=_require("classpath (DAMO_CLASSPATH)", classpath),
        conf_path=_require("confPath (DAMO_CONF_PATH)", conf_path),
        key_group=_require("keyGroup (DAMO_KEY_GROUP)", key_group),
        data=_require("data (DAMO_DATA)", data),
        agent_class=_require("agentClass (DAMO_CLASS)", agent_class),
    )

    # Airflow 로그로 결과 확인
    print(out, flush=True)


with DAG(
    dag_id="sample_damo_scp_test_v1",
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    tags=["sample", "damo", "safedb", "encrypt", "decrypt"],
) as dag:
    start = EmptyOperator(task_id="start")
    run = PythonOperator(task_id="run_damo_scp", python_callable=_run_damo)
    end = EmptyOperator(task_id="end")

    start >> run >> end

