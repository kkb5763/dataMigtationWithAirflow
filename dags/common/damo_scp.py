import os
import threading
from typing import List, Optional


_jvm_lock = threading.Lock()
_jvm_started = False


def _split_classpath(cp: str) -> List[str]:
    parts = [p.strip() for p in cp.split(os.pathsep)]
    return [p for p in parts if p]


def ensure_jvm_started(classpath: str) -> None:
    """
    Start JVM once per Python process.

    Airflow worker는 같은 프로세스 내에서 task가 여러 번 호출될 수 있어
    JVM은 중복 start를 피해야 합니다.
    """
    global _jvm_started
    if _jvm_started:
        return

    with _jvm_lock:
        if _jvm_started:
            return
        import jpype  # type: ignore

        if not jpype.isJVMStarted():
            jpype.startJVM(classpath=_split_classpath(classpath))
        _jvm_started = True


def scp_call(
    *,
    mode: str,
    classpath: str,
    conf_path: str,
    key_group: str,
    data: str,
    agent_class: str = "com.penta.scpdb.ScpDbAgent",
) -> str:
    """
    D'amo(SafeDB) ScpDbAgent 호출 공통 함수.

    mode:
      - "decrypt" => scpDecrypt(confPath, keyGroup, cipherText)
      - "encrypt" => scpEncrypt(confPath, keyGroup, plainText)
    """
    if not data:
        return ""

    mode_n = (mode or "").strip().lower()
    if mode_n not in {"decrypt", "encrypt"}:
        raise ValueError("mode must be 'decrypt' or 'encrypt'")

    ensure_jvm_started(classpath)

    import jpype  # type: ignore

    ScpDbAgent = jpype.JClass(agent_class)
    ag = ScpDbAgent()

    if mode_n == "decrypt":
        out = ag.scpDecrypt(conf_path, key_group, data)
    else:
        out = ag.scpEncrypt(conf_path, key_group, data)

    return "" if out is None else str(out).strip()


def env_or_var(name: str, default: Optional[str] = None) -> Optional[str]:
    """
    Airflow Variable > ENV 순서가 더 자연스럽지만,
    로컬 테스트/스크립트에서는 ENV가 편해서 ENV 우선으로 둡니다.
    """
    v = os.getenv(name)
    if v is not None and v != "":
        return v
    try:
        from airflow.models import Variable

        vv = Variable.get(name, default_var=default)
        return vv
    except Exception:
        return default

