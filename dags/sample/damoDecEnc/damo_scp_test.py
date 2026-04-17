import os
import sys
from typing import Optional

from common.damo_scp import env_or_var, scp_call


def _require(name: str, value: Optional[str]) -> str:
    if value is None or value == "":
        raise RuntimeError(f"Missing required setting: {name}")
    return value


def main() -> int:
    """
    D'amo(SafeDB) ScpDbAgent를 JPype로 직접 호출하는 순수 Python 테스트.

    필수 환경변수:
      - DAMO_CLASSPATH: D'amo 및 의존 JAR들의 classpath (os.pathsep로 구분)
      - DAMO_CONF_PATH: damo_api.conf 경로
      - DAMO_KEY_GROUP: 암호화 그룹명

    선택 환경변수:
      - DAMO_MODE: decrypt | encrypt (default: decrypt)
      - DAMO_DATA: 테스트할 문자열(암호문/평문). 없으면 argv[1] 사용
      - DAMO_CLASS: (default: com.penta.scpdb.ScpDbAgent)
    """

    damo_cp = _require("DAMO_CLASSPATH", env_or_var("DAMO_CLASSPATH"))
    conf_path = _require("DAMO_CONF_PATH", env_or_var("DAMO_CONF_PATH"))
    key_group = _require("DAMO_KEY_GROUP", env_or_var("DAMO_KEY_GROUP"))

    mode = (env_or_var("DAMO_MODE", "decrypt") or "decrypt").strip().lower()
    data = env_or_var("DAMO_DATA") or (sys.argv[1] if len(sys.argv) > 1 else "")
    data = _require("DAMO_DATA (env) or argv[1]", data)

    cls_name = (env_or_var("DAMO_CLASS", "com.penta.scpdb.ScpDbAgent") or "com.penta.scpdb.ScpDbAgent").strip()

    out = scp_call(
        mode=mode,
        classpath=damo_cp,
        conf_path=conf_path,
        key_group=key_group,
        data=data,
        agent_class=cls_name,
    )

    sys.stdout.write(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

