# D'amo Dec/Enc 테스트 (순수 동작 검증용)

이 폴더는 **D'amo(SafeDB) 라이브러리가 설치/배포된 환경에서**, Java로 직접 `ScpDbAgent`를 호출해
**encrypt/decrypt가 실제로 동작하는지**만 빠르게 확인하기 위한 샘플입니다.

## 파일

- `DamoScpCli.java`
  - `new ScpDbAgent()` 인스턴스 생성 후
  - `scpDecrypt(confPath, keyGroup, data)` 또는 `scpEncrypt(confPath, keyGroup, data)`를 호출합니다.
- `damo_scp_test.py`
  - JPype로 JVM을 띄운 뒤 `ScpDbAgent`를 직접 호출하는 **순수 Python 테스트**입니다.
- `sample_damo_scp_test_dag.py`
  - Airflow에서 `common.damo_scp`를 호출해 **DAG로 decrypt/encrypt 동작을 검증**합니다.

## 컴파일 (예시)

아래는 “개념 예시”입니다. 실제로는 D'amo 제공 JAR 경로들을 `-cp`에 넣어야 합니다.

```powershell
# repo root에서 실행한다고 가정
$damoJars = "C:\path\to\damo.jar;C:\path\to\deps\*"

# 컴파일 결과를 out 폴더에 생성
mkdir out -Force | Out-Null
javac -encoding UTF-8 -cp $damoJars -d out .\dags\sample\damoDecEnc\DamoScpCli.java
```

## 실행 (예시)

```powershell
$cp = "$damoJars;$(Resolve-Path .\out)"
java -cp $cp dags.sample.damoDecEnc.DamoScpCli scpDecrypt "C:\path\to\damo_api.conf" "KEY_GROUP_NAME" "암호문"
```

encrypt 테스트:

```powershell
java -cp $cp dags.sample.damoDecEnc.DamoScpCli scpEncrypt "C:\path\to\damo_api.conf" "KEY_GROUP_NAME" "평문"
```

## Python으로 테스트 (추천)

`jpype1`이 설치되어 있어야 합니다.

```powershell
pip install jpype1
```

환경변수 세팅 후 실행:

```powershell
$env:DAMO_CLASSPATH = "C:\path\to\damo.jar;C:\path\to\deps\*"
$env:DAMO_CONF_PATH = "C:\path\to\damo_api.conf"
$env:DAMO_KEY_GROUP = "KEY_GROUP_NAME"
$env:DAMO_MODE = "decrypt"   # or "encrypt"

python .\dags\sample\damoDecEnc\damo_scp_test.py "테스트데이터(암호문/평문)"
```

## Airflow DAG로 테스트

DAG 파일: `sample_damo_scp_test_v1` (`dags/sample/damoDecEnc/sample_damo_scp_test_dag.py`)

수동 트리거 시 `dag_run.conf`로 값 전달 가능:

```json
{
  "mode": "decrypt",
  "data": "암호문",
  "classpath": "C:\\path\\to\\damo.jar;C:\\path\\to\\deps\\*",
  "confPath": "C:\\path\\to\\damo_api.conf",
  "keyGroup": "KEY_GROUP_NAME"
}
```

또는 Airflow Variable/ENV로 아래 키를 세팅해도 됩니다:
- `DAMO_CLASSPATH`
- `DAMO_CONF_PATH`
- `DAMO_KEY_GROUP`
- `DAMO_MODE` (optional)
- `DAMO_DATA` (optional)

## 실패 시 체크 포인트

- D'amo/SafeDB JAR들이 classpath에 모두 포함됐는지
- `damo_api.conf` 경로/권한이 올바른지
- `KEY_GROUP_NAME`이 D'amo 정책에 존재하는지
- 라이브러리 메서드명이 실제로 `scpDecrypt`/`scpEncrypt`가 맞는지

