#!/usr/bin/env sh
set -eu

ROOT_DIR="/data/app/airflow/damo"
OUT_DIR="/data/app/airflow/damo/out"
SRC="/data/app/airflow/damo/src/DamoHttpApiDirect.java"
JAR="/data/app/airflow/damo/scpdb.jar"

# compile 먼저 (scpdb.jar 필요)
mkdir -p "$OUT_DIR"
javac -encoding UTF-8 -cp "$JAR" -d "$OUT_DIR" "$SRC"

# 기본 PORT=8081 (원하면 export PORT=... 로 변경)
# DAMO_GROUP 기본값은 KEY1 이지만, 운영 값이면 export DAMO_GROUP=... 로 지정 권장
export PORT="${PORT:-8081}"
export DAMO_GROUP="${DAMO_GROUP:-KEY1}"

exec java -cp "$OUT_DIR:$JAR" DamoHttpApiDirect
