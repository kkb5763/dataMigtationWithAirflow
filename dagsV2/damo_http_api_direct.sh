#!/usr/bin/env sh
# DamoHttpApiDirect 구동 (기본 PORT=8082)
set -eu

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"

ROOT_DIR="${DAMO_ROOT:-/data/app/airflow/damo}"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/out}"
SRC="${SRC:-$SCRIPT_DIR/DamoHttpApiDirect.java}"
JAR="${DAMO_JAR:-$ROOT_DIR/scpdb.jar}"

[ -f "$JAR" ] || { echo "Missing DAMO_JAR: $JAR" >&2; exit 1; }
[ -f "$SRC" ] || { echo "Missing source: $SRC" >&2; exit 1; }

mkdir -p "$OUT_DIR"
javac -encoding UTF-8 -cp "$JAR" -d "$OUT_DIR" "$SRC"

export PORT="${PORT:-8082}"
export DAMO_GROUP="${DAMO_GROUP:-KEY1}"
export DAMO_CONF_PATH="${DAMO_CONF_PATH:-$ROOT_DIR/scp.ini}"

echo "DamoHttpApiDirect  PORT=$PORT  GROUP=$DAMO_GROUP  CONF=$DAMO_CONF_PATH"
exec java -cp "$OUT_DIR:$JAR" DamoHttpApiDirect
