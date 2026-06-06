#!/usr/bin/env sh
# SafeDbHttpApiDirect 구동 (기본 PORT=8083)
set -eu

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"

ROOT_DIR="${SAFEDB_ROOT:-/data/app/airflow/safedb}"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/out}"
SRC="${SRC:-$SCRIPT_DIR/SafeDbHttpApiDirect.java}"
LIB_DIR="${SAFEDB_LIB_DIR:-$ROOT_DIR/lib}"
CONFIG_DIR="${SAFEDB_CONFIG_DIR:-$ROOT_DIR/config}"

[ -f "$SRC" ] || { echo "Missing source: $SRC" >&2; exit 1; }
[ -d "$LIB_DIR" ] || { echo "Missing SAFEDB_LIB_DIR: $LIB_DIR" >&2; exit 1; }

CLASSPATH=""
for j in "$LIB_DIR"/*.jar; do
  [ -f "$j" ] || continue
  if [ -n "$CLASSPATH" ]; then
    CLASSPATH="${CLASSPATH}:$j"
  else
    CLASSPATH="$j"
  fi
done
[ -n "$CLASSPATH" ] || { echo "No jars in $LIB_DIR" >&2; exit 1; }

if [ -d "$CONFIG_DIR" ]; then
  CLASSPATH="${CONFIG_DIR}:$CLASSPATH"
else
  echo "WARN: SAFEDB_CONFIG_DIR not found: $CONFIG_DIR" >&2
fi

mkdir -p "$OUT_DIR"
javac -encoding UTF-8 -cp "$CLASSPATH" -d "$OUT_DIR" "$SRC"

export PORT="${PORT:-8083}"

echo "SafeDbHttpApiDirect  PORT=$PORT  LIB=$LIB_DIR  CONFIG=$CONFIG_DIR"
exec java -cp "$OUT_DIR:$CLASSPATH" SafeDbHttpApiDirect
