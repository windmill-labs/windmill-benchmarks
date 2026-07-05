#!/bin/bash
# One-shot setup for the TPC-H-derived benchmark: Python venv + deps, Spark 4.0.0,
# MinIO server + client. Downloads land in $WM_TOOLS_DIR (default ~/tpch-bench/tools).
# A JDK 17 must already be installed and pointed to by $JAVA_HOME (Spark needs it).
set -euo pipefail

BENCH_HOME="${WM_BENCH_HOME:-$HOME/tpch-bench}"
TOOLS="${WM_TOOLS_DIR:-$BENCH_HOME/tools}"
mkdir -p "$TOOLS" "$BENCH_HOME"

echo "== Python venv + deps =="
uv venv --python 3.12 "$BENCH_HOME/.venv"
# shellcheck disable=SC1091
. "$BENCH_HOME/.venv/bin/activate"
uv pip install -r "$(dirname "$0")/requirements.txt"

echo "== Spark 4.0.0 =="
if [ ! -d "$TOOLS/spark" ]; then
  curl -sL -o "$TOOLS/spark.tgz" \
    https://archive.apache.org/dist/spark/spark-4.0.0/spark-4.0.0-bin-hadoop3.tgz
  tar xzf "$TOOLS/spark.tgz" -C "$TOOLS"
  mv "$TOOLS/spark-4.0.0-bin-hadoop3" "$TOOLS/spark"
  rm "$TOOLS/spark.tgz"
fi

echo "== MinIO server + client =="
[ -x "$TOOLS/minio" ] || { curl -sL -o "$TOOLS/minio" https://dl.min.io/server/minio/release/linux-amd64/minio; chmod +x "$TOOLS/minio"; }
[ -x "$TOOLS/mc" ]    || { curl -sL -o "$TOOLS/mc"    https://dl.min.io/client/mc/release/linux-amd64/mc;       chmod +x "$TOOLS/mc"; }

echo "== Check JDK =="
"${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk}/bin/java" -version 2>&1 | head -1 \
  || echo "!! Set JAVA_HOME to a JDK 17 install before running Spark."

echo "Done. Next: 'make minio' then 'make data' then 'make run'."
