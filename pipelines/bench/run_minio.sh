#!/bin/bash
# Start a local MinIO server and create the 'windmill' bucket used by the benchmark.
set -e
TOOLS="${WM_TOOLS_DIR:-$HOME/tpch-bench/tools}"
DATADIR="${WM_MINIO_DATA:-$HOME/tpch-bench/minio-data}"
export MINIO_ROOT_USER="${AWS_ACCESS_KEY:-minioadmin}"
export MINIO_ROOT_PASSWORD="${AWS_SECRET_KEY:-minioadmin}"
mkdir -p "$DATADIR"
nohup "$TOOLS/minio" server "$DATADIR" \
  --address 127.0.0.1:9000 --console-address 127.0.0.1:9001 > "$DATADIR/../minio.log" 2>&1 &
sleep 3
"$TOOLS/mc" alias set local http://127.0.0.1:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
"$TOOLS/mc" mb --ignore-existing "local/${S3_BUCKET:-windmill}"
echo "MinIO up at http://127.0.0.1:9000 (bucket: ${S3_BUCKET:-windmill})"
