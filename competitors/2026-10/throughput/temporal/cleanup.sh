set -uo pipefail
pkill -f chain_inner.sh; pkill -f chain2.sh; pkill -f run_pipeline.sh; pkill -f run_combo.sh; pkill -f "/proc/stat" 2>/dev/null
sleep 2
docker rm -f bench-wfw bench-actw bench-drv >/dev/null 2>&1
ps -ef | grep -E "chain|run_pipeline|run_combo|run_all" | grep -v grep || echo "(no bench scripts running)"
docker ps --format "{{.Names}}" | tr '\n' ' '; echo
mv ~/bench/pipeline.log ~/bench/pipeline_failed_attempt.log 2>/dev/null || true
