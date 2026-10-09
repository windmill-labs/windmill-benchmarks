set -euo pipefail
cd ~/bench_src && docker build -q -t bench-py . >/dev/null 2>&1
echo "=== settle=5 ==="
SETTLE=5 bash ~/run_combo.sh tuned sleep 2000 20 1 0 0 probeS5.json 2>&1 | grep -E "rep0|enqueued|wall"
echo "=== settle=30 ==="
SETTLE=30 bash ~/run_combo.sh tuned sleep 2000 20 1 0 0 probeS30.json 2>&1 | grep -E "rep0|enqueued|wall"
