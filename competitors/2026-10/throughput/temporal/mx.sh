set -euo pipefail
cd ~/bench_src && docker build -q -t bench-py . >/dev/null 2>&1
echo "=== A: 100wf x 20, pollers=default(5) ==="
bash ~/run_combo.sh tuned sleep 2000 20 1 0 0 probeA.json 2>&1 | grep -E "rep0|ready|enqueued|wall"
echo "=== B: 2000wf x 1, pollers=default(5) ==="
bash ~/run_combo.sh tuned sleep 2000 1 1 0 0 probeB.json 2>&1 | grep -E "rep0|ready|enqueued|wall"
