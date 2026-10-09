set -euo pipefail
echo "=== cpu, 100wf x 20, pollers=default ==="
SETTLE=5 bash ~/run_combo.sh tuned cpu 2000 20 1 0 0 probeCPU.json 2>&1 | tail -8
