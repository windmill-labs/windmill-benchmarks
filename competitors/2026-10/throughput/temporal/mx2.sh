set -euo pipefail
echo "=== C: 100wf x 20, pollers=20 ==="
bash ~/run_combo.sh tuned sleep 2000 20 1 0 20 probeC.json 2>&1 | grep -E "rep0|enqueued|wall"
echo "=== E: 100wf x 20, pollers=40 ==="
bash ~/run_combo.sh tuned sleep 2000 20 1 0 40 probeE.json 2>&1 | grep -E "rep0|enqueued|wall"
echo "=== F: 40wf x 50, pollers=20 ==="
bash ~/run_combo.sh tuned sleep 2000 50 1 0 20 probeF.json 2>&1 | grep -E "rep0|enqueued|wall"
