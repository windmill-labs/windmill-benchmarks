set -euo pipefail
rm -f ~/bench/run_all.log
mkdir -p ~/bench/out
nohup bash ~/run_all.sh > ~/bench/run_all.log 2>&1 &
echo "launched pid $!"
