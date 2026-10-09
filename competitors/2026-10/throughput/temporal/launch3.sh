set -uo pipefail
bash -n ~/diag_shards.sh && echo "syntax OK"
nohup bash ~/diag_shards.sh > ~/bench/diag.log 2>&1 &
echo "launched $!"
