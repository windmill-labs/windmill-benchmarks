set -uo pipefail
bash -n ~/run_combo2.sh && bash -n ~/final.sh && echo "syntax OK"
rm -f ~/bench/final.log
nohup bash ~/final.sh > ~/bench/final.log 2>&1 &
echo "launched pid $!"
