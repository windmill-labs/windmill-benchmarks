#!/bin/bash
cd ~/kbench
set -u
PE=20
echo "=== runtime probe (before measured runs, box idle) ==="
timeout 600 python3 runtime_probe.py 2>&1 | tail -6

for combo in "process sleep 1800" "process cpu 1800" "docker sleep 5400"; do
  set -- $combo
  R=$1; W=$2; T=$3
  echo "###### $R / $W"
  timeout $((T+600)) python3 th.py --workload $W --runner $R --units 2000 --per-exec $PE \
     --reps 5 --warmup 1 --timeout $T --tag _m \
     --out raw_${R}_${W}.json --dump-rows rows_${R}_${W}.json 2>&1 | tail -12
  echo "###### $R / $W END"
done
echo "###### ALL DONE"
