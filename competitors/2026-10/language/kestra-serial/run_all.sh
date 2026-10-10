#!/bin/bash
cd /home/ubuntu/kestra-bench
mkdir -p out
set -u
run() {  # lang runner workload n count
  local lang=$1 runner=$2 wl=$3 n=$4 count=$5
  echo "################ $lang $runner $wl"
  python3 harness.py --lang "$lang" --runner "$runner" --n "$n" --count "$count" \
    --reps 5 --warmup 1 --workload "$wl" \
    --out "out/kestra_$( [ "$lang" = py ] && echo python || echo js )_${runner}_${wl}.json" || echo "FAILED $lang $runner $wl"
}
# priority: fibonacci_40_10 in all four combinations
run py  process fibonacci_40_10 10 40
run py  docker  fibonacci_40_10 10 40
run js  process fibonacci_40_10 10 40
run js  docker  fibonacci_40_10 10 40
# then fibonacci_10_33
run py  process fibonacci_10_33 33 10
run py  docker  fibonacci_10_33 33 10
run js  process fibonacci_10_33 33 10
run js  docker  fibonacci_10_33 33 10
echo "ALLDONE"
