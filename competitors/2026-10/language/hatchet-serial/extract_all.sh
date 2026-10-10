#!/usr/bin/env bash
set -eu
cd ~/bench/harness
OUT=~/bench/out
export HATCHET_CLIENT_TOKEN=$(cat ~/bench/token.txt)
ids() { python3 -c "import json,sys;print(','.join(json.load(open(sys.argv[1]))['run_ids']))" "$1"; }

do_one() {
  local lang=$1 wl=$2 tasks=$3 interp=$4 sdk=$5
  python3 extract.py --workload "$wl" --tasks "$tasks" \
    --run-ids "$(ids $OUT/ids_${lang}_${wl}.json)" \
    --engine-version v0.107.0 --sdk-version "$sdk" \
    --interpreter "$interp" \
    --notes "lang=$lang; one worker, default slots; 1 discarded warmup + 5 measured reps back to back; engine settled 90s before the reps; timings from v1_tasks_olap.inserted_at + STARTED/FINISHED in v1_task_events_olap" \
    --out $OUT/extract_${lang}_${wl}.json
}

do_one py    fibonacci_40_10 40 $OUT/interpreter_py.json "hatchet-sdk 1.40.1 (python)"
do_one py    fibonacci_10_33 10 $OUT/interpreter_py.json "hatchet-sdk 1.40.1 (python)"
do_one ts    fibonacci_40_10 40 $OUT/interpreter_ts.json "@hatchet-dev/typescript-sdk 1.36.0"
do_one ts    fibonacci_10_33 10 $OUT/interpreter_ts.json "@hatchet-dev/typescript-sdk 1.36.0"
do_one pyctl fibonacci_40_10 40 $OUT/interpreter_py.json "hatchet-sdk 1.40.1 (python)"
echo "EXTRACT ALL OK"
