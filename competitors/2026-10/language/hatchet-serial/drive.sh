#!/usr/bin/env bash
# Measurement driver. One engine, one worker at a time, Python then TypeScript,
# with settling gaps so neither language is measured on a hotter engine.
set -u
cd ~/bench/harness
OUT=~/bench/out
mkdir -p $OUT
NET=hatchet-lite_default
log() { echo "[$(date -u +%H:%M:%S)] $*"; }

run_phase() {
  local image=$1 entry=$2 tag=$3 workload=$4
  log "=== $tag / $workload: reps"
  docker run --rm --network $NET --env-file env.list $image $entry \
      --workload "$workload" --warmup 1 --reps 5 \
      > $OUT/ids_${tag}_${workload}.json 2> $OUT/log_${tag}_${workload}.txt
  log "--- $tag / $workload done: $(cat $OUT/ids_${tag}_${workload}.json)"
  cat $OUT/log_${tag}_${workload}.txt
}

# ---------- Python phase ----------
docker stop bench-worker-ts >/dev/null 2>&1
docker rm -f bench-worker-ts >/dev/null 2>&1
docker rm -f bench-worker >/dev/null 2>&1
log "starting python worker"
docker run -d --name bench-worker --network $NET --env-file env.list bench-worker python worker.py >/dev/null
sleep 10
docker exec bench-worker python interpreter_bench.py > $OUT/interpreter_py.json
docker exec bench-worker python -c 'import hatchet_sdk, sys; print(hatchet_sdk.__version__ if hasattr(hatchet_sdk,"__version__") else "n/a"); print(sys.version)' > $OUT/pyver.txt 2>&1
log "settling 90s"
sleep 90
run_phase bench-worker "python run_bench.py" py fibonacci_40_10
log "idle gap 120s"
sleep 120
run_phase bench-worker "python run_bench.py" py fibonacci_10_33
log "python phase done; stopping python worker"
docker rm -f bench-worker >/dev/null 2>&1

# ---------- TypeScript phase ----------
log "idle gap 150s before TS phase"
sleep 150
docker rm -f bench-worker-ts >/dev/null 2>&1
log "starting ts worker"
docker run -d --name bench-worker-ts --network $NET --env-file env.list bench-worker-ts node dist/worker.js >/dev/null
sleep 10
docker exec bench-worker-ts node dist/interpreter_bench.js > $OUT/interpreter_ts.json
docker exec bench-worker-ts node -e 'console.log(process.version); console.log(require("/app/node_modules/@hatchet-dev/typescript-sdk/package.json").version)' > $OUT/tsver.txt 2>&1
log "settling 90s"
sleep 90
run_phase bench-worker-ts "node dist/run_bench.js" ts fibonacci_40_10
log "idle gap 120s"
sleep 120
run_phase bench-worker-ts "node dist/run_bench.js" ts fibonacci_10_33
log "ts phase done"

# ---------- Python control re-run (order-effect check) ----------
docker rm -f bench-worker-ts >/dev/null 2>&1
log "idle gap 150s before python control"
sleep 150
docker rm -f bench-worker >/dev/null 2>&1
docker run -d --name bench-worker --network $NET --env-file env.list bench-worker python worker.py >/dev/null
sleep 10
log "settling 90s"
sleep 90
run_phase bench-worker "python run_bench.py" pyctl fibonacci_40_10
log "ALL DONE"
