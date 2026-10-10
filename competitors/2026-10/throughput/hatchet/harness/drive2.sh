#!/usr/bin/env bash
# One workload end to end: bring up the worker fleet, settle, 1 warmup + 5 reps.
set -u
workload=$1
NW_OVERRIDE=${2:-}
SLOTS_OVERRIDE=${3:-}
PREFIX=${4:-$workload}
cd ~/bench/harness
OUT=~/bench/out
NET=hatchet-lite_default
TENANT=$(cat ~/bench/tenant.txt)
TOKEN=$(cat ~/bench/token.txt)
N=2000

case $workload in
  sleep) NW=1;  SLOTS=20 ;;
  cpu)   NW=20; SLOTS=1  ;;
  *) echo "unknown workload"; exit 1 ;;
esac
[ -n "$NW_OVERRIDE" ] && NW=$NW_OVERRIDE
[ -n "$SLOTS_OVERRIDE" ] && SLOTS=$SLOTS_OVERRIDE

log() { echo "[$(date -u +%H:%M:%S)] $*"; }

active() {
  curl -s -H "Authorization: Bearer $TOKEN" \
    "http://localhost:8888/api/v1/tenants/$TENANT/worker" \
  | python3 -c "import json,sys;r=json.load(sys.stdin).get('rows') or [];print(sum(1 for w in r if w.get('status')=='ACTIVE'))"
}

docker rm -f bench-fleet bench-client >/dev/null 2>&1
log "starting client container"
docker run -d --name bench-client --network $NET --env-file env.list bench-worker \
  sleep infinity >/dev/null

log "starting $NW worker process(es), $SLOTS slot(s) each"
docker run -d --name bench-fleet --network $NET --env-file env.list bench-worker \
  sh -c "for i in \$(seq 1 $NW); do python worker.py --workload $workload --slots $SLOTS --name ${PREFIX}-w\$i & done; wait" >/dev/null

for i in $(seq 1 60); do
  a=$(active); log "active workers: $a/$NW"; [ "$a" = "$NW" ] && break; sleep 3
done
a=$(active)
if [ "$a" != "$NW" ]; then log "FATAL: $a active workers, expected $NW"; exit 2; fi

log "interpreter probe inside the worker container"
docker exec bench-fleet python interpreter_bench.py > $OUT/interp_${PREFIX}.json
cat $OUT/interp_${PREFIX}.json

log "settling 90s"
sleep 90

for i in 0 1 2 3 4 5; do
  if [ "$i" = "0" ]; then tag="${PREFIX}-warmup"; else tag="${PREFIX}-r${i}"; fi
  log "=== $tag: enqueue $N"
  setsid nohup python3 sample_cpu.py $OUT/cpu_${tag}.json 45 >/dev/null 2>&1 </dev/null &
  SAMPLER=$!
  docker exec bench-client python enqueue.py --workload $workload --n $N \
      --tag $tag --expect-workers $NW > $OUT/enq_${tag}.json 2> $OUT/enqlog_${tag}.txt
  erc=$?
  if [ $erc -ne 0 ] || [ ! -s $OUT/enq_${tag}.json ]; then
    log "ENQUEUE FAILED (rc=$erc)"; cat $OUT/enqlog_${tag}.txt; exit 3
  fi
  cat $OUT/enq_${tag}.json
  python3 extract.py --tag $tag --n $N --concurrency 20 --raw-out $OUT/raw_${tag}.json \
      > $OUT/res_${tag}.json 2> $OUT/extlog_${tag}.txt
  rc=$?
  cat $OUT/extlog_${tag}.txt
  if [ $rc -ne 0 ]; then log "EXTRACT FAILED for $tag"; else cat $OUT/res_${tag}.json; fi
  log "--- $tag done; 60s gap"
  sleep 60
done
log "WORKLOAD $PREFIX DONE"
