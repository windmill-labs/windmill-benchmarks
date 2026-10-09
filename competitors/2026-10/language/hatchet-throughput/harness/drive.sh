#!/usr/bin/env bash
# The sleeping workload end to end, TypeScript worker: bring the worker up,
# settle, then 1 discarded warmup + 5 measured reps.
set -u
cd ~/bench/harness
OUT=~/bench/out
mkdir -p $OUT
NET=hatchet-lite_default
TENANT=$(cat ~/bench/tenant.txt)
TOKEN=$(cat ~/bench/token.txt)
N=2000
NW=1
SLOTS=20
PREFIX=tssleep

log() { echo "[$(date -u +%H:%M:%S)] $*"; }

active() {
  curl -s -H "Authorization: Bearer $TOKEN" \
    "http://localhost:8888/api/v1/tenants/$TENANT/worker" \
  | python3 -c "import json,sys;r=json.load(sys.stdin).get('rows') or [];print(sum(1 for w in r if w.get('status')=='ACTIVE'))"
}

docker rm -f bench-fleet-ts bench-client-ts >/dev/null 2>&1
log "starting client container"
docker run -d --name bench-client-ts --network $NET --env-file env.list bench-ts \
  sleep infinity >/dev/null

log "starting $NW worker process(es), $SLOTS slot(s) each"
docker run -d --name bench-fleet-ts --network $NET --env-file env.list bench-ts \
  sh -c "for i in \$(seq 1 $NW); do node dist/worker.js --workload sleep --slots $SLOTS --name ${PREFIX}-w\$i & done; wait" >/dev/null

for i in $(seq 1 60); do
  a=$(active); log "active workers: $a/$NW"; [ "$a" = "$NW" ] && break; sleep 3
done
a=$(active)
if [ "$a" != "$NW" ]; then log "FATAL: $a active workers, expected $NW"; docker logs bench-fleet-ts | tail -40; exit 2; fi

log "runtime probe inside the worker container"
docker exec bench-fleet-ts node dist/versions.js > $OUT/versions.json
cat $OUT/versions.json

log "settling 90s"
sleep 90

for i in 0 1 2 3 4 5; do
  if [ "$i" = "0" ]; then tag="${PREFIX}-warmup"; else tag="${PREFIX}-r${i}"; fi
  log "=== $tag: enqueue $N"
  docker exec bench-client-ts node dist/enqueue.js --workload sleep --n $N \
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
