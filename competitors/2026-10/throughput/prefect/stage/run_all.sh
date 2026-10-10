#!/bin/bash
# Full throughput matrix. Runs detached; every step appends to progress.log.
exec 9>/home/ubuntu/.run_all.lock
flock -n 9 || { echo "already running"; exit 9; }
cd /home/ubuntu
P=/home/ubuntu/progress.log
log(){ echo "$(date -Is) $*" >> $P; }
DC="sudo docker compose"
REPS=6          # rep 0 is the discarded warmup
N=2000

mkdir -p /home/ubuntu/flows/out

# ---------- phase 1: in_process (no worker needed) ----------
$DC stop worker >/dev/null 2>&1
log "PHASE in_process begin (worker stopped)"
for WL in sleep cpu; do
  for r in $(seq 0 $((REPS-1))); do
    TAG="ip_${WL}_r${r}"
    log "start $TAG"
    timeout 1800 $DC run --rm --no-deps -T -e N_UNITS=$N \
      worker python /opt/flows/inproc.py "$WL" 31 "$TAG" "/opt/flows/out/${TAG}.json" >/dev/null 2>&1
    log "done $TAG rc=$?"
    sleep 5
  done
done
log "PHASE in_process complete"

# ---------- phase 2: work_pool, stock worker query interval ----------
$DC -f compose.yml up -d worker >/dev/null 2>&1
sleep 20
log "PHASE work_pool stock begin QUERY_SECONDS=$($DC exec -T worker printenv PREFECT_WORKER_QUERY_SECONDS 2>/dev/null || echo default-10)"
for WL in sleep cpu; do
  for r in $(seq 0 $((REPS-1))); do
    TAG="wp_${WL}_r${r}"
    log "start $TAG"
    timeout 3000 $DC run --rm --no-deps -T -e N_UNITS=$N -e CREATE_CONC=100 \
      worker python /opt/flows/pool_driver.py "$WL" 31 "$TAG" "/opt/flows/out/${TAG}.json" >>/home/ubuntu/flows/out/${TAG}.log 2>&1
    log "done $TAG rc=$?"
    sleep 10
  done
done
log "PHASE work_pool stock complete"

# ---------- phase 3: work_pool, 0.1s worker query interval ----------
$DC -f compose.yml -f override_tuned.yml up -d worker >/dev/null 2>&1
sleep 20
log "PHASE work_pool tuned begin QUERY_SECONDS=$($DC exec -T worker printenv PREFECT_WORKER_QUERY_SECONDS)"
for WL in sleep cpu; do
  for r in $(seq 0 $((REPS-1))); do
    TAG="wt_${WL}_r${r}"
    log "start $TAG"
    timeout 3000 $DC run --rm --no-deps -T -e N_UNITS=$N -e CREATE_CONC=100 \
      worker python /opt/flows/pool_driver.py "$WL" 31 "$TAG" "/opt/flows/out/${TAG}.json" >>/home/ubuntu/flows/out/${TAG}.log 2>&1
    log "done $TAG rc=$?"
    sleep 10
  done
done
log "ALL COMPLETE"
