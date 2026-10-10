#!/bin/bash
# Revised matrix: smaller batches for the poll-bound configurations.
#   in_process sleep : 500 units  x 6 reps (1 warmup + 5)
#   work_pool stock  : 200 units  x 4 reps (1 warmup + 3)
#   work_pool tuned  : 400 units  x 4 reps (1 warmup + 3)
# in_process cpu (2000 units x 6) is already measured and is not re-run.
exec 9>/home/ubuntu/.run2.lock
flock -n 9 || { echo "already running"; exit 9; }
cd /home/ubuntu
P=/home/ubuntu/progress.log
log(){ echo "$(date -Is) $*" >> $P; }
DC="sudo docker compose"
mkdir -p /home/ubuntu/flows/out

# ---- in_process sleep, 500 units, with a recorder drain barrier ----
$DC stop worker >/dev/null 2>&1
$DC exec -T db psql -U prefect -d prefect -At -c \
  "delete from flow_run where left(tags->>0,9)='ip_sleep_'" >> $P 2>&1
log "RUN2 PHASE in_process sleep begin (500 units, worker stopped)"
for r in $(seq 0 5); do
  TAG="ip_sleep_r${r}"
  log "start $TAG"
  timeout 600 $DC run --rm --no-deps -T -e N_UNITS=500 \
    worker python /opt/flows/inproc.py sleep 31 "$TAG" "/opt/flows/out/${TAG}.json" >/dev/null 2>&1
  rc=$?
  n=0
  for i in $(seq 1 60); do
    n=$($DC exec -T db psql -U prefect -d prefect -At -c \
      "select count(*) from task_run tr join flow_run fr on fr.id=tr.flow_run_id
       where fr.tags->>0='$TAG' and tr.state_type::text='COMPLETED'" 2>/dev/null | tr -d ' ')
    [ "$n" = "500" ] && break
    sleep 3
  done
  log "done $TAG rc=$rc recorded_completed=$n/500"
  sleep 5
done
log "RUN2 PHASE in_process sleep complete"

# ---- work_pool, stock worker poll loop, 200 units ----
$DC -f compose.yml up -d --force-recreate worker >/dev/null 2>&1
sleep 25
log "RUN2 PHASE work_pool stock begin (200 units) QUERY_SECONDS=$($DC exec -T worker printenv PREFECT_WORKER_QUERY_SECONDS 2>/dev/null || echo 'unset/default-10')"
for WL in sleep cpu; do
  for r in $(seq 0 3); do
    TAG="wp_${WL}_r${r}"
    log "start $TAG"
    timeout 1200 $DC run --rm --no-deps -T -e N_UNITS=200 -e CREATE_CONC=100 \
      worker python /opt/flows/pool_driver.py "$WL" 31 "$TAG" "/opt/flows/out/${TAG}.json" \
      > /home/ubuntu/flows/out/${TAG}.log 2>&1
    log "done $TAG rc=$?"
    sleep 10
  done
done
log "RUN2 PHASE work_pool stock complete"

# ---- work_pool, 0.1s worker poll loop, 400 units ----
$DC -f compose.yml -f override_tuned.yml up -d --force-recreate worker >/dev/null 2>&1
sleep 25
log "RUN2 PHASE work_pool tuned begin (400 units) QUERY_SECONDS=$($DC exec -T worker printenv PREFECT_WORKER_QUERY_SECONDS)"
for WL in sleep cpu; do
  for r in $(seq 0 3); do
    TAG="wt_${WL}_r${r}"
    log "start $TAG"
    timeout 1200 $DC run --rm --no-deps -T -e N_UNITS=400 -e CREATE_CONC=100 \
      worker python /opt/flows/pool_driver.py "$WL" 31 "$TAG" "/opt/flows/out/${TAG}.json" \
      > /home/ubuntu/flows/out/${TAG}.log 2>&1
    log "done $TAG rc=$?"
    sleep 10
  done
done
log "RUN2 ALL COMPLETE"
