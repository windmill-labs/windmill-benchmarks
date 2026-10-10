#!/bin/bash
# Re-run the in_process sleep set with a drain barrier between reps.
#
# The first attempt lost records: the server's in-memory event broker holds
# 10000 messages (hardcoded in prefect/server/utilities/messaging/memory.py)
# and the task-run recorder could not drain a 15s rep before the next one
# started, so it logged "Subscription queue is full, dropping message" and
# half the task runs were never written. One rep's worth of events does fit,
# so this version waits for the recorder to persist all 2000 before starting
# the next rep. Nothing about the measured window changes.
exec 9>/home/ubuntu/.rerun.lock
flock -n 9 || { echo "already running"; exit 9; }
cd /home/ubuntu
P=/home/ubuntu/progress.log
log(){ echo "$(date -Is) $*" >> $P; }
DC="sudo docker compose"

$DC stop worker >/dev/null 2>&1
$DC exec -T db psql -U prefect -d prefect -At -c \
  "delete from flow_run where left(tags->>0,9)='ip_sleep_'" >> $P 2>&1
log "RERUN deleted old ip_sleep rows"

for r in $(seq 0 5); do
  TAG="ip_sleep_r${r}"
  log "start $TAG"
  timeout 1800 $DC run --rm --no-deps -T -e N_UNITS=2000 \
    worker python /opt/flows/inproc.py sleep 31 "$TAG" "/opt/flows/out/${TAG}.json" >/dev/null 2>&1
  rc=$?
  n=0
  for i in $(seq 1 90); do
    n=$($DC exec -T db psql -U prefect -d prefect -At -c \
      "select count(*) from task_run tr join flow_run fr on fr.id=tr.flow_run_id
       where fr.tags->>0='$TAG' and tr.state_type::text='COMPLETED'" 2>/dev/null | tr -d ' ')
    [ "$n" = "2000" ] && break
    sleep 5
  done
  log "done $TAG rc=$rc recorded_completed=$n"
  sleep 10
done
log "RERUN COMPLETE"
