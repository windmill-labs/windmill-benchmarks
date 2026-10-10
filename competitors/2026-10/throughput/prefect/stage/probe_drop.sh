#!/bin/bash
# Measure the task-run recorder's drain rate and the event-broker drop onset.
#
# The broker queue absorbs a burst of up to 10000 messages, so a single rep
# does not necessarily drop anything; what drops is a sustained production
# rate above the recorder's drain rate. This runs three back-to-back
# 2000-unit in_process sleep reps with no drain barrier, samples the number
# of rows the recorder has persisted once a second, and counts the drop
# warnings the server logs in that window.
cd /home/ubuntu
P=/home/ubuntu/progress.log
log(){ echo "$(date -Is) $*" >> $P; }
DC="sudo docker compose"
$DC stop worker >/dev/null 2>&1
$DC exec -T db psql -U prefect -d prefect -At -c \
  "delete from flow_run where left(tags->>0,10)='probe_drop'" >/dev/null 2>&1
rm -f /home/ubuntu/drain.txt
MARK=$(date -u +%Y-%m-%dT%H:%M:%S)
echo "$MARK" > /home/ubuntu/drop_mark.txt
log "DROPPROBE begin mark=$MARK"

# sample the recorder's progress once a second in the background
(
  for i in $(seq 1 600); do
    n=$($DC exec -T db psql -U prefect -d prefect -At -c \
      "select count(*) from task_run tr join flow_run fr on fr.id=tr.flow_run_id
       where left(fr.tags->>0,10)='probe_drop'" 2>/dev/null | tr -d ' ')
    echo "$(date +%s.%N) $n" >> /home/ubuntu/drain.txt
    sleep 1
  done
) &
SAMPLER=$!

for r in 0 1 2; do
  TAG="probe_drop_r${r}"
  log "DROPPROBE start $TAG"
  timeout 600 $DC run --rm --no-deps -T -e N_UNITS=2000 \
    worker python /opt/flows/inproc.py sleep 31 "$TAG" "/opt/flows/out/${TAG}.json" >/dev/null 2>&1
  log "DROPPROBE done $TAG rc=$?"
done
log "DROPPROBE runs finished, draining for 240s"
sleep 240
kill $SAMPLER 2>/dev/null
$DC exec -T db psql -U prefect -d prefect -At -F'|' -c \
  "select fr.tags->>0, count(*), count(*) filter (where tr.state_type::text='COMPLETED')
   from task_run tr join flow_run fr on fr.id=tr.flow_run_id
   where left(fr.tags->>0,10)='probe_drop' group by 1 order by 1" > /home/ubuntu/drop_counts.txt 2>&1
$DC logs server --since 30m 2>&1 | grep -F "Subscription queue is full" > /home/ubuntu/drop_lines.txt
wc -l < /home/ubuntu/drop_lines.txt > /home/ubuntu/drop_total.txt
log "DROPPROBE COMPLETE"
