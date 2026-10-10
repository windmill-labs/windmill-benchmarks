#!/bin/bash
# run_set.sh <label> <iters> <fib> <poll_interval> <reps>
set -e
cd /home/ubuntu
LABEL=$1; ITERS=$2; FIB=$3; PI=$4; REPS=$5
for r in $(seq 0 $((REPS-1))); do
  TAG="${LABEL}_r${r}"
  sudo docker compose run --rm --no-deps -T worker python /opt/flows/driver.py "$ITERS" "$FIB" "$TAG" "$PI" >/dev/null 2>&1
  echo "$(date -Is) done $TAG" >> /home/ubuntu/progress.log
done
echo "$(date -Is) SET COMPLETE $LABEL" >> /home/ubuntu/progress.log
