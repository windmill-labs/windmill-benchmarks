#!/bin/bash
# run_combo.sh <config> <workload> <total> <per_wf> <reps> <gap> <pollers> [outname]
set -euo pipefail
CONFIG=$1; KIND=$2; TOTAL=$3; PER_WF=$4; REPS=$5; GAP=$6; POLLERS=${7:-0}
OUTNAME=${8:-temporal_throughput_${CONFIG}_${KIND}.json}
CTL=~/bench/ctl; OUT=~/bench/out
rm -rf $CTL; mkdir -p $CTL $OUT
docker rm -f bench-wfw bench-actw bench-drv >/dev/null 2>&1 || true

NET="--network temporal-network -e TEMPORAL_ADDRESS=temporal:7233"
docker run -d --name bench-wfw $NET -v $CTL:/ctl bench-py python wfworker.py >/dev/null
docker run -d --name bench-actw $NET -v $CTL:/ctl bench-py python actworker.py "$KIND" 20 "$REPS" "$POLLERS" >/dev/null

for i in $(seq 1 180); do
  [ -f $CTL/wf_ready ] && [ -f $CTL/act_ready ] && break
  sleep 1
done
if [ ! -f $CTL/act_ready ]; then echo "WORKERS NOT READY"; docker logs bench-actw; docker logs bench-wfw; exit 1; fi
echo "workers ready: $(cat $CTL/act_ready)"

# host-wide cpu sampler
python3 - "$OUT/hostcpu_${OUTNAME%.json}.txt" <<'PY' &
import sys, time
out = open(sys.argv[1], "w")
def snap():
    f = open("/proc/stat").readline().split()[1:]
    v = [int(x) for x in f]
    return sum(v), sum(v) - v[3] - v[4]   # total, busy (minus idle+iowait)
p = snap()
while True:
    time.sleep(1.0)
    c = snap()
    dt = c[0] - p[0]
    out.write("%.3f %.3f\n" % (time.time(), (c[1]-p[1]) / dt * 32 if dt else 0))
    out.flush()
    p = c
PY
SAMPLER=$!

docker run --rm --name bench-drv $NET -e SETTLE="${SETTLE:-5}" -e PRERELEASE="${PRERELEASE:-0}" -v $CTL:/ctl bench-py \
  python driver.py "$CONFIG" "$KIND" "$TOTAL" "$PER_WF" "$REPS" "$GAP"

kill $SAMPLER 2>/dev/null || true

if [ "$REPS" -gt 1 ]; then MEAS=$(seq -s, 1 $((REPS-1))); else MEAS=0; fi
docker run --rm $NET -v $CTL:/ctl -v $OUT:/out bench-py \
  python reader.py "/out/$OUTNAME" "$MEAS"
cp $CTL/manifest.json "$OUT/manifest_${OUTNAME}"
echo "--- act worker log ---"; docker logs bench-actw 2>&1 | tail -3
docker rm -f bench-wfw bench-actw >/dev/null 2>&1 || true
