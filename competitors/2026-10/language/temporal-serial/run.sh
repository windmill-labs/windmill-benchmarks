#!/bin/bash
# run.sh <lang: py|ts> <config: stock|tuned> <workload: fibonacci_40_10|fibonacci_10_33>
set -euo pipefail
LANG_=$1; CONFIG=$2; WORKLOAD=$3
case "$WORKLOAD" in
  fibonacci_40_10) ITER=40; FIBO=10; SHORT=40x10 ;;
  fibonacci_10_33) ITER=10; FIBO=33; SHORT=10x33 ;;
  *) echo "unknown workload $WORKLOAD"; exit 1 ;;
esac
REPS=6        # rep 0 is warmup and discarded
GAP=5
TAG="${LANG_}-${CONFIG}-${SHORT}"
OUT=~/bench/out
mkdir -p "$OUT"

case "$LANG_" in
  py) IMAGE=bench-py; CMD="python harness.py" ;;
  ts) IMAGE=bench-ts; CMD="node dist/harness.js" ;;
  *) echo "unknown lang $LANG_"; exit 1 ;;
esac

echo "### $TAG  iter=$ITER fibo=$FIBO reps=$REPS(1 warmup) gap=${GAP}s"
docker run --rm --network temporal-network -e TEMPORAL_ADDRESS=temporal:7233 \
  "$IMAGE" $CMD "$ITER" "$FIBO" "$REPS" "$GAP" "$TAG" 2>&1 | grep -E '^(rep|DONE)' 

docker run --rm --network temporal-network -e TEMPORAL_ADDRESS=temporal:7233 \
  -v "$OUT":/out bench-py python reader.py \
  "/out/temporal_${LANG_}_${CONFIG}_${WORKLOAD}.json" \
  "$LANG_" "$CONFIG" "$WORKLOAD" "$TAG" 1,2,3,4,5
