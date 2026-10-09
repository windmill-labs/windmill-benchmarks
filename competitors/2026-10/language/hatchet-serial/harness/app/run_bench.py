"""Trigger phase: fire the sequential DAG warmup+reps times, emit the run ids."""
import argparse
import json
import sys
import time

from bench_defs import WORKFLOWS

ap = argparse.ArgumentParser()
ap.add_argument("--workload", required=True)
ap.add_argument("--warmup", type=int, default=1)
ap.add_argument("--reps", type=int, default=5)
args = ap.parse_args()

wf = WORKFLOWS[args.workload]
warmup_ids, run_ids = [], []

for i in range(args.warmup + args.reps):
    t = time.time()
    ref = wf.run(wait_for_result=False)
    rid = ref.workflow_run_id
    res = ref.result()
    wall = round(time.time() - t, 3)
    label = "warmup" if i < args.warmup else f"rep {i - args.warmup + 1}"
    print(f"  {label}: run {rid} wall {wall}s tasks {len(res)}", file=sys.stderr)
    (warmup_ids if i < args.warmup else run_ids).append(rid)

print(json.dumps({"warmup_ids": warmup_ids, "run_ids": run_ids}))
