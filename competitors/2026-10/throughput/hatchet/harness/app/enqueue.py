"""One repetition of the throughput benchmark.

Pre-enqueue is real, not approximated: every connected worker is paused before the
bulk trigger and unpaused immediately after it returns, so all N tasks are sitting
in Hatchet's queue before any of them is assigned to a slot.

Prints a JSON line with the enqueue wall time and the paused window.
"""
import argparse
import json
import sys
import time

from bench_defs import TASKS, In
from hatchet_api import list_workers, set_paused

ap = argparse.ArgumentParser()
ap.add_argument("--workload", required=True, choices=sorted(TASKS))
ap.add_argument("--n", type=int, default=2000)
ap.add_argument("--tag", required=True)
ap.add_argument("--expect-workers", type=int, required=True)
ap.add_argument("--no-pause", action="store_true")
a = ap.parse_args()

task = TASKS[a.workload]

workers = list_workers()
if len(workers) != a.expect_workers:
    print(f"FATAL: {len(workers)} active workers, expected {a.expect_workers}",
          file=sys.stderr)
    sys.exit(2)
wids = [w["metadata"]["id"] for w in workers]

# Build every trigger config up front; this is pure client-side work.
items = [task.create_bulk_run_item(input=In(i=i), additional_metadata={"rep": a.tag})
         for i in range(a.n)]

paused_at = None
if not a.no_pause:
    set_paused(wids, True)
    paused_at = time.time()
    time.sleep(0.5)  # let the assignment loop observe the pause

t0 = time.time()
refs = task.run_many(items, wait_for_result=False)
t1 = time.time()

if not a.no_pause:
    set_paused(wids, False)
t2 = time.time()

print(json.dumps({
    "tag": a.tag,
    "workload": a.workload,
    "n": len(refs),
    "enqueue_s": round(t1 - t0, 4),
    "paused_window_s": round(t2 - paused_at, 4) if paused_at else None,
    "unpause_s": round(t2 - t1, 4),
    "workers": len(wids),
}))
