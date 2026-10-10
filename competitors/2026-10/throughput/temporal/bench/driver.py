"""Drives one (config, workload) combination.

Per rep: start every workflow, wait until all TOTAL activity tasks are
enqueued (ActivityTaskScheduled present in every history) while the activity
worker is still held off, then release the worker and wait for completion.
"""
import asyncio
import json
import os
import sys
import time

from temporalio.api.enums.v1 import EventType
from temporalio.client import Client

from workflows import FanWorkflow

ADDR = os.environ["TEMPORAL_ADDRESS"]
CONFIG = sys.argv[1]
KIND = sys.argv[2]
TOTAL = int(sys.argv[3])
PER_WF = int(sys.argv[4])
REPS = int(sys.argv[5])
GAP = float(sys.argv[6])
SETTLE = float(os.environ.get("SETTLE", "5"))  # idle wait after the enqueue gate
PRERELEASE = os.environ.get("PRERELEASE") == "1"  # release the worker before submitting

NWF = TOTAL // PER_WF
ACT = "sleep_activity" if KIND == "sleep" else "cpu_activity"
ARG = 0.1 if KIND == "sleep" else 31
TAG = "tp-%s-%s" % (CONFIG, KIND)
SCHED = EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED


async def count_scheduled(client, wids):
    sem = asyncio.Semaphore(16)

    async def one(wid):
        async with sem:
            h = await client.get_workflow_handle(wid).fetch_history()
            return sum(1 for e in h.events if e.event_type == SCHED)

    return sum(await asyncio.gather(*[one(w) for w in wids]))


async def main() -> None:
    client = await Client.connect(ADDR)
    while not os.path.exists("/ctl/wf_ready") or not os.path.exists("/ctl/act_ready"):
        await asyncio.sleep(0.2)
    manifest = {
        "config": CONFIG,
        "workload": KIND,
        "total": TOTAL,
        "per_wf": PER_WF,
        "nwf": NWF,
        "activity": ACT,
        "arg": ARG,
        "reps": [],
    }
    for rep in range(REPS):
        wids = ["%s-r%d-w%03d" % (TAG, rep, k) for k in range(NWF)]
        if PRERELEASE:
            open("/ctl/go_%d" % rep, "w").close()
            await asyncio.sleep(1.0)
        t_sub0 = time.time()
        handles = await asyncio.gather(
            *[
                client.start_workflow(
                    FanWorkflow.run,
                    args=[ACT, ARG, PER_WF, "bench-act", "%s-r%d-w%03d" % (TAG, rep, k)],
                    id=wids[k],
                    task_queue="bench-wf",
                )
                for k in range(NWF)
            ]
        )
        t_sub1 = time.time()
        # wait for every activity task to be enqueued, worker still held off
        deadline = time.time() + (0 if PRERELEASE else 900)
        n = TOTAL if PRERELEASE else 0
        while time.time() < deadline:
            n = await count_scheduled(client, wids)
            if n >= TOTAL:
                break
            await asyncio.sleep(0.5)
        t_enq = time.time()
        if n < TOTAL:
            print("rep %d: ONLY %d/%d enqueued, aborting" % (rep, n, TOTAL), flush=True)
            sys.exit(2)
        print(
            "rep %d: %d workflows submitted in %.2fs, %d activities enqueued in %.2fs"
            % (rep, NWF, t_sub1 - t_sub0, n, t_enq - t_sub0),
            flush=True,
        )
        if not PRERELEASE:
            await asyncio.sleep(SETTLE)
            open("/ctl/go_%d" % rep, "w").close()
        t_go = time.time()
        results = await asyncio.gather(*[h.result() for h in handles])
        t_done = time.time()
        open("/ctl/stop_%d" % rep, "w").close()
        print(
            "rep %d: %d workflows done, sum=%d, wall %.2fs"
            % (rep, len(results), sum(results), t_done - t_go),
            flush=True,
        )
        manifest["reps"].append(
            {
                "rep": rep,
                "wids": wids,
                "submit_s": t_sub1 - t_sub0,
                "enqueue_s": t_enq - t_sub0,
                "settle_s": 0 if PRERELEASE else SETTLE,
                "prerelease": PRERELEASE,
                "exec_wall_s": t_done - t_go,
                "activities_enqueued_before_go": 0 if PRERELEASE else n,
            }
        )
        with open("/ctl/manifest.json", "w") as f:
            json.dump(manifest, f, indent=1)
        if rep < REPS - 1:
            await asyncio.sleep(GAP)
    print("DONE", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
