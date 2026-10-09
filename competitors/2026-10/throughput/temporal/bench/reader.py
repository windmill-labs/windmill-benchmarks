"""Reads every rep's event history and derives the reported metrics.

All numbers come from ActivityTaskScheduled / ActivityTaskStarted /
ActivityTaskCompleted event times recorded by the Temporal server; no harness
wall clock is used.
"""
import asyncio
import json
import math
import os
import sys

from temporalio.api.enums.v1 import EventType
from temporalio.client import Client

ADDR = os.environ["TEMPORAL_ADDRESS"]
OUT = sys.argv[1]
MEASURED = [int(x) for x in sys.argv[2].split(",")]

SCHED = EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
START = EventType.EVENT_TYPE_ACTIVITY_TASK_STARTED
COMP = EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED
FAIL = EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED
TIMEOUT = EventType.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT


def ts(e):
    return e.event_time.seconds + e.event_time.nanos / 1e9


def pct(xs, q):
    s = sorted(xs)
    if not s:
        return None
    k = max(1, int(math.ceil(q * len(s))))
    return s[k - 1]


def peak_concurrency(iv):
    ev = []
    for a, b in iv:
        ev.append((a, 1))
        ev.append((b, -1))
    ev.sort(key=lambda x: (x[0], x[1]))  # completions before starts at a tie
    cur = peak = 0
    for _, d in ev:
        cur += d
        peak = max(peak, cur)
    return peak


async def collect(client, wids):
    sem = asyncio.Semaphore(16)
    out = {}
    bad = {"failed": 0, "timed_out": 0}

    async def one(wid):
        async with sem:
            h = await client.get_workflow_handle(wid).fetch_history()
        sched_by_id, acts = {}, {}
        for e in h.events:
            if e.event_type == SCHED:
                a = e.activity_task_scheduled_event_attributes
                sched_by_id[e.event_id] = a.activity_id
                acts[a.activity_id] = {"sched": ts(e)}
            elif e.event_type == START:
                aid = sched_by_id.get(
                    e.activity_task_started_event_attributes.scheduled_event_id
                )
                if aid:
                    acts[aid]["start"] = ts(e)
            elif e.event_type == COMP:
                aid = sched_by_id.get(
                    e.activity_task_completed_event_attributes.scheduled_event_id
                )
                if aid:
                    acts[aid]["comp"] = ts(e)
            elif e.event_type == FAIL:
                bad["failed"] += 1
            elif e.event_type == TIMEOUT:
                bad["timed_out"] += 1
        return acts

    for acts in await asyncio.gather(*[one(w) for w in wids]):
        out.update(acts)
    return out, bad


async def main() -> None:
    man = json.load(open("/ctl/manifest.json"))
    client = await Client.connect(ADDR)
    reps = []
    for r in man["reps"]:
        if r["rep"] not in MEASURED:
            continue
        acts, bad = await collect(client, r["wids"])
        complete = {k: v for k, v in acts.items() if len(v) == 3}
        starts = [v["start"] for v in complete.values()]
        comps = [v["comp"] for v in complete.values()]
        lat = [v["comp"] - v["sched"] for v in complete.values()]
        ex = [v["comp"] - v["start"] for v in complete.values()]
        span = max(comps) - min(starts)
        iv = [(v["start"], v["comp"]) for v in complete.values()]
        cpu = None
        p = "/ctl/cpu_%d.json" % r["rep"]
        if os.path.exists(p):
            cpu = json.load(open(p))
        reps.append(
            {
                "rep": r["rep"],
                "jobs": len(complete),
                "jobs_scheduled": len(acts),
                "span_s": round(span, 4),
                "throughput_per_s": round(len(complete) / span, 3),
                "latency_p50_s": round(pct(lat, 0.50), 4),
                "latency_p95_s": round(pct(lat, 0.95), 4),
                "exec_p50_s": round(pct(ex, 0.50), 4),
                "exec_p95_s": round(pct(ex, 0.95), 4),
                "exec_sum_s": round(sum(ex), 3),
                "effective_concurrency": round(sum(ex) / span, 3),
                "peak_concurrency_from_timestamps": peak_concurrency(iv),
                "activities_failed": bad["failed"],
                "activities_timed_out": bad["timed_out"],
                "enqueue_phase_s": round(r["enqueue_s"], 3),
                "submit_phase_s": round(r["submit_s"], 3),
                "activities_enqueued_before_go": r["activities_enqueued_before_go"],
                "worker_cpu_seconds": round(cpu["cpu_usec_delta"] / 1e6, 3)
                if cpu and cpu.get("cpu_usec_delta")
                else None,
                "worker_cores_busy_over_span": round(cpu["cpu_usec_delta"] / 1e6 / span, 2)
                if cpu and cpu.get("cpu_usec_delta")
                else None,
            }
        )
    doc = {
        "engine": "temporal",
        "config": man["config"],
        "workload": man["workload"],
        "concurrency": 20,
        "reps": reps,
        "_manifest": {k: v for k, v in man.items() if k != "reps"},
    }
    with open(OUT, "w") as f:
        json.dump(doc, f, indent=1)
    for r in reps:
        print(
            "rep%d n=%d span=%.3fs tput=%.1f/s latp50=%.3f latp95=%.3f execp50=%.4f "
            "peak=%d effconc=%.2f cores=%s enq=%.2fs"
            % (
                r["rep"],
                r["jobs"],
                r["span_s"],
                r["throughput_per_s"],
                r["latency_p50_s"],
                r["latency_p95_s"],
                r["exec_p50_s"],
                r["peak_concurrency_from_timestamps"],
                r["effective_concurrency"],
                r["worker_cores_busy_over_span"],
                r["enqueue_phase_s"],
            ),
            flush=True,
        )


if __name__ == "__main__":
    asyncio.run(main())
