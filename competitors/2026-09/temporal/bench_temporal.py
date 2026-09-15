#!/usr/bin/env python3
"""Temporal side of the orchestrator benchmark.

One workflow runs N activities sequentially (each awaited before the next is
scheduled), each computing fibo(n) with the naive recursion. Per-activity
timings come from the workflow's event history (server-side timestamps), not
from wall-clock prints in this process.

Subcommands:
  worker  -- run the single worker process
  run     -- drive one workload and emit the results JSON
  interp  -- measure raw interpreter speed inside the worker's activity executor
"""
import argparse
import asyncio
import importlib.metadata
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.worker import Worker

with workflow.unsafe.imports_passed_through():
    from temporalio.api.enums.v1 import EventType
    from temporalio.api.workflowservice.v1 import GetSystemInfoRequest

TASK_QUEUE = "bench"
TARGET = "localhost:7233"


def fibo(x: int) -> int:
    return x if x <= 1 else fibo(x - 1) + fibo(x - 2)


@activity.defn
def fibo_activity(n: int) -> int:
    return fibo(n)


@activity.defn
def interpreter_bench() -> dict:
    """Raw interpreter speed in the exact process/executor activities run in."""
    f33 = []
    for _ in range(3):
        t = time.perf_counter()
        fibo(33)
        f33.append(round(time.perf_counter() - t, 3))
    t = time.perf_counter()
    fibo(10)
    f10 = time.perf_counter() - t
    return {"version": sys.version, "fibo33_s": f33, "fibo10_s": round(f10, 6)}


@workflow.defn
class SequentialFibo:
    @workflow.run
    async def run(self, steps: int, n: int) -> int:
        result = 0
        for _ in range(steps):
            result = await workflow.execute_activity(
                fibo_activity,
                n,
                start_to_close_timeout=timedelta(minutes=10),
            )
        return result


@workflow.defn
class InterpreterBench:
    @workflow.run
    async def run(self) -> dict:
        return await workflow.execute_activity(
            interpreter_bench, start_to_close_timeout=timedelta(minutes=10)
        )


# --------------------------------------------------------------------------- worker


async def cmd_worker(args):
    client = await Client.connect(TARGET)
    # Sync (def) activities need an executor that actually runs them; a thread
    # pool is the documented straightforward choice. max_workers must be >= the
    # worker's default max_concurrent_activities (100).
    with ThreadPoolExecutor(max_workers=100) as executor:
        worker = Worker(
            client,
            task_queue=TASK_QUEUE,
            workflows=[SequentialFibo, InterpreterBench],
            activities=[fibo_activity, interpreter_bench],
            activity_executor=executor,
        )
        print("worker up", flush=True)
        await worker.run()


# ------------------------------------------------------------------- history timings


def _ts(pb) -> float:
    return pb.seconds + pb.nanos / 1e9


async def activity_timings(handle) -> dict:
    """Per-activity timings pulled from the workflow's event history.

    ActivityTaskScheduled -> created_at (server knew about the activity)
    ActivityTaskStarted   -> started_at (worker began executing it)
    ActivityTaskCompleted -> completed_at
    Started events are keyed back to their ActivityTaskScheduled event id.
    """
    bad = {
        EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED,
        EventType.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
        EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED,
        EventType.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT,
    }
    scheduled, started, completed, anomalies, attempts = {}, {}, {}, [], set()
    async for ev in handle.fetch_history_events():
        t = _ts(ev.event_time)
        if ev.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
            scheduled[ev.event_id] = t
        elif ev.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_STARTED:
            a = ev.activity_task_started_event_attributes
            started[a.scheduled_event_id] = t
            attempts.add(a.attempt)
        elif ev.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
            completed[ev.activity_task_completed_event_attributes.scheduled_event_id] = t
        elif ev.event_type in bad:
            anomalies.append((ev.event_id, EventType.Name(ev.event_type)))

    if anomalies:
        raise RuntimeError(f"retry/timeout events in history: {anomalies}")
    if attempts - {1}:
        raise RuntimeError(f"activity attempts other than 1: {sorted(attempts)}")
    ids = sorted(scheduled)
    missing = [i for i in ids if i not in started or i not in completed]
    if missing:
        raise RuntimeError(f"history incomplete for scheduled events {missing}")
    t0 = scheduled[ids[0]]
    return {
        "workers": 1,
        "created_at": [round(scheduled[i] - t0, 3) for i in ids],
        "started_at": [round(started[i] - t0, 3) for i in ids],
        "completed_at": [round(completed[i] - t0, 3) for i in ids],
    }


async def server_version(client) -> str:
    resp = await client.service_client.workflow_service.get_system_info(
        GetSystemInfoRequest()
    )
    return resp.server_version


# ------------------------------------------------------------------------------ run


async def cmd_run(args):
    client = await Client.connect(TARGET)
    ver = await server_version(client)
    sdk = importlib.metadata.version("temporalio")

    runs = []
    for i in range(args.warmup + args.reps):
        wf_id = f"bench-{args.steps}x{args.fib}-{int(time.time() * 1000)}-{i}"
        handle = await client.start_workflow(
            SequentialFibo.run,
            args=[args.steps, args.fib],
            id=wf_id,
            task_queue=TASK_QUEUE,
        )
        await handle.result()
        t = await activity_timings(handle)
        if len(t["created_at"]) != args.steps:
            raise RuntimeError(
                f"expected {args.steps} activities, got {len(t['created_at'])}"
            )
        total = max(t["completed_at"])
        label = "warmup" if i < args.warmup else f"rep {i - args.warmup + 1}"
        print(
            f"  {label}: {len(t['created_at'])} activities, total {total:.3f}s ({wf_id})",
            file=sys.stderr,
            flush=True,
        )
        if i >= args.warmup:
            runs.append(t)

    result = {
        "engine": "temporal",
        "version": ver,
        "sdk": sdk,
        "workload": args.workload,
        "runs": runs,
        "totals": [round(max(r["completed_at"]), 3) for r in runs],
    }
    if args.interp:
        result["interpreter"] = json.load(open(args.interp))
    result["notes"] = args.notes
    text = json.dumps(result, indent=2)
    if args.out:
        open(args.out, "w").write(text)
    print(text)


async def cmd_interp(args):
    client = await Client.connect(TARGET)
    handle = await client.start_workflow(
        InterpreterBench.run,
        id=f"interp-{int(time.time() * 1000)}",
        task_queue=TASK_QUEUE,
    )
    res = await handle.result()
    text = json.dumps(res, indent=2)
    if args.out:
        open(args.out, "w").write(text)
    print(text)


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("worker")

    r = sub.add_parser("run")
    r.add_argument("--steps", type=int, required=True)
    r.add_argument("--fib", type=int, required=True)
    r.add_argument("--reps", type=int, default=5)
    r.add_argument("--warmup", type=int, default=1)
    r.add_argument("--workload", required=True)
    r.add_argument("--out", default="")
    r.add_argument("--interp", default="")
    r.add_argument("--notes", default="")

    i = sub.add_parser("interp")
    i.add_argument("--out", default="")

    args = ap.parse_args()
    fn = {"worker": cmd_worker, "run": cmd_run, "interp": cmd_interp}[args.cmd]
    asyncio.run(fn(args))


if __name__ == "__main__":
    main()
