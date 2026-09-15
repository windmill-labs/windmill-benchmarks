#!/usr/bin/env python3
"""Prefect side of the orchestrator benchmark (runs INSIDE the prefect-client container).

Runs `warmup + reps` flow runs of `steps` sequential Prefect tasks, each computing
fibo(n) with the naive recursion. Tasks are called directly (no .submit()), so each
call is its own server-tracked task run and they execute strictly in order.

It only records the flow run ids; all timings are extracted afterwards from the
Prefect server's Postgres database by extract.py on the host.
"""
import argparse
import json
import sys
import time

from prefect import flow, task
from prefect.context import get_run_context


def fibo(n):
    return n if n <= 1 else fibo(n - 1) + fibo(n - 2)


@task
def fibo_task(n: int, i: int) -> int:
    # `i` differs per call so that no two task runs in a flow run share inputs.
    return fibo(n)


@flow
def bench(n: int, steps: int):
    for i in range(steps):
        fibo_task(n, i)
    return str(get_run_context().flow_run.id)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--steps", type=int, required=True)
    ap.add_argument("--fib", type=int, required=True)
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--warmup", type=int, default=1)
    ap.add_argument("--settle", type=float, default=3.0)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    ids = []
    for i in range(args.warmup + args.reps):
        t0 = time.time()
        fr_id = bench(args.fib, args.steps)
        label = "warmup" if i < args.warmup else f"rep {i - args.warmup + 1}"
        print(f"{label}: flow_run={fr_id} wall={time.time() - t0:.3f}s", file=sys.stderr, flush=True)
        if i >= args.warmup:
            ids.append(fr_id)
        # let Prefect's background state/log writers drain between runs
        time.sleep(args.settle)

    with open(args.out, "w") as f:
        json.dump({"steps": args.steps, "fib": args.fib, "flow_run_ids": ids}, f, indent=2)
    print(json.dumps(ids))


if __name__ == "__main__":
    main()
