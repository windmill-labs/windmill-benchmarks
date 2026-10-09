"""Configuration 1: 2000 @task calls inside one flow, 20-way via ThreadPoolTaskRunner.

Tasks run in the flow's own process. All 2000 are .submit()-ed before any
result is awaited, so submission is not serialised behind execution.
"""
import json
import os
import resource
import sys
import time

from prefect import flow, task, tags
from prefect.task_runners import ThreadPoolTaskRunner

N_UNITS = int(os.environ.get("N_UNITS", "2000"))
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "20"))


def fibo(n: int) -> int:
    return n if n <= 1 else fibo(n - 1) + fibo(n - 2)


@task(name="unit", persist_result=False, cache_policy=None)
def unit(workload: str, n: int):
    if workload == "sleep":
        time.sleep(0.1)
        return 0
    return fibo(n)


@flow(name="inproc", task_runner=ThreadPoolTaskRunner(max_workers=MAX_WORKERS),
      persist_result=False)
def inproc(workload: str, n: int):
    t_sub0 = time.time()
    futs = [unit.submit(workload, n) for _ in range(N_UNITS)]
    t_sub1 = time.time()
    ok = 0
    for f in futs:
        f.result(raise_on_failure=True)
        ok += 1
    return {"submit_start": t_sub0, "submit_end": t_sub1, "ok": ok}


def main(workload: str, n: int, tag: str, out: str) -> None:
    r0 = resource.getrusage(resource.RUSAGE_SELF)
    w0 = time.time()
    with tags(tag):
        st = inproc(workload, n)
    w1 = time.time()
    r1 = resource.getrusage(resource.RUSAGE_SELF)
    cpu = (r1.ru_utime - r0.ru_utime) + (r1.ru_stime - r0.ru_stime)
    rec = {
        "tag": tag, "workload": workload, "n": n, "units": N_UNITS,
        "max_workers": MAX_WORKERS,
        "wall_start": w0, "wall_end": w1, "wall_s": round(w1 - w0, 3),
        "submit_s": round(st["submit_end"] - st["submit_start"], 3),
        "ok": st["ok"],
        "flow_proc_cpu_s": round(cpu, 3),
        "cores_busy_flow_proc": round(cpu / (w1 - w0), 3),
        "python": sys.version,
    }
    with open(out, "w") as fh:
        json.dump(rec, fh)
    print(json.dumps(rec), flush=True)


if __name__ == "__main__":
    main(sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4])
