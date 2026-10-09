"""Steady-state ceiling: feed 400 fibo(31) calls through a 20-worker process
pool (and 400 sleep(0.1) calls through a 20-thread pool) with no orchestrator
in the path. This is the fastest the same worker code could possibly go."""
import concurrent.futures as cf
import json
import time

from common import fibo

N = 400


def sl(_):
    time.sleep(0.1)
    return 1


if __name__ == "__main__":
    out = {}
    pex = cf.ProcessPoolExecutor(max_workers=20)
    list(pex.map(fibo, [28] * 20))
    runs = []
    for _ in range(3):
        t0 = time.monotonic()
        res = list(pex.map(fibo, [31] * N, chunksize=1))
        w = time.monotonic() - t0
        assert all(v == 1346269 for v in res)
        runs.append({"n": N, "wall_s": round(w, 4), "per_s": round(N / w, 2)})
    out["cpu_fibo31_processpool20"] = runs

    tex = cf.ThreadPoolExecutor(max_workers=20)
    list(tex.map(sl, range(20)))
    runs = []
    for _ in range(3):
        t0 = time.monotonic()
        list(tex.map(sl, range(N)))
        w = time.monotonic() - t0
        runs.append({"n": N, "wall_s": round(w, 4), "per_s": round(N / w, 2)})
    out["sleep_threadpool20"] = runs
    print(json.dumps(out, indent=1))
