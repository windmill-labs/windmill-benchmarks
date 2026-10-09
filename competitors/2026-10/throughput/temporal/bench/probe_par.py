"""fibo(31) under 20-way process parallelism: the per-call cost the activity
worker actually pays when all 20 slots are busy."""
import concurrent.futures as cf
import json
import time

from common import fibo


def timed(n):
    t = time.monotonic()
    v = fibo(n)
    return time.monotonic() - t, v


if __name__ == "__main__":
    ex = cf.ProcessPoolExecutor(max_workers=20)
    list(ex.map(fibo, [28] * 20))
    rounds = []
    for _ in range(3):
        t0 = time.monotonic()
        res = list(ex.map(timed, [31] * 20))
        wall = time.monotonic() - t0
        per = [round(r[0], 5) for r in res]
        rounds.append(
            {
                "wall_s": round(wall, 5),
                "per_call_mean_s": round(sum(r[0] for r in res) / len(res), 5),
                "per_call_max_s": round(max(r[0] for r in res), 5),
                "per_call_min_s": round(min(r[0] for r in res), 5),
                "values_ok": all(r[1] == 1346269 for r in res),
            }
        )
    print(json.dumps({"fibo31_parallel20_rounds": rounds}, indent=1))
