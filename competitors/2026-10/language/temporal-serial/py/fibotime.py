import json
import platform
import sys
import time


def fibo(n):
    if n <= 1:
        return n
    return fibo(n - 1) + fibo(n - 2)


runs = []
for _ in range(3):
    t = time.monotonic()
    fibo(33)
    runs.append(round(time.monotonic() - t, 4))

print(json.dumps({
    "version": platform.python_version(),
    "impl": platform.python_implementation(),
    "fibo33_runs_s": runs,
    "fibo33_total_s": round(sum(runs), 4),
}))
