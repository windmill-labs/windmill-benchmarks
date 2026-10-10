"""Raw CPython speed in the exact container/image the Hatchet worker tasks run in.

Standalone on purpose (no hatchet import) so it can be exec'd inside the live
worker container. The recursion is byte-for-byte the one the benchmark tasks use.
"""
import json
import sys
import time


def fibo(n: int) -> int:
    return n if n <= 1 else fibo(n - 1) + fibo(n - 2)


def timed(n: int) -> float:
    t = time.perf_counter()
    fibo(n)
    return round(time.perf_counter() - t, 6)


if __name__ == "__main__":
    fibo(20)  # warm the code path so we are not timing first-call overhead
    print(json.dumps({
        "version": sys.version,
        "fibo33_s": [timed(33) for _ in range(3)],
        "fibo10_s": timed(10),
    }))
