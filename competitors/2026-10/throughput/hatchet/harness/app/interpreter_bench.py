"""Raw CPython speed in the exact image the worker tasks run in."""
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
    fibo(20)  # warm the code path
    print(json.dumps({
        "version": sys.version,
        "fibo31_s": [timed(31) for _ in range(3)],
        "fibo10_s": timed(10),
    }))
