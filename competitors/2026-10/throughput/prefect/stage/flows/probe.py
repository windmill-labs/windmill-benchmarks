"""Runtime probe: executed in the same image/process shape as the bodies."""
import json
import platform
import sys
import time


def fibo(n: int) -> int:
    return n if n <= 1 else fibo(n - 1) + fibo(n - 2)


if __name__ == "__main__":
    fibo(20)  # touch the code path
    t = []
    for _ in range(3):
        s = time.perf_counter()
        fibo(31)
        t.append(round(time.perf_counter() - s, 4))
    import prefect
    print(json.dumps({
        "python_version": sys.version,
        "python_version_short": platform.python_version(),
        "implementation": platform.python_implementation(),
        "prefect_version": prefect.__version__,
        "fibo31_s": t,
        "fibo31_median_s": sorted(t)[1],
    }))
