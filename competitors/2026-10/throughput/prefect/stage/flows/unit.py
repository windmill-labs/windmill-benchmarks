"""One dispatched unit: a flow of its own, deployed to the process work pool."""
import time

from prefect import flow


def fibo(n: int) -> int:
    return n if n <= 1 else fibo(n - 1) + fibo(n - 2)


@flow(name="unit")
def unit(workload: str = "sleep", n: int = 31):
    if workload == "sleep":
        time.sleep(0.1)
        return 0
    return fibo(n)
