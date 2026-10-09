"""Activity definitions. Kept free of workflow imports so the workflow sandbox
never has to import this module."""
import time

from temporalio import activity

ACT_QUEUE = "bench-act"
WF_QUEUE = "bench-wf"


def fibo(n: int) -> int:
    if n <= 1:
        return n
    return fibo(n - 1) + fibo(n - 2)


@activity.defn(name="sleep_activity")
def sleep_activity(seconds: float) -> int:
    time.sleep(seconds)
    return 1


@activity.defn(name="cpu_activity")
def cpu_activity(n: int) -> int:
    return fibo(n)
