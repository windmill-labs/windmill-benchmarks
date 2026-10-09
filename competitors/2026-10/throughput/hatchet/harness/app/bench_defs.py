"""Throughput benchmark task definitions.

Two independent single-task workflows ("standalone tasks" in Hatchet terms), so a
batch of N runs is N tasks that are all immediately runnable - no DAG, no parents,
nothing serialising them.

  tp-sleep : body is time.sleep(0.1)   -> releases the GIL, 20 slots in one process
                                          really do serve 20 tasks at once
  tp-cpu   : body is naive recursive fibo(31) -> ~100ms of real CPU, holds the GIL,
                                          so concurrency must come from 20 processes
"""
import os

from hatchet_sdk import Context, Hatchet
from pydantic import BaseModel

hatchet = Hatchet(debug=False)

FIB_N = int(os.environ.get("FIB_N", "31"))
SLEEP_S = float(os.environ.get("SLEEP_S", "0.1"))


class In(BaseModel):
    i: int = 0


def fibo(n: int) -> int:
    return n if n <= 1 else fibo(n - 1) + fibo(n - 2)


@hatchet.task(name="tp-sleep", input_validator=In)
def tp_sleep(input: In, ctx: Context) -> dict:
    import time

    time.sleep(SLEEP_S)
    return {"ok": 1}


@hatchet.task(name="tp-cpu", input_validator=In)
def tp_cpu(input: In, ctx: Context) -> dict:
    return {"r": fibo(FIB_N)}


TASKS = {"sleep": tp_sleep, "cpu": tp_cpu}
