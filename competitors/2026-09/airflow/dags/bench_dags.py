"""Orchestration benchmark DAGs: N sequential TaskFlow tasks, each computing fibo(k)."""
from __future__ import annotations

import datetime

from airflow.sdk import dag, task


def fibo(n):
    return n if n <= 1 else fibo(n - 1) + fibo(n - 2)


def build(dag_id: str, n_tasks: int, fib_n: int):
    @dag(
        dag_id=dag_id,
        schedule=None,
        start_date=datetime.datetime(2024, 1, 1),
        catchup=False,
        tags=["bench"],
    )
    def _dag():
        prev = None
        for i in range(n_tasks):

            @task(task_id=f"t{i}")
            def step(k: int = fib_n):
                return fibo(k)

            cur = step()
            if prev is not None:
                prev >> cur
            prev = cur

    return _dag()


bench_40_10 = build("bench_40_10", 40, 10)
bench_10_33 = build("bench_10_33", 10, 33)
