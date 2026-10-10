"""Throughput workload B: 2000 independent task instances, body = naive fibo(31).

Fan-out uses dynamic task mapping over 2000 inputs, so all 2000 task instances
belong to a single DAG run and are created up-front by the scheduler.
"""

from datetime import datetime

from airflow.sdk import DAG, task

N = 2000
FIBO_N = 31


def fibo(n: int) -> int:
    if n <= 1:
        return n
    return fibo(n - 1) + fibo(n - 2)


with DAG(
    dag_id="tp_cpu",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["benchmark"],
) as dag:

    @task(task_id="unit")
    def unit(i: int):
        return fibo(FIBO_N)

    unit.expand(i=list(range(N)))
