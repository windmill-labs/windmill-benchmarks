from datetime import datetime

from airflow import DAG
from airflow.decorators import dag, task

ITER = 10
FIBO_N = 33


def fibo(n):
    if n <= 1:
        return n
    else:
        return fibo(n - 1) + fibo(n - 2)


with DAG(
    dag_id="bench_{}".format(ITER),
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["benchmark"],
) as dag:
    for i in range(ITER):

        @task(task_id=f"task_{i}")
        def task_module():
            return fibo(FIBO_N)

        fibo_task = task_module()

        if i > 0:
            previous_task >> fibo_task
        previous_task = fibo_task
