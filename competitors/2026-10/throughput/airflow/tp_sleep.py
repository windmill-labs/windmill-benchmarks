"""Throughput workload A: 2000 independent task instances, body = time.sleep(0.1).

Fan-out uses dynamic task mapping over 2000 inputs, so all 2000 task instances
belong to a single DAG run and are created up-front by the scheduler.
"""

import time
from datetime import datetime

from airflow.sdk import DAG, task

N = 2000

with DAG(
    dag_id="tp_sleep",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["benchmark"],
) as dag:

    @task(task_id="unit")
    def unit(i: int):
        time.sleep(0.1)
        return i

    unit.expand(i=list(range(N)))
