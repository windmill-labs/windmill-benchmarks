Windmill VS Airflow, Prefect and Temporal
=============================================

### Benchmarks

2 kind of workflows:
- 10 sequential long running tasks 
- 40 sequential lightweight tasks

We took the Fibonacci numbers as a good template for both use case:
```python
def fibo(n: int):
    if n <= 1:
        return n
    else:
        return fibo(n - 1) + fibo(n - 2)
```
For the lightweight tasks we computed `fibo(10)` and for the long running tasks, we choose `fibo(33)`.

We executed the benchmarks on an EC2 t2-medium instance, always using Docker compose to wpin up the orchestrator.

Retrieving the results and assembling them is a very manual tasks and differs by orchestrator.

### Howtos

[Airflow](./airflow/README.md)
[Prefect](./prefect/README.md)
[Temporal](./temporal/README.md)
[Windmill](./windmill/README.md)
