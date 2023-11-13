from prefect import flow, task

ITER = 10
FIBO_N = 33


def fibo(n: int):
    if n <= 1:
        return n
    else:
        return fibo(n - 1) + fibo(n - 2)


@task
def fibo_task():
    return fibo(FIBO_N)


@flow(name="bench_{}".format(ITER))
def benchmark_flow():
    for i in range(ITER):
        fibo_task()


if __name__ == "__main__":
    benchmark_flow.serve(name="bench_{}".format(ITER))
