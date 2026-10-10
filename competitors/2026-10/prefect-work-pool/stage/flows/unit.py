from prefect import flow


def fibo(n: int) -> int:
    return n if n <= 1 else fibo(n - 1) + fibo(n - 2)


@flow(name="unit")
def unit(n: int = 10) -> int:
    return fibo(n)
