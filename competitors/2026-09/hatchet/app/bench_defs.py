"""Shared Hatchet workflow declarations for the orchestration benchmark.

Two workflows, both strictly sequential: task i+1 declares task i as its parent.
"""
from hatchet_sdk import Context, EmptyModel, Hatchet

hatchet = Hatchet(debug=False)


def fibo(n: int) -> int:
    return n if n <= 1 else fibo(n - 1) + fibo(n - 2)


def build(name: str, steps: int, n: int):
    wf = hatchet.workflow(name=name)
    prev = None
    for i in range(steps):

        def make(nn: int):
            def fn(input: EmptyModel, ctx: Context) -> dict:
                return {"result": fibo(nn)}

            return fn

        prev = wf.task(name=f"s{i}", parents=([prev] if prev is not None else []))(make(n))
    return wf


WF_40_10 = build("bench-40-10", 40, 10)
WF_10_33 = build("bench-10-33", 10, 33)

WORKFLOWS = {"fibonacci_40_10": WF_40_10, "fibonacci_10_33": WF_10_33}
