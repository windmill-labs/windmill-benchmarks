import os
from hatchet_sdk import Context, Hatchet
from pydantic import BaseModel

hatchet = Hatchet(debug=True)

class WorkflowInput(BaseModel):
    n: int = 10

fibo_wf = hatchet.workflow(name="StaticSequentialFibo", input_validator=WorkflowInput)

def fibo(n: int) -> int:
    if n <= 1:
        return n
    return fibo(n - 1) + fibo(n - 2)

ITERATIONS = int(os.environ.get("ITERATIONS", 5))
task_refs = []

for i in range(ITERATIONS):
    @fibo_wf.task(name=f"fibo_task_{i}", parents=[task_refs[i-1]] if i > 0 else [])
    def fibo_task(input: WorkflowInput, ctx: Context, idx=i) -> dict:
        print(f"Task {idx}: Computing fibo({input.n})")
        result = fibo(input.n)
        return {"index": idx, "fibo": result}

    task_refs.append(fibo_task)

def main() -> None:
    worker = hatchet.worker(slots=1, name="fibo-worker", workflows=[fibo_wf])
    worker.start()

if __name__ == "__main__":
    main()