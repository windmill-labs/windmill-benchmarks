import asyncio
import os
import sys
import time
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.worker import Worker


def fibo(n: int) -> int:
    if n <= 1:
        return n
    return fibo(n - 1) + fibo(n - 2)


@activity.defn
async def fibo_activity(n: int) -> int:
    return fibo(n)


@workflow.defn
class BenchWorkflow:
    @workflow.run
    async def run(self, iters: int, fibo_n: int) -> None:
        for i in range(iters):
            await workflow.execute_activity(
                fibo_activity,
                fibo_n,
                activity_id="task_{}".format(i),
                start_to_close_timeout=timedelta(seconds=360),
            )


async def main():
    iters = int(sys.argv[1])
    fibo_n = int(sys.argv[2])
    reps = int(sys.argv[3])
    gap = float(sys.argv[4])
    tag = sys.argv[5]
    addr = os.environ.get("TEMPORAL_ADDRESS", "temporal:7233")
    client = await Client.connect(addr)
    tq = "bench-{}".format(tag)
    async with Worker(
        client,
        task_queue=tq,
        workflows=[BenchWorkflow],
        activities=[fibo_activity],
    ):
        # give the pollers a moment to register on the task queue
        await asyncio.sleep(2)
        for r in range(reps):
            t0 = time.monotonic()
            await client.execute_workflow(
                BenchWorkflow.run,
                args=[iters, fibo_n],
                id="{}-rep{}".format(tag, r),
                task_queue=tq,
            )
            print("rep {} wall {:.3f}".format(r, time.monotonic() - t0), flush=True)
            if r < reps - 1:
                await asyncio.sleep(gap)
    print("DONE", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
