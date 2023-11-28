import asyncio
from dataclasses import dataclass
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.worker import Worker

ITER = 40
FIBO_N = 10


def fibo(n: int):
    if n <= 1:
        return n
    else:
        return fibo(n - 1) + fibo(n - 2)


@activity.defn
async def fibo_activity(n: int) -> int:
    return fibo(n)


@workflow.defn
class Bench10Workflow:
    @workflow.run
    async def run(self) -> None:
        for i in range(ITER):
            await workflow.execute_activity(
                fibo_activity,
                FIBO_N,
                activity_id="task_{}".format(i),
                start_to_close_timeout=timedelta(seconds=360),
            )


async def main():
    client = await Client.connect("localhost:7233")

    async with Worker(
        client,
        task_queue="bench-{}".format(ITER),
        workflows=[Bench10Workflow],
        activities=[fibo_activity],
    ):
        await client.execute_workflow(
            Bench10Workflow.run,
            id="bench-{}".format(ITER),
            task_queue="bench-{}".format(ITER),
        )


if __name__ == "__main__":
    asyncio.run(main())
