"""Workflow worker. Serves the bench-wf task queue only, so it never competes
for activity slots."""
import asyncio
import os

from temporalio.client import Client
from temporalio.worker import Worker

from workflows import FanWorkflow


async def main() -> None:
    client = await Client.connect(os.environ["TEMPORAL_ADDRESS"])
    async with Worker(client, task_queue="bench-wf", workflows=[FanWorkflow]):
        with open("/ctl/wf_ready", "w") as f:
            f.write("1")
        print("workflow worker ready", flush=True)
        while True:
            await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(main())
