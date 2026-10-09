import asyncio
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy


@workflow.defn
class FanWorkflow:
    """Fans out `count` independent activities onto a dedicated activity task
    queue and waits for all of them."""

    @workflow.run
    async def run(
        self, act_name: str, arg: Any, count: int, act_queue: str, base: str
    ) -> int:
        futs = [
            workflow.execute_activity(
                act_name,
                args=[arg],
                task_queue=act_queue,
                activity_id="%s-%04d" % (base, i),
                start_to_close_timeout=timedelta(seconds=600),
                retry_policy=RetryPolicy(maximum_attempts=1),
            )
            for i in range(count)
        ]
        res = await asyncio.gather(*futs)
        return len(res)
