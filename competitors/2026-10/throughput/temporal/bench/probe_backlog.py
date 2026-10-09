import asyncio, os, sys
from temporalio.client import Client
from temporalio.api.enums.v1 import TaskQueueType
from temporalio.api.taskqueue.v1 import TaskQueue, TaskQueueVersionSelection
from temporalio.api.workflowservice.v1 import DescribeTaskQueueRequest

async def main():
    c = await Client.connect(os.environ["TEMPORAL_ADDRESS"])
    req = DescribeTaskQueueRequest(
        namespace="default",
        task_queue=TaskQueue(name=sys.argv[1]),
        api_mode=2,  # ENHANCED
        versions=TaskQueueVersionSelection(unversioned=True),
        task_queue_types=[TaskQueueType.TASK_QUEUE_TYPE_ACTIVITY],
        report_stats=True,
    )
    r = await c.service_client.workflow_service.describe_task_queue(req)
    print(r)

asyncio.run(main())
