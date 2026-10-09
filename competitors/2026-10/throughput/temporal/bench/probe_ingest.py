"""With no activity worker running, start the workflows and watch how fast the
activity tasks actually land in matching."""
import asyncio, os, sys, time
from temporalio.client import Client
from temporalio.api.enums.v1 import TaskQueueType, EventType
from temporalio.api.taskqueue.v1 import TaskQueue, TaskQueueVersionSelection
from temporalio.api.workflowservice.v1 import DescribeTaskQueueRequest
from workflows import FanWorkflow

TOTAL = int(sys.argv[1]); PER = int(sys.argv[2]); TAGX = sys.argv[3]

async def backlog(c, q="bench-act"):
    req = DescribeTaskQueueRequest(
        namespace="default", task_queue=TaskQueue(name=q), api_mode=2,
        versions=TaskQueueVersionSelection(unversioned=True),
        task_queue_types=[TaskQueueType.TASK_QUEUE_TYPE_ACTIVITY], report_stats=True)
    r = await c.service_client.workflow_service.describe_task_queue(req)
    tot = r.stats.approximate_backlog_count
    for vi in r.versions_info.values():
        for ti in vi.types_info.values():
            tot += ti.stats.approximate_backlog_count
    return tot

async def main():
    c = await Client.connect(os.environ["TEMPORAL_ADDRESS"])
    n = TOTAL // PER
    t0 = time.time()
    hs = await asyncio.gather(*[c.start_workflow(FanWorkflow.run,
        args=["sleep_activity", 0.1, PER, "bench-act", "%s-w%03d" % (TAGX, k)],
        id="%s-w%03d" % (TAGX, k), task_queue="bench-wf") for k in range(n)])
    print("submitted %d wf in %.2fs" % (n, time.time()-t0), flush=True)
    for i in range(80):
        b = await backlog(c)
        print("t=%.2f backlog=%d" % (time.time()-t0, b), flush=True)
        if b >= TOTAL: break
        await asyncio.sleep(0.5)
    print("final t=%.2f backlog=%d" % (time.time()-t0, b), flush=True)
    for h in hs:
        await h.terminate()

asyncio.run(main())
