import asyncio, os, sys, time
from temporalio.client import Client
from temporalio.api.enums.v1 import TaskQueueType, EventType
from temporalio.api.taskqueue.v1 import TaskQueue, TaskQueueVersionSelection
from temporalio.api.workflowservice.v1 import DescribeTaskQueueRequest
from workflows import FanWorkflow

TOTAL = int(sys.argv[1]); PER = int(sys.argv[2]); TAGX = sys.argv[3]
SCHED = EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED

async def raw(c, q="bench-act"):
    req = DescribeTaskQueueRequest(
        namespace="default", task_queue=TaskQueue(name=q), api_mode=2,
        versions=TaskQueueVersionSelection(unversioned=True),
        task_queue_types=[TaskQueueType.TASK_QUEUE_TYPE_ACTIVITY], report_stats=True)
    return await c.service_client.workflow_service.describe_task_queue(req)

async def sched_count(c, wids):
    sem = asyncio.Semaphore(16)
    async def one(w):
        async with sem:
            h = await c.get_workflow_handle(w).fetch_history()
            return sum(1 for e in h.events if e.event_type == SCHED)
    return sum(await asyncio.gather(*[one(w) for w in wids]))

async def main():
    c = await Client.connect(os.environ["TEMPORAL_ADDRESS"])
    n = TOTAL // PER
    wids = ["%s-w%03d" % (TAGX, k) for k in range(n)]
    t0 = time.time()
    hs = await asyncio.gather(*[c.start_workflow(FanWorkflow.run,
        args=["sleep_activity", 0.1, PER, "bench-act", w],
        id=w, task_queue="bench-wf") for w in wids])
    print("submitted %d wf in %.2fs" % (n, time.time()-t0), flush=True)
    for i in range(14):
        sc = await sched_count(c, wids)
        print("t=%.2f scheduled_events=%d" % (time.time()-t0, sc), flush=True)
        if sc >= TOTAL: break
        await asyncio.sleep(0.5)
    r = await raw(c)
    print("=== RAW DESCRIBE ===", flush=True)
    print(r, flush=True)
    for h in hs:
        await h.terminate()

asyncio.run(main())
