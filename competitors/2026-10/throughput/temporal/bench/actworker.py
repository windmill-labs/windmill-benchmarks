"""Activity worker with exactly SLOTS concurrent activity slots.

The worker is held off until the driver has finished enqueueing every activity
task for a rep, so the measured window contains execution only.  Started and
stopped once per rep via files in /ctl; the executor (and, for the cpu
workload, its warmed process pool) survives across reps.
"""
import asyncio
import concurrent.futures
import json
import multiprocessing.managers
import os
import sys
import time

from temporalio.client import Client
from temporalio.worker import PollerBehaviorSimpleMaximum, SharedStateManager, Worker

from common import cpu_activity, fibo, sleep_activity

KIND = sys.argv[1]
SLOTS = int(sys.argv[2])
REPS = int(sys.argv[3])
POLLERS = int(sys.argv[4]) if len(sys.argv) > 4 else 0  # 0 = SDK default (5)


def cpu_usec():
    """Container-local cpu time, from its own cgroup v2 cpu.stat."""
    try:
        with open("/sys/fs/cgroup/cpu.stat") as f:
            for line in f:
                if line.startswith("usage_usec"):
                    return int(line.split()[1])
    except Exception:
        pass
    return None


async def main() -> None:
    client = await Client.connect(os.environ["TEMPORAL_ADDRESS"])
    ssm = None
    if KIND == "sleep":
        ex = concurrent.futures.ThreadPoolExecutor(max_workers=SLOTS)
        acts = [sleep_activity]
        mech = (
            "single worker process, ThreadPoolExecutor(max_workers=20) as "
            "activity_executor, max_concurrent_activities=20"
        )
        list(ex.map(time.sleep, [0.01] * SLOTS))
    else:
        ex = concurrent.futures.ProcessPoolExecutor(max_workers=SLOTS)
        mgr = multiprocessing.managers.SyncManager()
        mgr.start()
        ssm = SharedStateManager.create_from_multiprocessing(mgr)
        acts = [cpu_activity]
        mech = (
            "single worker process, ProcessPoolExecutor(max_workers=20) as "
            "activity_executor, max_concurrent_activities=20, 20 OS processes "
            "so 20 cores can be busy despite the GIL"
        )
        for _ in range(2):
            list(ex.map(fibo, [28] * SLOTS))

    mech += ", activity_task_poller_behavior=%s" % (
        "PollerBehaviorSimpleMaximum(%d)" % POLLERS if POLLERS else "SDK default PollerBehaviorSimpleMaximum(5)"
    )
    with open("/ctl/act_ready", "w") as f:
        f.write(mech)
    print("activity worker ready: %s" % mech, flush=True)

    for rep in range(REPS):
        while not os.path.exists("/ctl/go_%d" % rep):
            await asyncio.sleep(0.02)
        c0, t0 = cpu_usec(), time.time()
        async with Worker(
            client,
            task_queue="bench-act",
            activities=acts,
            activity_executor=ex,
            max_concurrent_activities=SLOTS,
            shared_state_manager=ssm,
            **(
                {"activity_task_poller_behavior": PollerBehaviorSimpleMaximum(POLLERS)}
                if POLLERS
                else {}
            ),
        ):
            while not os.path.exists("/ctl/stop_%d" % rep):
                await asyncio.sleep(0.05)
        c1, t1 = cpu_usec(), time.time()
        with open("/ctl/cpu_%d.json" % rep, "w") as f:
            json.dump(
                {
                    "rep": rep,
                    "cpu_usec_delta": (c1 - c0) if (c0 is not None and c1 is not None) else None,
                    "worker_up_s": t1 - t0,
                    "mechanism": mech,
                },
                f,
            )
        print("rep %d: worker stopped" % rep, flush=True)
    print("ALL REPS DONE", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
