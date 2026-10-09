"""Read activity timings out of Temporal's workflow event history."""
import asyncio
import json
import os
import sys

from temporalio.client import Client


def ts(e):
    t = e.event_time
    return t.seconds + t.nanos / 1e9


async def main():
    out_path = sys.argv[1]
    lang = sys.argv[2]
    config = sys.argv[3]
    workload = sys.argv[4]
    tag = sys.argv[5]
    rep_ids = [int(x) for x in sys.argv[6].split(",")]
    addr = os.environ.get("TEMPORAL_ADDRESS", "temporal:7233")
    client = await Client.connect(addr)

    reps = []
    for r in rep_ids:
        handle = client.get_workflow_handle("{}-rep{}".format(tag, r))
        hist = await handle.fetch_history()
        sched = {}
        started = {}
        completed = {}
        for e in hist.events:
            which = e.WhichOneof("attributes")
            if which == "activity_task_scheduled_event_attributes":
                sched[e.event_id] = ts(e)
            elif which == "activity_task_started_event_attributes":
                started[e.activity_task_started_event_attributes.scheduled_event_id] = ts(e)
            elif which == "activity_task_completed_event_attributes":
                completed[e.activity_task_completed_event_attributes.scheduled_event_id] = ts(e)
        keys = sorted(sched)
        if not keys:
            raise SystemExit("no activities found for rep {}".format(r))
        base = sched[keys[0]]
        reps.append(
            {
                "workers": 1,
                "created_at": [round(sched[k] - base, 3) for k in keys],
                "started_at": [round(started[k] - base, 3) for k in keys],
                "completed_at": [round(completed[k] - base, 3) for k in keys],
            }
        )

    with open(out_path, "w") as f:
        json.dump(
            {"lang": lang, "config": config, "workload": workload, "reps": reps}, f
        )
    print("wrote {} ({} reps)".format(out_path, len(reps)))


if __name__ == "__main__":
    asyncio.run(main())
