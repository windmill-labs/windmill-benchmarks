#!/usr/bin/env python3
"""Dump one workflow's full event history with relative timestamps."""
import asyncio
import sys

from temporalio.client import Client


async def main():
    wf_id = sys.argv[1]
    client = await Client.connect("localhost:7233")
    h = client.get_workflow_handle(wf_id)
    evs = []
    async for ev in h.fetch_history_events():
        evs.append(ev)
    t0 = evs[0].event_time.seconds + evs[0].event_time.nanos / 1e9
    prev = t0
    for ev in evs:
        t = ev.event_time.seconds + ev.event_time.nanos / 1e9
        name = ev.event_type
        try:
            from temporalio.api.enums.v1 import EventType

            name = EventType.Name(ev.event_type).replace("EVENT_TYPE_", "")
        except Exception:
            pass
        print(f"{ev.event_id:4d} {t - t0:8.3f} (+{t - prev:6.3f}) {name}")
        prev = t


asyncio.run(main())
