"""Configuration 2: 2000 deployment runs on a process work pool.

All 2000 flow runs are created up front, concurrently, through the API
client (not run_deployment), so creation is never the bottleneck and the
5s run_deployment poll_interval plays no part. The driver then waits for
every run to reach a terminal state; all timings are read from the
database afterwards, never from this process's clock.
"""
import asyncio
import json
import os
import sys
import time

from prefect import get_client

N_UNITS = int(os.environ.get("N_UNITS", "2000"))
CREATE_CONC = int(os.environ.get("CREATE_CONC", "40"))


async def run(workload: str, n: int, tag: str, out: str) -> None:
    async with get_client() as client:
        dep = await client.read_deployment_by_name("unit/bench")
        sem = asyncio.Semaphore(CREATE_CONC)
        ids = []

        async def one(i: int):
            async with sem:
                fr = await client.create_flow_run_from_deployment(
                    dep.id,
                    parameters={"workload": workload, "n": n},
                    tags=[tag],
                    name=f"{tag}-{i:04d}",
                )
                ids.append(str(fr.id))

        c0 = time.time()
        await asyncio.gather(*(one(i) for i in range(N_UNITS)))
        c1 = time.time()
        print(json.dumps({"created": len(ids), "create_s": round(c1 - c0, 3)}), flush=True)

        done = 0
        last = -1
        t0 = time.time()
        while done < N_UNITS:
            await asyncio.sleep(2)
            runs = await client.read_flow_runs(
                flow_run_filter={"tags": {"all_": [tag]}},
                limit=1,
            ) if False else None
            # count terminal states cheaply via the counts endpoint
            resp = await client._client.post(
                "/flow_runs/count",
                json={"flow_runs": {"tags": {"all_": [tag]},
                                    "state": {"type": {"any_": ["COMPLETED", "FAILED", "CRASHED", "CANCELLED"]}}}},
            )
            done = resp.json()
            if done != last:
                print(f"{round(time.time()-t0,1)}s terminal={done}", flush=True)
                last = done
            if time.time() - t0 > 7200:
                print("TIMEOUT", flush=True)
                break
        w1 = time.time()
        resp = await client._client.post(
            "/flow_runs/count",
            json={"flow_runs": {"tags": {"all_": [tag]},
                                "state": {"type": {"any_": ["COMPLETED"]}}}},
        )
        completed = resp.json()
        rec = {"tag": tag, "workload": workload, "n": n, "units": N_UNITS,
               "create_s": round(c1 - c0, 3), "create_start": c0, "create_end": c1,
               "wait_end": w1, "terminal": done, "completed": completed}
        with open(out, "w") as fh:
            json.dump(rec, fh)
        print(json.dumps(rec), flush=True)


if __name__ == "__main__":
    asyncio.run(run(sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]))
