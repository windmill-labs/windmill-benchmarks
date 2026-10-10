"""Trigger N sequential child flow runs through the work pool.

Each unit is a deployment run: created by this client, polled for by the
worker, executed as a subprocess, recorded. That is the same unit of work
the other engines in the study are measured on.
"""
import sys

from prefect.deployments import run_deployment


def main(iters: int, n: int, tag: str, poll_interval: float) -> None:
    for i in range(iters):
        fr = run_deployment(
            name="unit/bench",
            parameters={"n": n},
            tags=[tag],
            timeout=None,
            poll_interval=poll_interval,
        )
        print(f"{i} {fr.name} {fr.state_type}", flush=True)


if __name__ == "__main__":
    main(int(sys.argv[1]), int(sys.argv[2]), sys.argv[3], float(sys.argv[4]))
