#!/usr/bin/env python3
"""Build hatchet_runtime.json from the per-container interpreter probes."""
import json
import os
import sys

SRC, DST = sys.argv[1], sys.argv[2]
py = json.load(open(os.path.join(SRC, "interpreter_py.json")))
ts = json.load(open(os.path.join(SRC, "interpreter_ts.json")))
pyver = open(os.path.join(SRC, "pyver.txt")).read().strip().splitlines()
tsver = open(os.path.join(SRC, "tsver.txt")).read().strip().splitlines()

out = {
    "hatchet": {
        "engine_version": "v0.107.0",
        "image": "ghcr.io/hatchet-dev/hatchet/hatchet-lite@sha256:db8fb8e009a5c892859e52e4aa50b17aa11ab1dd23801cd320492f2942db30d7",
        "image_tag_resolved_from_digest": "v0.107.0",
        "postgres": "15.6",
        "engine_default_version": "V1",
    },
    "python": {
        "worker_image": "python:3.12-slim",
        "sdk": "hatchet-sdk 1.40.1",
        "version": py["version"],
        "fibo33_s": py["fibo33_s"],
        "fibo33_s_mean": round(sum(py["fibo33_s"]) / len(py["fibo33_s"]), 6),
        "fibo10_s": py["fibo10_s"],
        "measured_in": "docker exec bench-worker python interpreter_bench.py",
    },
    "typescript": {
        "worker_image": "node:22-slim",
        "sdk": "@hatchet-dev/typescript-sdk " + (tsver[1] if len(tsver) > 1 else "1.36.0"),
        "version": ts["version"],
        "fibo33_s": ts["fibo33_s"],
        "fibo33_s_mean": round(sum(ts["fibo33_s"]) / len(ts["fibo33_s"]), 6),
        "fibo10_s": ts["fibo10_s"],
        "measured_in": "docker exec bench-worker-ts node dist/interpreter_bench.js",
    },
    "host": {
        "instance": "AWS EC2 m7i.xlarge, 4 vCPU, 16 GiB, us-east-2",
        "note": "one engine on the box, one worker at a time; Python and TypeScript phases "
                "separated by idle gaps and each preceded by a 90s settle",
    },
}
with open(os.path.join(DST, "hatchet_runtime.json"), "w") as f:
    json.dump(out, f, indent=2)
print(json.dumps(out, indent=2))
