#!/usr/bin/env python3
"""Windmill side of the orchestrator benchmark.

Creates a flow of N sequential python steps, each computing fibo(n), runs it,
and emits the per-step timings in the schema the docs site consumes:
{workers, created_at[], started_at[], completed_at[]} in seconds, relative to
the first step's created_at.
"""
import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

BASE = "http://localhost"
WS = "admins"
# A Windmill token for the `admins` workspace. Export WM_TOKEN, or point
# WM_TOKEN_FILE at a file holding it. Never commit either.
TOKEN = os.environ.get("WM_TOKEN") or open(
    os.environ.get("WM_TOKEN_FILE", "wm_token")
).read().strip()
DB_CONTAINER = "windmill-db-1"

SCRIPT = """def main(n: int = 10):
    def fibo(x: int) -> int:
        return x if x <= 1 else fibo(x - 1) + fibo(x - 2)
    return fibo(n)
"""


def api(method, path, body=None, raw=False):
    req = urllib.request.Request(
        BASE + path,
        method=method,
        headers={"Authorization": "Bearer " + TOKEN, "Content-Type": "application/json"},
    )
    data = json.dumps(body).encode() if body is not None else None
    with urllib.request.urlopen(req, data, timeout=60) as r:
        text = r.read().decode()
    if raw:
        return text
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def make_flow(path, steps, n):
    modules = [
        {
            "id": f"s{i}",
            "value": {
                "type": "rawscript",
                "language": "python3",
                "content": SCRIPT,
                "input_transforms": {"n": {"type": "static", "value": n}},
            },
        }
        for i in range(steps)
    ]
    body = {
        "path": path,
        "summary": f"bench {steps} x fibo({n})",
        "value": {"modules": modules},
        "schema": {"type": "object", "properties": {}, "required": []},
    }
    try:
        api("POST", f"/api/w/{WS}/flows/create", body, raw=True)
    except urllib.error.HTTPError as e:
        if e.code not in (400, 409):
            raise
        api("POST", f"/api/w/{WS}/flows/update/{path}", body, raw=True)


def run_flow(path, timeout_s=1800):
    job_id = api("POST", f"/api/w/{WS}/jobs/run/f/{path}", {}, raw=True).strip().strip('"')
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        job = api("GET", f"/api/w/{WS}/jobs_u/get/{job_id}")
        if job.get("type") == "CompletedJob":
            if not job.get("success", False):
                raise RuntimeError(f"flow job {job_id} failed: {json.dumps(job.get('result'))[:300]}")
            return job_id
        time.sleep(0.5)
    raise TimeoutError(f"flow job {job_id} did not finish in {timeout_s}s")


def step_timings(flow_job_id):
    """Per-step timings, pulled from the job table rather than from the API."""
    sql = (
        "SELECT EXTRACT(EPOCH FROM created_at)::text, EXTRACT(EPOCH FROM started_at)::text, "
        "COALESCE(duration_ms,0)::text FROM v2_as_completed_job "
        f"WHERE parent_job = '{flow_job_id}' ORDER BY created_at, started_at"
    )
    out = subprocess.run(
        ["sudo", "docker", "exec", DB_CONTAINER, "psql", "-U", "postgres", "-d", "windmill",
         "-t", "-A", "-F", "|", "-c", sql],
        capture_output=True, text=True, check=True,
    ).stdout.strip()
    rows = [r.split("|") for r in out.splitlines() if r.strip()]
    if not rows:
        raise RuntimeError(f"no child jobs found for {flow_job_id}")
    created = [float(r[0]) for r in rows]
    started = [float(r[1]) for r in rows]
    dur = [float(r[2]) / 1000.0 for r in rows]
    t0 = min(created)
    return {
        "workers": 1,
        "created_at": [round(c - t0, 3) for c in created],
        "started_at": [round(s - t0, 3) for s in started],
        "completed_at": [round(s - t0 + d, 3) for s, d in zip(started, dur)],
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--steps", type=int, required=True)
    ap.add_argument("--fib", type=int, required=True)
    ap.add_argument("--reps", type=int, default=1)
    ap.add_argument("--warmup", type=int, default=1)
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    path = f"u/admin/bench_{args.steps}_{args.fib}"
    make_flow(path, args.steps, args.fib)

    runs = []
    for i in range(args.warmup + args.reps):
        job_id = run_flow(path)
        t = step_timings(job_id)
        total = max(t["completed_at"])
        label = "warmup" if i < args.warmup else f"rep {i - args.warmup + 1}"
        print(f"  {label}: {len(t['created_at'])} steps, total {total:.3f}s", file=sys.stderr)
        if i >= args.warmup:
            runs.append(t)

    result = {"steps": args.steps, "fib": args.fib, "runs": runs,
              "totals": [round(max(r["completed_at"]), 3) for r in runs]}
    text = json.dumps(result, indent=2)
    if args.out:
        open(args.out, "w").write(text)
    print(text)


if __name__ == "__main__":
    main()
