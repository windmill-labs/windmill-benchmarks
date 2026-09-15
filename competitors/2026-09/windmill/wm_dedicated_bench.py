#!/usr/bin/env python3
"""Dedicated-worker benchmark for Windmill EE.

Dedicated workers bind to a deployed script path, so the flow has to call a
script rather than inline code. That is a second change on top of "dedicated",
so this also runs a script-based control on the normal worker: the only
difference between the two rows is which worker group runs the steps.
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
        BASE + path, method=method,
        headers={"Authorization": "Bearer " + TOKEN, "Content-Type": "application/json"},
    )
    data = json.dumps(body).encode() if body is not None else None
    with urllib.request.urlopen(req, data, timeout=120) as r:
        text = r.read().decode()
    if raw:
        return text
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def ensure_script(path, dedicated):
    """Deploy (or redeploy) the fibo script, with or without the dedicated flag."""
    body = {
        "path": path,
        "summary": f"fibo bench (dedicated={dedicated})",
        "description": "",
        "content": SCRIPT,
        "language": "python3",
        "dedicated_worker": dedicated,
        "schema": {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {"n": {"type": "integer", "default": 10}},
            "required": [],
        },
    }
    try:
        existing = api("GET", f"/api/w/{WS}/scripts/get/p/{path}")
        body["parent_hash"] = existing["hash"]
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise
    return api("POST", f"/api/w/{WS}/scripts/create", body, raw=True).strip()


def set_worker_group(group, script_path):
    api("POST", f"/api/configs/update/worker__{group}",
        {"dedicated_worker": f"{WS}:{script_path}" if script_path else None}, raw=True)


def make_flow(path, steps, n, script_path):
    modules = [
        {
            "id": f"s{i}",
            "value": {
                "type": "script",
                "path": script_path,
                "input_transforms": {"n": {"type": "static", "value": n}},
            },
        }
        for i in range(steps)
    ]
    body = {
        "path": path,
        "summary": f"bench {steps} x fibo({n}) via {script_path}",
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
                raise RuntimeError(f"flow {job_id} failed: {json.dumps(job.get('result'))[:300]}")
            return job_id
        time.sleep(0.5)
    raise TimeoutError(f"flow {job_id} did not finish")


def step_timings(flow_job_id):
    sql = (
        "SELECT EXTRACT(EPOCH FROM created_at)::text, EXTRACT(EPOCH FROM started_at)::text, "
        "COALESCE(duration_ms,0)::text, COALESCE(tag,'') FROM v2_as_completed_job "
        f"WHERE parent_job = '{flow_job_id}' ORDER BY created_at, started_at"
    )
    out = subprocess.run(
        ["sudo", "docker", "exec", DB_CONTAINER, "psql", "-U", "postgres", "-d", "windmill",
         "-t", "-A", "-F", "|", "-c", sql],
        capture_output=True, text=True, check=True,
    ).stdout.strip()
    rows = [r.split("|") for r in out.splitlines() if r.strip()]
    if not rows:
        raise RuntimeError(f"no child jobs for {flow_job_id}")
    created = [float(r[0]) for r in rows]
    started = [float(r[1]) for r in rows]
    dur = [float(r[2]) / 1000.0 for r in rows]
    tags = {r[3] for r in rows}
    t0 = min(created)
    return {
        "workers": 1,
        "created_at": [round(c - t0, 3) for c in created],
        "started_at": [round(s - t0, 3) for s in started],
        "completed_at": [round(s - t0 + d, 3) for s, d in zip(started, dur)],
    }, tags


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--steps", type=int, required=True)
    ap.add_argument("--fib", type=int, required=True)
    ap.add_argument("--mode", choices=["dedicated", "control"], required=True)
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--warmup", type=int, default=1)
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    dedicated = args.mode == "dedicated"
    script_path = f"u/admin/fibo_{'ded' if dedicated else 'plain'}"
    ensure_script(script_path, dedicated)
    if dedicated:
        set_worker_group("dedicated", script_path)
        print("  waiting 25s for the dedicated worker to pick up its config", file=sys.stderr)
        time.sleep(25)

    flow_path = f"u/admin/bench_{args.mode}_{args.steps}_{args.fib}"
    make_flow(flow_path, args.steps, args.fib, script_path)

    runs, all_tags = [], set()
    for i in range(args.warmup + args.reps):
        job_id = run_flow(flow_path)
        t, tags = step_timings(job_id)
        all_tags |= tags
        total = max(t["completed_at"])
        label = "warmup" if i < args.warmup else f"rep {i - args.warmup + 1}"
        print(f"  {label}: {len(t['created_at'])} steps, total {total:.3f}s, tags={sorted(tags)}", file=sys.stderr)
        if i >= args.warmup:
            runs.append(t)

    result = {
        "engine": "windmill_dedicated" if dedicated else "windmill_script",
        "version": "1.811.1 EE",
        "workload": f"fibonacci_{args.steps}_{args.fib}",
        "runs": runs,
        "totals": [round(max(r["completed_at"]), 3) for r in runs],
        "step_tags": sorted(all_tags),
        "notes": (
            "Windmill EE 1.811.1. Flow steps call a deployed python script by path (dedicated "
            "workers bind to a script path, so inline steps cannot be used). "
            + ("Steps run on a dedicated worker (worker group 'dedicated', dedicated_worker set to "
               "the script, script setting dedicated_worker=true). " if dedicated else
               "CONTROL: identical flow and script, steps run on the normal worker, so the only "
               "difference from the dedicated row is which worker group executes them. ")
            + "SLEEP_QUEUE=5 on both workers. Timings from v2_as_completed_job for children of the "
              "flow job. 1 warmup discarded + 5 reps."
        ),
    }
    text = json.dumps(result, indent=1)
    if args.out:
        open(args.out, "w").write(text)
    print(json.dumps({"totals": result["totals"], "step_tags": result["step_tags"]}))


if __name__ == "__main__":
    main()
