#!/usr/bin/env python3
"""Kestra side of the orchestrator benchmark.

Builds a flow of N sequential io.kestra.plugin.scripts.python.Script tasks, each
computing fibo(n) with the naive recursion, runs it warmup+reps times, and emits
per-taskrun timings taken from Kestra's own execution object (state histories),
relative to the first taskrun's CREATED timestamp.
"""
import argparse
import base64
import datetime as dt
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request

BASE = "http://localhost:8080"
TENANT = "main"
USER = os.environ.get("KESTRA_USER", "admin@kestra.io")
PASS = os.environ["KESTRA_PASSWORD"]  # set to the password used at first-run setup
NS = "bench"

TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:?\d{2})$")

AUTH = "Basic " + base64.b64encode(f"{USER}:{PASS}".encode()).decode()

SCRIPT_BODY = """def fibo(n): return n if n <= 1 else fibo(n-1) + fibo(n-2)
print(fibo({n}))
"""


def api(method, path, body=None, ctype="application/json", raw=False, timeout=120):
    headers = {"Authorization": AUTH}
    data = None
    if body is not None:
        headers["Content-Type"] = ctype
        data = body.encode() if isinstance(body, str) else json.dumps(body).encode()
    req = urllib.request.Request(BASE + path, method=method, headers=headers)
    with urllib.request.urlopen(req, data, timeout=timeout) as r:
        text = r.read().decode()
    if raw:
        return text
    return json.loads(text) if text.strip() else None


PROCESS_RUNNER = "io.kestra.plugin.core.runner.Process"


def runner_lines(runner):
    """Docker is the plugin default, so omit taskRunner entirely for it."""
    if runner == "process":
        return ["    taskRunner:", f"      type: {PROCESS_RUNNER}"]
    return []


def flow_yaml(flow_id, steps, n, runner="docker"):
    lines = [f"id: {flow_id}", f"namespace: {NS}", "tasks:"]
    body = SCRIPT_BODY.format(n=n)
    for i in range(steps):
        lines.append(f"  - id: t{i:03d}")
        lines.append("    type: io.kestra.plugin.scripts.python.Script")
        lines.extend(runner_lines(runner))
        lines.append("    script: |")
        for bl in body.rstrip("\n").split("\n"):
            lines.append("      " + bl)
    return "\n".join(lines) + "\n"


def upsert_flow(flow_id, yaml_text):
    try:
        api("POST", f"/api/v1/{TENANT}/flows", yaml_text, "application/x-yaml", raw=True)
    except urllib.error.HTTPError as e:
        if e.code not in (409, 422, 400):
            raise
        api("PUT", f"/api/v1/{TENANT}/flows/{NS}/{flow_id}", yaml_text,
            "application/x-yaml", raw=True)


def run_flow(flow_id, timeout_s=3600):
    boundary = "----kestrabench"
    part = (f"--{boundary}\r\nContent-Disposition: form-data; name=\"_\"\r\n\r\n1\r\n"
            f"--{boundary}--\r\n")
    ex = api("POST", f"/api/v1/{TENANT}/executions/{NS}/{flow_id}", part,
             f"multipart/form-data; boundary={boundary}")
    eid = ex["id"]
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        ex = api("GET", f"/api/v1/{TENANT}/executions/{eid}")
        state = ex["state"]["current"]
        if state in ("SUCCESS", "FAILED", "KILLED", "WARNING"):
            if state != "SUCCESS":
                raise RuntimeError(f"execution {eid} ended {state}")
            return eid, ex
        time.sleep(0.25)
    raise TimeoutError(f"execution {eid} did not finish in {timeout_s}s")


def parse_ts(s):
    m = TS_RE.match(s)
    if not m:
        raise ValueError("unparseable timestamp: " + s)
    base, frac, tz = m.group(1), (m.group(2) or "0"), m.group(3)
    frac = (frac + "000000")[:6]
    tz = "+00:00" if tz == "Z" else (tz if ":" in tz else tz[:3] + ":" + tz[3:])
    return dt.datetime.fromisoformat(base + "." + frac + tz).timestamp()


def timings(ex, expected_steps):
    """created/started/completed per taskrun, from Kestra's state histories."""
    rows = []
    for tr in ex["taskRunList"]:
        hist = tr["state"]["histories"]
        by = {}
        for h in hist:
            by.setdefault(h["state"], h["date"])
        created = parse_ts(by["CREATED"])
        started = parse_ts(by.get("RUNNING", by["CREATED"]))
        completed = parse_ts(hist[-1]["date"])
        rows.append((created, started, completed))
    if len(rows) != expected_steps:
        raise RuntimeError(f"expected {expected_steps} taskruns, got {len(rows)}")
    rows.sort(key=lambda r: r[0])
    t0 = rows[0][0]
    return {
        "workers": 1,
        "created_at": [round(c - t0, 3) for c, _, _ in rows],
        "started_at": [round(s - t0, 3) for _, s, _ in rows],
        "completed_at": [round(e - t0, 3) for _, _, e in rows],
    }


INTERP_SCRIPT = """    script: |
      import sys, time, json
      def fibo(n): return n if n <= 1 else fibo(n-1) + fibo(n-2)
      f33 = []
      for _ in range(3):
          t = time.perf_counter(); fibo(33); f33.append(round(time.perf_counter() - t, 3))
      t = time.perf_counter(); fibo(10); f10 = round(time.perf_counter() - t, 6)
      print("INTERP_JSON " + json.dumps({"version": sys.version, "fibo33_s": f33, "fibo10_s": f10}))
"""


def interpreter_probe(runner="docker"):
    flow_id = "interpreter_probe" if runner == "docker" else "interpreter_probe_process"
    head = [f"id: {flow_id}", "namespace: bench", "tasks:", "  - id: probe",
            "    type: io.kestra.plugin.scripts.python.Script"]
    head.extend(runner_lines(runner))
    upsert_flow(flow_id, "\n".join(head) + "\n" + INTERP_SCRIPT)
    eid, _ = run_flow(flow_id)
    logs = api("GET", f"/api/v1/{TENANT}/logs/{eid}?minLevel=INFO")
    for entry in logs:
        msg = entry.get("message") or ""
        if "INTERP_JSON" in msg:
            return json.loads(msg.split("INTERP_JSON", 1)[1].strip())
    raise RuntimeError("interpreter probe produced no INTERP_JSON line")


RUNNER_LABEL = {
    "docker": "io.kestra.plugin.scripts.runner.docker.Docker "
              "(plugin default for io.kestra.plugin.scripts.python.Script; "
              "one python:3.13-slim container per task run)",
    "process": "process",
}

NOTES = {
    "docker": (
        "Task type io.kestra.plugin.scripts.python.Script with no taskRunner set, "
        "which Kestra 2.0.2 resolves to its plugin default "
        "io.kestra.plugin.scripts.runner.docker.Docker (verified: the flow is stored "
        "with that taskRunner materialised even though the submitted YAML omits it, "
        "and no pluginDefaults are configured). Every task run starts and tears down "
        "its own python:3.13-slim container; the image was pre-pulled, and container "
        "start/stop is included in the numbers and is the bulk of the per-task cost. "
        "started_at is stamped when the worker picks the task up, BEFORE the container "
        "is created, so completed_at - started_at includes container startup, not just "
        "Python execution."
    ),
    "process": (
        "Task type io.kestra.plugin.scripts.python.Script with taskRunner explicitly "
        "set to io.kestra.plugin.core.runner.Process (that is the class name in Kestra "
        "2.0.2; io.kestra.plugin.scripts.runner.process.Process does not exist in this "
        "version). This is NOT the default: omitting taskRunner yields the Docker "
        "runner. The script runs as a local subprocess of the worker, i.e. inside the "
        "kestra/kestra container, using that image's own CPython rather than "
        "python:3.13-slim, so the interpreter field differs from the Docker-runner run "
        "and raw compute is not directly comparable between the two."
    ),
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--steps", type=int, required=True)
    ap.add_argument("--fib", type=int, required=True)
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--warmup", type=int, default=1)
    ap.add_argument("--workload", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--interp", default="")
    ap.add_argument("--runner", choices=("docker", "process"), default="docker")
    args = ap.parse_args()

    version = api("GET", "/api/v1/configs")["version"]
    prefix = "bench" if args.runner == "docker" else "bench_proc"
    flow_id = f"{prefix}_{args.steps}_{args.fib}"
    upsert_flow(flow_id, flow_yaml(flow_id, args.steps, args.fib, args.runner))

    interp = json.load(open(args.interp)) if args.interp else None

    runs = []
    for i in range(args.warmup + args.reps):
        t_wall = time.time()
        eid, ex = run_flow(flow_id)
        t = timings(ex, args.steps)
        total = max(t["completed_at"])
        label = "warmup" if i < args.warmup else f"rep {i - args.warmup + 1}"
        print(f"  {label}: exec {eid}, {len(t['created_at'])} taskruns, "
              f"total {total:.3f}s (wall {time.time() - t_wall:.1f}s)", file=sys.stderr)
        if i >= args.warmup:
            runs.append(t)

    result = {
        "engine": "kestra",
        "version": version,
        "runner": RUNNER_LABEL[args.runner],
        "workload": args.workload,
        "runs": runs,
        "totals": [round(max(r["completed_at"]), 3) for r in runs],
    }
    if interp:
        result["interpreter"] = interp
    result["notes"] = NOTES[args.runner] + (
        " Timings read from Kestra's REST API: GET /api/v1/main/executions/{id}, "
        "field taskRunList[].state.histories. created_at = CREATED history entry "
        "(taskrun created/queued by the executor), started_at = RUNNING entry "
        "(worker began executing, i.e. after the task was dispatched and the Docker "
        "container was being set up), completed_at = terminal (SUCCESS) entry. "
        "No wall-clock prints inside the tasks. All values in seconds relative to "
        "the first taskrun's CREATED timestamp of that run. "
        "Kestra 2.0.2 OSS standalone (server standalone) on Postgres, from the "
        "official docker-compose.yml, all defaults; single node, worker started "
        "with 32 threads but the flow is strictly sequential so concurrency is 1. "
        "1 warmup run discarded, then 5 measured reps."
    )
    open(args.out, "w").write(json.dumps(result, indent=1))
    print(json.dumps({"totals": result["totals"]}))


if __name__ == "__main__":
    main()
