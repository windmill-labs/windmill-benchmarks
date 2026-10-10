#!/usr/bin/env python3
"""Measure, inside the process that actually runs a Kestra task, the Python
version and three fibo(31) timings - once per task runner."""
import sys, os, json, re
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kapi import upsert_flow, trigger, wait_exec, req, NS

BODY = '''import sys, time, platform, os
def fibo(n):
    if n <= 1:
        return n
    return fibo(n - 1) + fibo(n - 2)
ts = []
for _ in range(3):
    t = time.perf_counter(); fibo(31); ts.append(round(time.perf_counter() - t, 4))
t = time.perf_counter()
for _ in range(1000):
    fibo(10)
f10 = (time.perf_counter() - t) / 1000
print("PROBE " + __import__("json").dumps({
  "version": sys.version,
  "implementation": platform.python_implementation(),
  "executable": sys.executable,
  "fibo31_s": ts,
  "fibo10_s": f10,
  "cpu_count": os.cpu_count(),
  "sched_affinity": len(os.sched_getaffinity(0)),
}))
'''

def flow(fid, runner):
    p = [f"id: {fid}", f"namespace: {NS}", "tasks:",
         "  - id: probe",
         "    type: io.kestra.plugin.scripts.python.Script"]
    if runner == "docker":
        p.append("    containerImage: python:slim")
    else:
        p.append("    taskRunner:")
        p.append("      type: io.kestra.plugin.core.runner.Process")
    p.append("    script: |")
    p += ["      " + l if l.strip() else "" for l in BODY.strip("\n").split("\n")]
    return "\n".join(p) + "\n"

out = {}
for runner in ("process", "docker"):
    fid = f"rtprobe_{runner}"
    upsert_flow(fid, flow(fid, runner))
    eid = trigger(fid)
    ex = wait_exec(eid, timeout=600, poll=1.0)
    st = ex["state"]["current"]
    logs = req("GET", f"/logs/{eid}")
    msgs = [l.get("message") or "" for l in logs]
    hit = [m for m in msgs if m.startswith("PROBE ")]
    if st != "SUCCESS" or not hit:
        out[runner] = {"error": st, "logs": msgs[-10:]}
        continue
    d = json.loads(hit[0][6:])
    cmds = [m for m in msgs if "Starting command" in m or m.startswith("Command")]
    d["kestra_log_cmd_lines"] = cmds[:4]
    out[runner] = d
    print(runner, json.dumps(d)[:400], flush=True)

json.dump(out, open("/home/ubuntu/kbench/runtime_probe.json", "w"), indent=1)
print("wrote runtime_probe.json")
