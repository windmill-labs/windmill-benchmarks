#!/usr/bin/env python3
"""Kestra per-task cost harness.

Builds a flow of N explicit sequential script tasks, runs it, and reads
timings out of the execution's taskRunList[].state.histories.
"""
import json, sys, time, urllib.request, urllib.error, base64, os, datetime, uuid, argparse

BASE = "http://localhost:8080/api/v1/main"
AUTH = base64.b64encode(b"admin@kestra.io:Admin1234!").decode()
NS = "company.team"

def req(method, path, data=None, ctype=None, raw=False):
    url = BASE + path
    r = urllib.request.Request(url, method=method, data=data)
    r.add_header("Authorization", "Basic " + AUTH)
    if ctype:
        r.add_header("Content-Type", ctype)
    try:
        with urllib.request.urlopen(r, timeout=120) as resp:
            body = resp.read()
    except urllib.error.HTTPError as e:
        body = e.read()
        raise RuntimeError(f"{method} {path} -> {e.code}: {body[:800].decode(errors='replace')}")
    if raw:
        return body
    return json.loads(body) if body else None

PY_SCRIPT = """
def fibo(n: int):
  if n <= 1:
      return n
  else:
      return fibo(n - 1) + fibo(n - 2)

print(str(fibo(%d)))
"""

JS_SCRIPT = """function fibo(n) {
  if (n <= 1) {
    return n
  } else {
    return fibo(n-1) + fibo(n-2)
  }
}
console.log(fibo(%d))
"""

def indent(s, pad):
    return "\n".join(pad + l if l.strip() else "" for l in s.strip("\n").split("\n"))

def gen_flow(flow_id, lang, runner, n, count, js_image=None):
    if lang == "py":
        ttype = "io.kestra.plugin.scripts.python.Script"
        image = "    containerImage: python:slim\n"
        script = PY_SCRIPT % n
    else:
        ttype = "io.kestra.plugin.scripts.node.Script"
        image = ("    containerImage: %s\n" % js_image) if js_image else ""
        script = JS_SCRIPT % n
    runner_block = ""
    if runner == "process":
        runner_block = "    taskRunner:\n      type: io.kestra.plugin.core.runner.Process\n"
    parts = [f"id: {flow_id}", f"namespace: {NS}", "tasks:"]
    for i in range(count):
        parts.append(f"  - id: t_{i:03d}")
        parts.append(f"    type: {ttype}")
        if image:
            parts.append(image.rstrip("\n"))
        if runner_block:
            parts.append(runner_block.rstrip("\n"))
        parts.append("    script: |")
        parts.append(indent(script, "      "))
    return "\n".join(parts) + "\n"

def upsert_flow(flow_id, yaml_text):
    try:
        req("POST", "/flows", yaml_text.encode(), "application/x-yaml")
    except RuntimeError as e:
        if "already exist" in str(e) or "409" in str(e) or "422" in str(e):
            req("PUT", f"/flows/{NS}/{flow_id}", yaml_text.encode(), "application/x-yaml")
        else:
            raise

def trigger(flow_id):
    boundary = "----b" + uuid.uuid4().hex
    body = (f"--{boundary}--\r\n").encode()
    ex = req("POST", f"/executions/{NS}/{flow_id}?wait=false", body,
             f"multipart/form-data; boundary={boundary}")
    return ex["id"]

def wait(exec_id, timeout=3600):
    t0 = time.time()
    while True:
        ex = req("GET", f"/executions/{exec_id}")
        st = ex["state"]["current"]
        if st in ("SUCCESS", "FAILED", "KILLED", "WARNING", "CANCELLED"):
            return ex
        if time.time() - t0 > timeout:
            raise RuntimeError(f"timeout waiting for {exec_id} (state {st})")
        time.sleep(0.5)

def parse_iso(s):
    s = s.replace("Z", "+00:00")
    return datetime.datetime.fromisoformat(s).timestamp()

def extract(ex, count):
    trs = [tr for tr in ex["taskRunList"] if tr["taskId"].startswith("t_")]
    if len(trs) != count:
        raise RuntimeError(f"expected {count} task runs, got {len(trs)} (state {ex['state']['current']})")
    trs.sort(key=lambda tr: int(tr["taskId"][2:]))
    created, started, completed = [], [], []
    for tr in trs:
        h = {x["state"]: x["date"] for x in tr["state"]["histories"]}
        for k in ("CREATED", "RUNNING", "SUCCESS"):
            if k not in h:
                raise RuntimeError(f"task {tr['taskId']} missing {k} in histories: {tr['state']['histories']}")
        created.append(parse_iso(h["CREATED"]))
        started.append(parse_iso(h["RUNNING"]))
        completed.append(parse_iso(h["SUCCESS"]))
    base = created[0]
    rnd = lambda xs: [round(x - base, 3) for x in xs]
    return {"workers": 1, "created_at": rnd(created), "started_at": rnd(started), "completed_at": rnd(completed)}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lang", required=True, choices=["py", "js"])
    ap.add_argument("--runner", required=True, choices=["docker", "process"])
    ap.add_argument("--n", type=int, required=True)
    ap.add_argument("--count", type=int, required=True)
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--warmup", type=int, default=1)
    ap.add_argument("--out", required=True)
    ap.add_argument("--workload", required=True)
    ap.add_argument("--js-image", default=None)
    ap.add_argument("--suffix", default="")
    a = ap.parse_args()

    flow_id = f"bench_{a.lang}_{a.runner}_{a.workload}{a.suffix}".replace("-", "_")
    yaml_text = gen_flow(flow_id, a.lang, a.runner, a.n, a.count, a.js_image)
    open(f"/tmp/{flow_id}.yml", "w").write(yaml_text)
    upsert_flow(flow_id, yaml_text)
    print(f"[{flow_id}] flow upserted ({a.count} tasks, n={a.n})", flush=True)

    reps = []
    for i in range(a.warmup + a.reps):
        tag = "warmup" if i < a.warmup else f"rep{i - a.warmup + 1}"
        eid = trigger(flow_id)
        ex = wait(eid)
        st = ex["state"]["current"]
        dur = ex["state"].get("duration")
        print(f"[{flow_id}] {tag} exec={eid} state={st} dur={dur}", flush=True)
        if st != "SUCCESS":
            raise RuntimeError(f"{tag} execution {eid} ended {st}")
        r = extract(ex, a.count)
        if i >= a.warmup:
            reps.append(r)
        total = r["completed_at"][-1]
        print(f"[{flow_id}] {tag} total={total:.3f}s per_task={total/a.count*1000:.1f}ms", flush=True)
    out = {"lang": "python" if a.lang == "py" else "js",
           "runner": a.runner, "workload": a.workload, "reps": reps}
    json.dump(out, open(a.out, "w"))
    print(f"[{flow_id}] wrote {a.out}", flush=True)

if __name__ == "__main__":
    main()
