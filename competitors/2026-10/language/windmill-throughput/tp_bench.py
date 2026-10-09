#!/usr/bin/env python3
"""Windmill throughput benchmark: N independent jobs, 20 concurrent workers.

Jobs are enqueued with scheduled_in_secs so the whole batch is in the queue
before any of it becomes runnable; throughput is then measured from the first
job that starts to the last that completes, which excludes submission.
"""
import argparse, json, os, statistics, subprocess, sys, time
from datetime import datetime, timezone, timedelta
import urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor

BASE = "http://localhost"; WS = "admins"
TOKEN = open(os.environ.get("WM_TOKEN_FILE", "wm_token")).read().strip()
DB = os.environ.get("DB_CONTAINER", "ubuntu-db-1")

BODIES = {
 "sleep_bun": "export async function main() {\n\tawait new Promise((r) => setTimeout(r, 100));\n}\n",
 "sleep": "import time\ndef main():\n    time.sleep(0.1)\n",
 "cpu":   "def main():\n    def fibo(n):\n        return n if n <= 1 else fibo(n-1) + fibo(n-2)\n    return fibo(31)\n",
}

def api(method, path, body=None, raw=False):
    req = urllib.request.Request(BASE+path, method=method,
        headers={"Authorization":"Bearer "+TOKEN, "Content-Type":"application/json"})
    data = json.dumps(body).encode() if body is not None else None
    with urllib.request.urlopen(req, data, timeout=120) as r:
        t = r.read().decode()
    if raw: return t
    try: return json.loads(t)
    except json.JSONDecodeError: return t

def psql(sql):
    return subprocess.run(["sudo","docker","exec",DB,"psql","-U","postgres","-d","windmill",
                           "-t","-A","-F","|","-c",sql], capture_output=True, text=True, check=True).stdout.strip()

def ensure_script(path, body, dedicated, lang="python3"):
    payload = {"path": path, "summary": "", "description": "", "content": body,
               "language": lang, "schema": {"type":"object","properties":{},"required":[]},
               "dedicated_worker": dedicated}
    try:
        api("POST", f"/api/w/{WS}/scripts/create", payload, raw=True)
    except urllib.error.HTTPError as e:
        if e.code not in (400, 409): raise
        prev = api("GET", f"/api/w/{WS}/scripts/get/p/{path}")
        payload["parent_hash"] = prev["hash"]
        api("POST", f"/api/w/{WS}/scripts/create", payload, raw=True)
    for _ in range(60):
        try:
            if api("GET", f"/api/w/{WS}/scripts/get/p/{path}"): return
        except urllib.error.HTTPError: pass
        time.sleep(1)
    raise RuntimeError("script never became visible")

def submit(path, n, when_iso):
    import urllib.parse
    q = urllib.parse.quote(when_iso, safe="")
    def one(_):
        return api("POST", f"/api/w/{WS}/jobs/run/p/{path}?scheduled_for={q}", {}, raw=True).strip().strip('"')
    with ThreadPoolExecutor(max_workers=48) as ex:
        return list(ex.map(one, range(n)))

def wait_done(path, marker, n, timeout=1800):
    deadline = time.time()+timeout
    while time.time() < deadline:
        c = int(psql(f"select count(*) from v2_as_completed_job where script_path='{path}' and created_at >= '{marker}'") or 0)
        if c >= n: return
        time.sleep(2)
    raise TimeoutError(f"only {c}/{n} finished")

def collect(path, marker):
    rows = psql("select extract(epoch from created_at), extract(epoch from started_at), "
                f"coalesce(duration_ms,0) from v2_as_completed_job where script_path='{path}' "
                f"and created_at >= '{marker}' and success order by started_at")
    out=[]
    for r in rows.splitlines():
        if not r.strip(): continue
        c,s,d = r.split("|"); out.append((float(c), float(s), float(s)+float(d)/1000.0))
    return out

def peak_concurrency(iv):
    ev=[]
    for _,s,e in iv: ev.append((s,1)); ev.append((e,-1))
    ev.sort(); cur=mx=0
    for _,d in ev:
        cur+=d; mx=max(mx,cur)
    return mx

def pct(v,q):
    v=sorted(v); k=(len(v)-1)*q; f=int(k)
    return v[f] if f+1>=len(v) else v[f]+(v[f+1]-v[f])*(k-f)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--workload", required=True, choices=["sleep","cpu"])
    ap.add_argument("--jobs", type=int, default=2000)
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--warmup", type=int, default=1)
    ap.add_argument("--dedicated", action="store_true")
    ap.add_argument("--lang", default="python3", choices=["python3", "bun"])
    ap.add_argument("--delay", type=int, default=45)
    ap.add_argument("--out", required=True)
    a=ap.parse_args()

    key = a.workload + ("_bun" if a.lang == "bun" else "")
    suffix = "ts" if a.lang == "bun" else "py"
    path=f"u/admin/tp_{suffix}_{a.workload}"
    ensure_script(path, BODIES[key], a.dedicated, a.lang)

    reps=[]
    for i in range(a.warmup+a.reps):
        marker=psql("select now()")
        time.sleep(1)
        # absolute release instant: every job in the batch unlocks together
        from datetime import datetime, timezone, timedelta
        rel_dt = datetime.now(timezone.utc) + timedelta(seconds=a.delay)
        release = rel_dt.isoformat().replace("+00:00", "Z")
        release_epoch = rel_dt.timestamp()
        t0=time.time(); submit(path, a.jobs, release)
        sub=time.time()-t0
        if time.time() > release_epoch:
            raise RuntimeError(f"submission of {a.jobs} took {sub:.1f}s, past the {a.delay}s release; raise --delay")
        wait_done(path, marker, a.jobs)
        iv=collect(path, marker)
        if len(iv)<a.jobs: raise RuntimeError(f"collected {len(iv)} of {a.jobs}")
        span=max(e for _,_,e in iv)-min(s for _,s,_ in iv)
        lat=[e-release_epoch for _,_,e in iv]; ex=[e-s for _,s,e in iv]; qw=[s-release_epoch for _,s,_ in iv]
        rec={"jobs":len(iv), "submit_s":round(sub,2), "span_s":round(span,3),
             "throughput_per_s":round(len(iv)/span,1),
             "latency_p50_s":round(pct(lat,.5),3), "latency_p95_s":round(pct(lat,.95),3),
             "exec_p50_s":round(pct(ex,.5),4), "queue_wait_p50_s":round(pct(qw,.5),3),
             "peak_concurrency":peak_concurrency(iv),
             "avg_concurrency":round(sum(ex)/span,2)}
        lbl="warmup" if i<a.warmup else f"rep {i-a.warmup+1}"
        print(f"  {lbl}: {rec['throughput_per_s']}/s span={rec['span_s']}s avg_conc={rec['avg_concurrency']} peak_conc={rec['peak_concurrency']} exec_p50={rec['exec_p50_s']}s", file=sys.stderr, flush=True)
        if i>=a.warmup: reps.append(rec)

    res={"engine":"windmill","config":"dedicated" if a.dedicated else "stock",
         "language":"typescript" if a.lang=="bun" else "python",
         "workload":a.workload,"concurrency":20,
         "concurrency_mechanism":"20 dedicated worker processes" if a.dedicated else "20 worker processes, one job each",
         "is_default":False,"reps":reps,
         "median_throughput_per_s":round(statistics.median(r["throughput_per_s"] for r in reps),1)}
    open(a.out,"w").write(json.dumps(res,indent=1))
    print(json.dumps({k:res[k] for k in ("config","workload","median_throughput_per_s")}))

if __name__=="__main__": main()
