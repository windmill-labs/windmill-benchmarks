#!/usr/bin/env python3
"""Kestra throughput harness.

Shape: `units / per_exec` executions of a flow whose single
io.kestra.plugin.core.flow.Parallel task holds `per_exec` independent Python
script tasks. All executions for a rep are submitted up front with a thread
pool, so every unit is enqueued before most of the work happens; the only
thing throttling the run is the worker thread pool (--worker-thread).

Timings come from each execution's taskRunList[].state.histories
(CREATED / RUNNING / SUCCESS), never from harness wall clock.
"""
import sys, os, json, time, argparse, uuid, concurrent.futures as cf
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kapi import upsert_flow, trigger, get_exec, parse_iso, req, NS, TERMINAL

SLEEP_BODY = """import time
time.sleep(0.1)
print("ok")
"""

CPU_BODY = """def fibo(n):
    if n <= 1:
        return n
    return fibo(n - 1) + fibo(n - 2)

print(fibo(31))
"""

BODIES = {"sleep": SLEEP_BODY, "cpu": CPU_BODY}


def ind(s, pad):
    return "\n".join(pad + l if l.strip() else "" for l in s.strip("\n").split("\n"))


def task_block(tid, workload, runner, pad):
    out = [f"{pad}- id: {tid}", f"{pad}  type: io.kestra.plugin.scripts.python.Script"]
    if runner == "docker":
        out.append(f"{pad}  containerImage: python:slim")
    else:
        out.append(f"{pad}  taskRunner:")
        out.append(f"{pad}    type: io.kestra.plugin.core.runner.Process")
    out.append(f"{pad}  script: |")
    out.append(ind(BODIES[workload], pad + "    "))
    return out


def gen_flow(flow_id, workload, runner, per_exec):
    p = ["id: " + flow_id, "namespace: " + NS, "tasks:",
         "  - id: fanout",
         "    type: io.kestra.plugin.core.flow.Parallel",
         "    tasks:"]
    for i in range(per_exec):
        p += task_block(f"u{i:03d}", workload, runner, "      ")
    return "\n".join(p) + "\n"



# ---------------------------------------------------------------- CPU sampling
import threading, glob

NCPU = os.cpu_count()


def _cpu_jiffies():
    with open("/proc/stat") as f:
        parts = f.readline().split()[1:]
    vals = [int(x) for x in parts]
    idle = vals[3] + vals[4]
    return sum(vals), idle


def _count_task_procs():
    n = 0
    for c in glob.glob("/proc/[0-9]*/comm"):
        try:
            with open(c) as f:
                nm = f.read().strip()
        except OSError:
            continue
        if nm.startswith("python3") or nm == "python":
            n += 1
    return n


class Sampler(threading.Thread):
    """Samples host-wide CPU busy-cores and the number of live python task
    processes, so workload B can be checked for actually using ~20 cores."""

    def __init__(self, interval=0.5):
        super().__init__(daemon=True)
        self.interval = interval
        self.stop_flag = threading.Event()
        self.cores = []
        self.procs = []

    def run(self):
        tot, idle = _cpu_jiffies()
        while not self.stop_flag.wait(self.interval):
            t2, i2 = _cpu_jiffies()
            dt, di = t2 - tot, i2 - idle
            if dt > 0:
                self.cores.append(round((1 - di / dt) * NCPU, 2))
            tot, idle = t2, i2
            self.procs.append(_count_task_procs())

    def stats(self):
        self.stop_flag.set()
        self.join(timeout=5)
        c = sorted(self.cores)
        p = sorted(self.procs)
        mid = lambda xs: xs[len(xs) // 2] if xs else None
        # drop the first and last sample of the window (ramp up / drain)
        inner_c = sorted(self.cores[1:-1]) or c
        return {"samples": len(self.cores),
                "busy_cores_p50": mid(inner_c),
                "busy_cores_max": max(c) if c else None,
                "python_procs_p50": mid(p[1:-1] or p),
                "python_procs_max": max(p) if p else None,
                "host_ncpu": NCPU}


def collect(ex):
    rows = []
    for tr in ex.get("taskRunList") or []:
        if tr["taskId"] == "fanout":
            continue
        h = {}
        for x in tr["state"]["histories"]:
            h.setdefault(x["state"], x["date"])
        if not all(k in h for k in ("CREATED", "RUNNING", "SUCCESS")):
            raise RuntimeError(f"task {tr['taskId']} missing states, has {list(h)}")
        rows.append((parse_iso(h["CREATED"]), parse_iso(h["RUNNING"]), parse_iso(h["SUCCESS"])))
    return rows


def pct(xs, q):
    xs = sorted(xs)
    k = (len(xs) - 1) * q
    lo, hi = int(k), min(int(k) + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def conc_stats(rows, step=0.1):
    ev = []
    for _, r, s in rows:
        ev.append((r, 1)); ev.append((s, -1))
    ev.sort(key=lambda e: (e[0], -e[1]))
    cur = peak = 0
    for _, d in ev:
        cur += d
        peak = max(peak, cur)
    # how long concurrency sat above 20
    t0 = min(r for _, r, _ in rows); t1 = max(s for _, _, s in rows)
    over = 0.0
    prev_t, cur = t0, 0
    for t, d in ev:
        if cur > 20:
            over += t - prev_t
        cur += d
        prev_t = t
    return peak, round(over, 3)


def analyse(rows, units):
    if len(rows) != units:
        raise RuntimeError(f"expected {units} unit task runs, got {len(rows)}")
    first_start = min(r for _, r, _ in rows)
    last_done = max(s for _, _, s in rows)
    span = last_done - first_start
    lat = [s - c for c, _, s in rows]
    exe = [s - r for _, r, s in rows]
    peak, over = conc_stats(rows)
    return {
        "jobs": units,
        "span_s": round(span, 3),
        "throughput_per_s": round(units / span, 3),
        "latency_p50_s": round(pct(lat, 0.5), 3),
        "latency_p95_s": round(pct(lat, 0.95), 3),
        "exec_p50_s": round(pct(exe, 0.5), 3),
        "exec_p95_s": round(pct(exe, 0.95), 3),
        "peak_concurrency": peak,
        "seconds_above_20_concurrent": over,
        "mean_concurrency": round(sum(exe) / span, 2),
    }


def run_rep(flow_id, n_exec, tag, timeout, submit_threads=24, poll=1.0):
    """Submit every execution of the rep up front, then poll each execution by id.

    The /executions/search endpoint silently ignores flowId/labels filters in
    2.0.2, so polling is done per execution id.
    """
    rid = tag + "-" + uuid.uuid4().hex[:8]
    t0 = time.time()
    with cf.ThreadPoolExecutor(submit_threads) as pool:
        eids = list(pool.map(lambda _: trigger(flow_id, labels=[("rep", rid)]), range(n_exec)))
    sub = time.time() - t0
    assert len(set(eids)) == n_exec, "duplicate execution ids"
    deadline = time.time() + timeout
    done = {}
    while len(done) < n_exec:
        pending = [e for e in eids if e not in done]
        with cf.ThreadPoolExecutor(16) as pool:
            for eid, ex in zip(pending, pool.map(get_exec, pending)):
                if ex["state"]["current"] in TERMINAL:
                    done[eid] = ex
        if len(done) < n_exec:
            if time.time() > deadline:
                raise RuntimeError(f"{tag}: timeout, {len(done)}/{n_exec} terminal")
            time.sleep(poll)
    bad = [(e, x["state"]["current"]) for e, x in done.items() if x["state"]["current"] != "SUCCESS"]
    if bad:
        raise RuntimeError(f"{tag}: {len(bad)} executions not SUCCESS: {bad[:5]}")
    rows, size = [], 0
    for ex in done.values():
        rows += collect(ex)
        size += len(json.dumps(ex))
    return rows, {"submit_s": round(sub, 3), "n_exec": n_exec,
                  "exec_json_bytes_total": size, "rep_label": rid}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workload", choices=["sleep", "cpu"], required=True)
    ap.add_argument("--runner", choices=["process", "docker"], required=True)
    ap.add_argument("--units", type=int, default=2000)
    ap.add_argument("--per-exec", type=int, default=100)
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--warmup", type=int, default=1)
    ap.add_argument("--timeout", type=int, default=3600)
    ap.add_argument("--tag", default="")
    ap.add_argument("--out", default=None)
    ap.add_argument("--dump-rows", default=None)
    a = ap.parse_args()

    assert a.units % a.per_exec == 0, "units must be a multiple of per-exec"
    flow_id = f"tp_{a.runner}_{a.workload}_{a.per_exec}{a.tag}"
    yml = gen_flow(flow_id, a.workload, a.runner, a.per_exec)
    open(f"/home/ubuntu/kbench/{flow_id}.yml", "w").write(yml)
    upsert_flow(flow_id, yml)
    n_exec = a.units // a.per_exec
    print(f"[{flow_id}] upserted; {n_exec} executions x {a.per_exec} units = {a.units}", flush=True)

    results, all_rows = [], []
    for i in range(a.warmup + a.reps):
        tag = "warmup" if i < a.warmup else f"rep{i - a.warmup + 1}"
        t0 = time.time()
        samp = Sampler()
        samp.start()
        rows, meta = run_rep(flow_id, n_exec, tag, a.timeout)
        wall = time.time() - t0
        meta["cpu"] = samp.stats()
        r = analyse(rows, a.units)
        r["wall_s"] = round(wall, 3)
        r.update(meta)
        print(f"[{flow_id}] {tag} tp={r['throughput_per_s']:.2f}/s span={r['span_s']:.2f}s "
              f"lat_p50={r['latency_p50_s']:.3f} lat_p95={r['latency_p95_s']:.3f} "
              f"exec_p50={r['exec_p50_s']:.3f} peak={r['peak_concurrency']} "
              f"over20={r['seconds_above_20_concurrent']}s mean_conc={r['mean_concurrency']} "
              f"submit={r['submit_s']}s wall={wall:.1f}s "
              f"cores_p50={r['cpu']['busy_cores_p50']} cores_max={r['cpu']['busy_cores_max']} "
              f"pyprocs_p50={r['cpu']['python_procs_p50']}", flush=True)
        if i >= a.warmup:
            results.append(r)
            all_rows.append(rows)
    if a.out:
        json.dump(results, open(a.out, "w"), indent=1)
        print(f"[{flow_id}] wrote {a.out}", flush=True)
    if a.dump_rows:
        out = []
        for rows in all_rows:
            base = min(r for _, r, _ in rows)
            out.append([[round(c - base, 3), round(r - base, 3), round(s - base, 3)] for c, r, s in rows])
        json.dump(out, open(a.dump_rows, "w"))
        print(f"[{flow_id}] wrote {a.dump_rows}", flush=True)


if __name__ == "__main__":
    main()
