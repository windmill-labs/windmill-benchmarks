#!/usr/bin/env python3
"""Airflow throughput harness: 2000 independent task instances, 20 concurrent.

Timings come from the Airflow metadata DB (task_instance), not harness wall-clock.
"""
import json
import os
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

BASE = "http://localhost:8080"
USER = os.environ.get("AF_USER", "airflow")
PWD = os.environ.get("AF_PASS", "airflow")
N = 2000
COMPOSE = ["docker", "compose", "-f", os.path.expanduser("~/af/docker-compose.yaml")]


def api(path, method="GET", body=None, tok=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json", "Authorization": "Bearer " + tok} if tok
        else {"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        raw = r.read()
    return json.loads(raw) if raw else {}


API_ERRORS = []


def api_retry(path, method="GET", body=None, tok=None, tries=8):
    last = None
    for i in range(tries):
        try:
            return api(path, method, body, tok)
        except Exception as e:
            last = e
            API_ERRORS.append("%s %s %s: %r" % (time.strftime("%H:%M:%S"), method, path, e))
            time.sleep(min(2 ** i, 15))
    raise last


def token():
    return api_retry("/auth/token", "POST", {"username": USER, "password": PWD})["access_token"]


def psql(sql):
    p = subprocess.run(
        COMPOSE + ["exec", "-T", "postgres", "psql", "-U", "airflow", "-At", "-F", "|", "-c", sql],
        capture_output=True, text=True, cwd=os.path.expanduser("~/af"))
    if p.returncode != 0:
        raise RuntimeError("psql failed: " + p.stderr[-2000:])
    return [l for l in p.stdout.strip().split("\n") if l]


def pct(xs, q):
    if not xs:
        return None
    s = sorted(xs)
    if len(s) == 1:
        return s[0]
    i = q * (len(s) - 1)
    lo, hi = int(i), min(int(i) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (i - lo)


def cpu_snapshot():
    with open("/proc/stat") as f:
        for line in f:
            if line.startswith("cpu "):
                v = [int(x) for x in line.split()[1:]]
                idle = v[3] + v[4]  # idle + iowait
                return sum(v), idle
    raise RuntimeError("no cpu line")


class CpuSampler(threading.Thread):
    """Samples host-wide CPU so we can check ~20 cores are busy for workload B."""

    def __init__(self):
        super().__init__(daemon=True)
        self.stop = threading.Event()
        self.samples = []  # (t, total, idle)

    def run(self):
        while not self.stop.is_set():
            try:
                tot, idle = cpu_snapshot()
                self.samples.append((time.time(), tot, idle))
            except Exception:
                pass
            self.stop.wait(1.0)

    def busy_cores(self, t0, t1):
        """Mean busy cores over [t0, t1] from the cumulative jiffy counters."""
        pts = [s for s in self.samples if t0 - 1.5 <= s[0] <= t1 + 1.5]
        if len(pts) < 2:
            return None
        ncpu = os.cpu_count()
        dtot = pts[-1][1] - pts[0][1]
        didle = pts[-1][2] - pts[0][2]
        if dtot <= 0:
            return None
        return round((dtot - didle) / dtot * ncpu, 2)


def trigger(dag_id, run_id, tok):
    return api_retry(f"/api/v2/dags/{dag_id}/dagRuns", "POST",
                     {"dag_run_id": run_id, "logical_date": None, "conf": {}}, tok)


def wait_run(dag_id, run_id, tok, timeout=5400):
    """Poll the metadata DB directly: the api-server's SQLAlchemy pool is a
    bottleneck under load in the stock compose and returns 500s."""
    t0 = time.time()
    last = 0
    while time.time() - t0 < timeout:
        try:
            rows = psql("select state from dag_run where dag_id='%s' and run_id='%s'" % (dag_id, run_id))
            st = rows[0] if rows else "?"
        except Exception as e:
            API_ERRORS.append("%s psql dag_run: %r" % (time.strftime("%H:%M:%S"), e))
            time.sleep(2)
            continue
        if st in ("success", "failed"):
            # wait for every task_instance to have an end_date written
            for _ in range(30):
                pend = psql("select count(*) from task_instance where dag_id='%s' and run_id='%s'"
                            " and map_index >= 0 and end_date is null" % (dag_id, run_id))
                if pend and pend[0] == "0":
                    break
                time.sleep(1)
            return st, tok
        if time.time() - last > 25:
            last = time.time()
            try:
                c = psql("select coalesce(state,'(none)'), count(*) from task_instance where dag_id='%s'"
                         " and run_id='%s' and map_index >= 0 group by 1 order by 1" % (dag_id, run_id))
                print("    [%5.0fs] run=%s ti: %s" % (time.time() - t0, st, " ".join(c)), flush=True)
            except Exception:
                pass
        time.sleep(1.0)
    raise TimeoutError("dag run did not finish in %ss" % timeout)


def fetch(dag_id, run_id):
    rows = psql(
        "select map_index,"
        " extract(epoch from scheduled_dttm), extract(epoch from queued_dttm),"
        " extract(epoch from start_date), extract(epoch from end_date), state, try_number"
        " from task_instance where dag_id='%s' and run_id='%s' and map_index >= 0" % (dag_id, run_id))
    out = []
    for r in rows:
        f = r.split("|")
        out.append({
            "map_index": int(f[0]),
            "sched": float(f[1]) if f[1] else None,
            "queued": float(f[2]) if f[2] else None,
            "start": float(f[3]) if f[3] else None,
            "end": float(f[4]) if f[4] else None,
            "state": f[5],
            "try": int(f[6]) if f[6] else None,
        })
    return out


def analyse(tis, sampler):
    ok = [t for t in tis if t["state"] == "success"]
    full = [t for t in ok if t["start"] and t["end"]]
    first_start = min(t["start"] for t in full)
    last_end = max(t["end"] for t in full)
    span = last_end - first_start

    lat = [t["end"] - t["queued"] for t in full if t["queued"]]
    lat_sched = [t["end"] - t["sched"] for t in full if t["sched"]]
    ex = [t["end"] - t["start"] for t in full]

    # peak concurrency by sweep line over (start, end)
    ev = []
    for t in full:
        ev.append((t["start"], 1))
        ev.append((t["end"], -1))
    ev.sort(key=lambda x: (x[0], -x[1]))
    cur = peak = 0
    series = []
    for ts, d in ev:
        cur += d
        peak = max(peak, cur)
        series.append((ts, cur))

    # steady-state window: from the first moment concurrency reaches the cap to
    # the last moment it is still at the cap.
    cap = 20
    at_cap = [ts for ts, c in series if c >= cap]
    steady = None
    if len(at_cap) >= 2:
        t1, t2 = at_cap[0], at_cap[-1]
        if t2 - t1 > 1.0:
            done = sum(1 for t in full if t1 <= t["end"] <= t2)
            steady = {"t1_rel": round(t1 - first_start, 3), "t2_rel": round(t2 - first_start, 3),
                      "window_s": round(t2 - t1, 3), "completed_in_window": done,
                      "throughput_per_s": round(done / (t2 - t1), 3)}

    # time-weighted mean concurrency over the span
    area = 0.0
    for i in range(len(series) - 1):
        area += series[i][1] * (series[i + 1][0] - series[i][0])
    mean_conc = area / span if span > 0 else None

    max_sched = max((t["sched"] for t in tis if t["sched"]), default=None)
    min_sched = min((t["sched"] for t in tis if t["sched"]), default=None)

    r = {
        "jobs": len(ok),
        "n_task_instances": len(tis),
        "n_success": len(ok),
        "n_not_success": len(tis) - len(ok),
        "span_s": round(span, 3),
        "throughput_per_s": round(len(ok) / span, 3),
        "latency_p50_s": round(pct(lat, 0.5), 4),
        "latency_p95_s": round(pct(lat, 0.95), 4),
        "exec_p50_s": round(pct(ex, 0.5), 4),
        "exec_p95_s": round(pct(ex, 0.95), 4),
        "latency_from_scheduled_p50_s": round(pct(lat_sched, 0.5), 4),
        "latency_from_scheduled_p95_s": round(pct(lat_sched, 0.95), 4),
        "peak_concurrency": peak,
        "mean_concurrency": round(mean_conc, 3) if mean_conc else None,
        "retries": sum(1 for t in tis if (t["try"] or 1) > 1),
        "steady_state": steady,
        "all_scheduled_before_first_start": bool(max_sched and max_sched <= first_start),
        "last_scheduled_minus_first_start_s": round(max_sched - first_start, 3) if max_sched else None,
        "ti_creation_spread_s": round(max_sched - min_sched, 3) if max_sched and min_sched else None,
        "mean_busy_cores_host": sampler.busy_cores(first_start, last_end) if sampler else None,
    }
    return r


def apiserver_pool_timeouts():
    p = subprocess.run(COMPOSE + ["logs", "airflow-apiserver", "--since", "60m"],
                       capture_output=True, text=True, cwd=os.path.expanduser("~/af"))
    return p.stdout.count("QueuePool limit of size")


def main():
    config = sys.argv[1]
    workload = sys.argv[2]
    reps = int(sys.argv[3]) if len(sys.argv) > 3 else 5
    dag_id = {"sleep": "tp_sleep", "cpu": "tp_cpu"}[workload]

    tok = token()
    api_retry(f"/api/v2/dags/{dag_id}", "PATCH", {"is_paused": False}, tok)
    time.sleep(2)

    results = []
    for rep in range(reps + 1):
        tag = "warmup" if rep == 0 else "rep%d" % rep
        run_id = "tp-%s-%s-%s-%d" % (config, workload, tag, int(time.time()))
        sampler = CpuSampler()
        sampler.start()
        t_trig = time.time()
        trigger(dag_id, run_id, tok)
        state, tok = wait_run(dag_id, run_id, tok)
        sampler.stop.set()
        time.sleep(1.5)
        tis = fetch(dag_id, run_id)
        r = analyse(tis, sampler)
        r["run_id"] = run_id
        r["dag_run_state"] = state
        r["harness_wall_s"] = round(time.time() - t_trig, 3)
        r["apiserver_db_pool_timeouts_cumulative"] = apiserver_pool_timeouts()
        print("  %s %s %s: state=%s success=%d/%d span=%.2fs thr=%.2f/s "
              "lat_p50=%.3f lat_p95=%.3f exec_p50=%.3f peak_conc=%d mean_conc=%.1f cores=%s"
              % (config, workload, tag, state, r["n_success"], r["n_task_instances"], r["span_s"],
                 r["throughput_per_s"], r["latency_p50_s"], r["latency_p95_s"], r["exec_p50_s"],
                 r["peak_concurrency"], r["mean_concurrency"] or 0, r["mean_busy_cores_host"]),
              flush=True)
        if rep > 0:
            results.append(r)
        else:
            r["discarded"] = True
            warm = r
        time.sleep(5)

    out = {
        "engine": "airflow",
        "config": config,
        "workload": workload,
        "concurrency": 20,
        "warmup_discarded": warm,
        "reps": results,
        "harness_api_errors": API_ERRORS,
    }
    path = os.path.expanduser("~/af/out/airflow_throughput_%s_%s.json" % (config, workload))
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(out, f, indent=2)
    print("wrote", path, flush=True)


if __name__ == "__main__":
    main()
