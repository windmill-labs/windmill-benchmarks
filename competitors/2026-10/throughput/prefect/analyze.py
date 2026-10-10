#!/usr/bin/env python3
"""Turn Prefect's state tables into throughput figures.

Timings come from task_run_state / flow_run_state, written by the Prefect
server, never from the harness clock. Two anchors need explaining:

  * task_run.created is the time the server's task-run recorder inserted
    the row, which lags the run by tens of seconds, so it is unusable as a
    submission anchor. For in-process tasks the submission time is instead
    read out of the task run id, which is a uuid7 minted by
    ThreadPoolTaskRunner.submit() at the moment of submission.
  * flow_run.created is written synchronously by the API call that creates
    the deployment run, so for the work-pool configuration it is the real
    submission time and is used as-is.
"""
import csv
import glob
import json
import os
import statistics
import sys

S = os.path.dirname(os.path.abspath(__file__))
USER_HZ = 100.0
NCPU = 32
UNITS = 2000
FIBO31 = 0.1111
CEIL = {"sleep": 20 / 0.1, "cpu": 20 / FIBO31}

CONFIGS = {"ip_": "in_process", "wp_": "work_pool", "wt_": "work_pool_tuned"}


def uuid7_epoch(u):
    return int(u.replace("-", "")[:12], 16) / 1000.0


def pct(xs, q):
    xs = sorted(xs)
    if not xs:
        return None
    k = max(0, min(len(xs) - 1, int(round(q * (len(xs) - 1)))))
    return xs[k]


def f(v):
    return float(v) if v not in (None, "") else None


def load(path, submit_from_uuid):
    out = {}
    with open(path) as fh:
        for row in csv.DictReader(fh):
            tag = row["tag"]
            if not tag:
                continue
            sub = uuid7_epoch(row["id"]) if submit_from_uuid else f(row["rec_created"])
            out.setdefault(tag, []).append({
                "submit": sub,
                "rec_created": f(row["rec_created"]),
                "expected": f(row["expected_start"]),
                "pending": f(row["pending"]),
                "running": f(row["running"]),
                "completed": f(row["completed"]),
                "state": row["state_type"],
            })
    return out


def load_cpu(path):
    s = []
    with open(path) as fh:
        for line in fh:
            p = line.split()
            if len(p) < 12 or p[1] != "cpu":
                continue
            v = [int(x) for x in p[2:12]]
            s.append((float(p[0]), sum(v) - v[3] - v[4]))
    s.sort()
    return s


def cores_busy(samples, t0, t1):
    inside = [x for x in samples if t0 <= x[0] <= t1]
    if len(inside) < 3:
        return None
    a, b = inside[0], inside[-1]
    return round((b[1] - a[1]) / USER_HZ / (b[0] - a[0]), 2)


def peak_conc(units):
    ev = []
    for u in units:
        if u["running"] and u["completed"]:
            ev += [(u["running"], 1), (u["completed"], -1)]
    ev.sort(key=lambda x: (x[0], -x[1]))
    cur = peak = 0
    for _t, d in ev:
        cur += d
        peak = max(peak, cur)
    return peak


def rep_stats(units, cpu):
    done = [u for u in units if u["state"] == "COMPLETED" and u["running"] and u["completed"]]
    t0 = min(u["running"] for u in done)
    t1 = max(u["completed"] for u in done)
    span = t1 - t0
    lat = [u["completed"] - u["submit"] for u in done]
    ex = [u["completed"] - u["running"] for u in done]
    subs = [u["submit"] for u in done]
    return {
        "jobs": len(done),
        "units_seen": len(units),
        "not_completed": len(units) - len(done),
        "span_s": round(span, 3),
        "throughput_per_s": round(len(done) / span, 2),
        "latency_p50_s": round(pct(lat, 0.50), 3),
        "latency_p95_s": round(pct(lat, 0.95), 3),
        "exec_p50_s": round(pct(ex, 0.50), 4),
        "exec_p95_s": round(pct(ex, 0.95), 4),
        "peak_concurrency": peak_conc(units),
        "submit_spread_s": round(max(subs) - min(subs), 3),
        "submit_to_firststart_s": round(t0 - min(subs), 3),
        "host_cores_busy": cores_busy(cpu, t0, t1),
        "_win": [t0, t1],
    }


def main():
    allu = load(f"{S}/task_units.csv", True)
    allu.update(load(f"{S}/flow_units.csv", False))
    cpu = load_cpu(f"{S}/cpu_samples.txt")
    harness = {}
    for p in glob.glob(f"{S}/out/*.json"):
        d = json.load(open(p))
        harness[d["tag"]] = d

    groups = {}
    for tag, units in allu.items():
        pre = tag[:3]
        if pre not in CONFIGS:
            continue
        wl, rep = tag[3:].rsplit("_r", 1)
        groups.setdefault((CONFIGS[pre], wl), {})[int(rep)] = (tag, units)

    report = {}
    for (cfg, wl), reps in sorted(groups.items()):
        rows = []
        for r in sorted(reps):
            tag, units = reps[r]
            try:
                st = rep_stats(units, cpu)
            except ValueError:
                st = {"jobs": 0, "units_seen": len(units), "error": "no completed units"}
            st["rep"], st["tag"] = r, tag
            h = harness.get(tag, {})
            for k in ("submit_s", "create_s", "flow_proc_cpu_s", "cores_busy_flow_proc",
                      "wall_s", "completed", "terminal", "ok"):
                if k in h:
                    st["h_" + k] = h[k]
            rows.append(st)
        report[f"{cfg}|{wl}"] = rows
    json.dump(report, open(f"{S}/reps_raw.json", "w"), indent=1)

    print(f"{'config':18}{'wl':6}{'reps':5}{'jobs':16}{'thr_med':>9}{'range':>18}"
          f"{'%ceil':>7}{'span':>9}{'lat50':>8}{'lat95':>8}{'exec50':>8}{'peak':>6}  hostcores")
    for key, rows in sorted(report.items()):
        cfg, wl = key.split("|")
        m = [r for r in rows if r["rep"] > 0 and r.get("jobs")]
        if not m:
            print(f"{cfg:18}{wl:6} no measured reps yet ({len(rows)} rows)")
            continue
        th = [r["throughput_per_s"] for r in m]
        med = statistics.median(th)
        print(f"{cfg:18}{wl:6}{len(m):<5}{str(sorted({r['jobs'] for r in m})):16}"
              f"{med:9.2f}{f'{min(th):.2f}-{max(th):.2f}':>18}"
              f"{100*med/CEIL[wl]:6.1f}%{statistics.median([r['span_s'] for r in m]):9.1f}"
              f"{statistics.median([r['latency_p50_s'] for r in m]):8.2f}"
              f"{statistics.median([r['latency_p95_s'] for r in m]):8.2f}"
              f"{statistics.median([r['exec_p50_s'] for r in m]):8.3f}"
              f"{max(r['peak_concurrency'] for r in m):6}"
              f"  {[r['host_cores_busy'] for r in m]}")


if __name__ == "__main__":
    main()
