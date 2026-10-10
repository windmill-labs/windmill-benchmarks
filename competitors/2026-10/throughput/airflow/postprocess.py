#!/usr/bin/env python3
"""Re-derive every metric from the Airflow metadata DB for a finished series,
and cross-check CPU use against the independent host /proc/stat log.

Must run BEFORE the DB is wiped for the next configuration.
"""
import json, os, sys, subprocess

sys.path.insert(0, os.path.expanduser("~/af"))
from harness import psql, pct  # noqa

CAP = 20
CPULOG = os.path.expanduser("~/af/cpulog.tsv")


def cpu_samples():
    out = []
    if not os.path.exists(CPULOG):
        return out
    for line in open(CPULOG):
        f = line.split()
        if len(f) == 3:
            out.append((float(f[0]), int(f[1]), int(f[2])))
    return out


SAMPLES = cpu_samples()
NCPU = int(subprocess.run(["nproc"], capture_output=True, text=True).stdout.strip())


def busy_cores(t0, t1):
    pts = [s for s in SAMPLES if t0 <= s[0] <= t1]
    if len(pts) < 2:
        return None
    dtot = pts[-1][1] - pts[0][1]
    didle = pts[-1][2] - pts[0][2]
    if dtot <= 0:
        return None
    return round((dtot - didle) / dtot * NCPU, 2)


def fetch(dag_id, run_id):
    rows = psql(
        "select map_index, extract(epoch from scheduled_dttm), extract(epoch from queued_dttm),"
        " extract(epoch from start_date), extract(epoch from end_date), state, try_number"
        " from task_instance where dag_id='%s' and run_id='%s' and map_index >= 0" % (dag_id, run_id))
    out = []
    for r in rows:
        f = r.split("|")
        g = lambda i: float(f[i]) if f[i] else None
        out.append({"mi": int(f[0]), "sched": g(1), "queued": g(2), "start": g(3),
                    "end": g(4), "state": f[5], "try": int(f[6]) if f[6] else None})
    return out


def analyse(tis):
    ok = [t for t in tis if t["state"] == "success" and t["start"] and t["end"]]
    first_start = min(t["start"] for t in ok)
    last_end = max(t["end"] for t in ok)
    span = last_end - first_start

    lat = [t["end"] - t["queued"] for t in ok if t["queued"]]
    lat_s = [t["end"] - t["sched"] for t in ok if t["sched"]]
    ex = [t["end"] - t["start"] for t in ok]

    ev = []
    for t in ok:
        ev += [(t["start"], 1), (t["end"], -1)]
    ev.sort(key=lambda x: (x[0], -x[1]))
    cur = peak = 0
    series = []
    for ts, d in ev:
        cur += d
        peak = max(peak, cur)
        series.append((ts, cur))
    area = sum(series[i][1] * (series[i + 1][0] - series[i][0]) for i in range(len(series) - 1))
    mean_conc = area / span if span else None

    at_cap = [ts for ts, c in series if c >= CAP]
    steady = None
    if len(at_cap) >= 2 and at_cap[-1] - at_cap[0] > 1.0:
        t1, t2 = at_cap[0], at_cap[-1]
        done = sum(1 for t in ok if t1 <= t["end"] <= t2)
        in_win = [t for t in ok if t1 <= t["end"] <= t2]
        sub = [(ts, c) for ts, c in series if t1 <= ts <= t2]
        sarea = sum(sub[i][1] * (sub[i + 1][0] - sub[i][0]) for i in range(len(sub) - 1))
        steady = {
            "definition": "from the first instant concurrency reaches 20 to the last instant it is still 20",
            "window_s": round(t2 - t1, 3),
            "start_offset_s": round(t1 - first_start, 3),
            "completed_in_window": done,
            "throughput_per_s": round(done / (t2 - t1), 3),
            "mean_concurrency": round(sarea / (t2 - t1), 3) if t2 > t1 else None,
            "exec_p50_s": round(pct([t["end"] - t["start"] for t in in_win], 0.5), 4) if in_win else None,
            "mean_busy_cores_host": busy_cores(t1, t2),
        }

    max_sched = max((t["sched"] for t in tis if t["sched"]), default=None)
    min_sched = min((t["sched"] for t in tis if t["sched"]), default=None)
    return {
        "jobs": len(ok),
        "span_s": round(span, 3),
        "throughput_per_s": round(len(ok) / span, 3),
        "latency_p50_s": round(pct(lat, 0.5), 4),
        "latency_p95_s": round(pct(lat, 0.95), 4),
        "exec_p50_s": round(pct(ex, 0.5), 4),
        "exec_p95_s": round(pct(ex, 0.95), 4),
        "latency_from_scheduled_p50_s": round(pct(lat_s, 0.5), 4),
        "latency_from_scheduled_p95_s": round(pct(lat_s, 0.95), 4),
        "n_task_instances": len(tis),
        "n_success": len([t for t in tis if t["state"] == "success"]),
        "n_not_success": len([t for t in tis if t["state"] != "success"]),
        "states": {s: len([t for t in tis if t["state"] == s]) for s in sorted({t["state"] for t in tis})},
        "retries": sum(1 for t in tis if (t["try"] or 1) > 1),
        "peak_concurrency": peak,
        "mean_concurrency": round(mean_conc, 3) if mean_conc else None,
        "mean_busy_cores_host": busy_cores(first_start, last_end),
        "all_task_instances_ready_before_first_start": bool(max_sched and max_sched <= first_start),
        "ti_scheduled_spread_s": round(max_sched - min_sched, 3) if max_sched and min_sched else None,
        "last_scheduled_minus_first_start_s": round(max_sched - first_start, 3) if max_sched else None,
        "steady_state": steady,
    }


def main():
    config = sys.argv[1]
    for workload in ("sleep", "cpu"):
        src = os.path.expanduser("~/af/out/airflow_throughput_%s_%s.json" % (config, workload))
        if not os.path.exists(src):
            print("missing", src)
            continue
        d = json.load(open(src))
        dag_id = {"sleep": "tp_sleep", "cpu": "tp_cpu"}[workload]
        for key in ("warmup_discarded",):
            r = d.get(key)
            if r:
                a = analyse(fetch(dag_id, r["run_id"]))
                a["run_id"] = r["run_id"]
                a["dag_run_state"] = r["dag_run_state"]
                d[key] = a
        new = []
        for r in d["reps"]:
            a = analyse(fetch(dag_id, r["run_id"]))
            a["run_id"] = r["run_id"]
            a["dag_run_state"] = r["dag_run_state"]
            a["apiserver_db_pool_timeouts_cumulative"] = r.get("apiserver_db_pool_timeouts_cumulative")
            new.append(a)
        d["reps"] = new
        dst = os.path.expanduser("~/af/out/final_airflow_throughput_%s_%s.json" % (config, workload))
        json.dump(d, open(dst, "w"), indent=2)
        print("wrote", dst)
        for r in d["reps"]:
            print("  %s thr=%.2f/s span=%.1fs succ=%d/%d peak=%d mean_conc=%.1f cores=%s steady_thr=%s steady_cores=%s"
                  % (r["run_id"][-12:], r["throughput_per_s"], r["span_s"], r["n_success"],
                     r["n_task_instances"], r["peak_concurrency"], r["mean_concurrency"],
                     r["mean_busy_cores_host"],
                     r["steady_state"]["throughput_per_s"] if r["steady_state"] else None,
                     r["steady_state"]["mean_busy_cores_host"] if r["steady_state"] else None))


if __name__ == "__main__":
    main()
