#!/usr/bin/env python3
"""Write the per-combination result files in the requested schema."""
import json
import os
import statistics

S = os.path.dirname(os.path.abspath(__file__))
FIBO31 = 0.1111
CEIL = {"sleep": 20 / 0.1, "cpu": 20 / FIBO31}

MECH = {
    "in_process": (
        "ThreadPoolTaskRunner(max_workers=20); 2000 @task .submit() calls inside "
        "one flow run. Tasks execute as threads of the flow's own process; nothing "
        "is queued server-side and no task is dispatched to a worker."
    ),
    "work_pool": (
        "Process work pool 'bench' with concurrency limit 20; 2000 deployment runs "
        "of the 'unit' flow. The worker claims scheduled runs on its poll loop and "
        "executes each as its own subprocess."
    ),
    "work_pool_tuned": (
        "Process work pool 'bench' with concurrency limit 20, worker poll loop at "
        "PREFECT_WORKER_QUERY_SECONDS=0.1; 2000 deployment runs of the 'unit' flow, "
        "each executed as its own subprocess."
    ),
}

IS_DEFAULT = {"in_process": True, "work_pool": True, "work_pool_tuned": False}

# Batch size per combination. The poll-bound work-pool configurations run a
# stable rate, so they are measured on smaller batches; the figures are
# units/second in every case and must not be read as equal-sized runs.
BATCH = {
    ("in_process", "sleep"): 500,
    ("in_process", "cpu"): 2000,
    ("work_pool", "sleep"): 200,
    ("work_pool", "cpu"): 200,
    ("work_pool_tuned", "sleep"): 400,
    ("work_pool_tuned", "cpu"): 400,
}
BATCH_WHY = {
    ("in_process", "sleep"): "500 units per rep. At 2000 units per rep this "
        "configuration outruns the Prefect server's event broker and the "
        "task-run recorder silently loses about half the task runs, so the "
        "database cannot account for every unit; 500 stays under that ceiling "
        "and all 6 reps recorded 500/500.",
    ("in_process", "cpu"): "2000 units per rep, the original batch size. This "
        "configuration runs at 8.4 units/s, far below the recorder's drain "
        "rate, and all 6 reps recorded 2000/2000.",
    ("work_pool", "sleep"): "200 units per rep. Throughput here is a stable "
        "poll-bound rate of one pool-worth per worker query interval, so a "
        "smaller batch measures the same rate in a tenth of the time.",
    ("work_pool", "cpu"): "200 units per rep, same reason.",
    ("work_pool_tuned", "sleep"): "400 units per rep. Throughput here is a "
        "stable process-startup-bound rate.",
    ("work_pool_tuned", "cpu"): "400 units per rep, same reason.",
}

NOTES = {
    "in_process": (
        "Prefect's default execution path for @task. All 2000 futures are submitted "
        "before any result is awaited; submitting them takes under 0.2s. "
        "ThreadPoolTaskRunner is the default task runner, but its default max_workers "
        "is unbounded (sys.maxsize), so the 20-way cap is imposed by this experiment. "
        "task_run.created is written by the server's task-run recorder tens of seconds "
        "after the fact, so the per-unit submission anchor is the uuid7 that "
        "ThreadPoolTaskRunner.submit() mints at submission time; latency is "
        "that timestamp to the COMPLETED transition."
    ),
    "work_pool": (
        "Stock worker poll loop: PREFECT_WORKER_QUERY_SECONDS left at its default of "
        "10s, PREFECT_WORKER_PREFETCH_SECONDS at 10s. run_deployment is not used at "
        "all: all 2000 runs are created up front through the API client "
        "(create_flow_run_from_deployment, 100 concurrent), so run_deployment's 5s "
        "poll_interval plays no part and creation is roughly an order of magnitude "
        "faster than the worker consumes. flow_run.created is written synchronously "
        "by that API call, so it is the real submission time."
    ),
    "work_pool_tuned": (
        "Same as work_pool but with PREFECT_WORKER_QUERY_SECONDS=0.1 instead of the "
        "default 10s, which is the single change. run_deployment is still not used, "
        "so its 5s poll_interval default is not in play."
    ),
}


def main():
    raw = json.load(open(f"{S}/reps_raw.json"))
    index = {}
    for key, rows in raw.items():
        cfg, wl = key.split("|")
        measured = [r for r in rows if r["rep"] > 0 and r.get("jobs")]
        warmup = [r for r in rows if r["rep"] == 0]
        reps = [{
            "jobs": r["jobs"],
            "span_s": r["span_s"],
            "throughput_per_s": r["throughput_per_s"],
            "latency_p50_s": r["latency_p50_s"],
            "latency_p95_s": r["latency_p95_s"],
            "exec_p50_s": r["exec_p50_s"],
            "exec_p95_s": r["exec_p95_s"],
            "peak_concurrency": r["peak_concurrency"],
            "submit_spread_s": r["submit_spread_s"],
            "host_cores_busy": r["host_cores_busy"],
            "units_seen": r["units_seen"],
            "not_completed": r["not_completed"],
        } for r in measured]
        th = [r["throughput_per_s"] for r in measured]
        doc = {
            "engine": "prefect",
            "config": cfg,
            "workload": wl,
            "units_per_rep": BATCH[(cfg, wl)],
            "batch_size_note": BATCH_WHY[(cfg, wl)],
            "reps_measured": len(reps),
            "reps_warmup_discarded": len(warmup),
            "concurrency": 20,
            "concurrency_mechanism": MECH[cfg],
            "is_default": IS_DEFAULT[cfg],
            "notes": NOTES[cfg],
            "reps": reps,
            "warmup_discarded": [{"jobs": w.get("jobs"), "span_s": w.get("span_s"),
                                  "throughput_per_s": w.get("throughput_per_s")} for w in warmup],
            "summary": {
                "units_per_rep": BATCH[(cfg, wl)],
                "throughput_median_per_s": round(statistics.median(th), 2),
                "throughput_min_per_s": min(th),
                "throughput_max_per_s": max(th),
                "throughput_stdev_per_s": round(statistics.stdev(th), 3) if len(th) > 1 else 0.0,
                "theoretical_ceiling_per_s": round(CEIL[wl], 1),
                "pct_of_ceiling": round(100 * statistics.median(th) / CEIL[wl], 2),
                "latency_p50_median_s": round(statistics.median([r["latency_p50_s"] for r in measured]), 3),
                "latency_p95_median_s": round(statistics.median([r["latency_p95_s"] for r in measured]), 3),
                "exec_p50_median_s": round(statistics.median([r["exec_p50_s"] for r in measured]), 4),
                "peak_concurrency_max": max(r["peak_concurrency"] for r in measured),
                "host_cores_busy_median": round(statistics.median(
                    [r["host_cores_busy"] for r in measured if r["host_cores_busy"]]), 2),
            },
        }
        if cfg == "in_process":
            cb = [r.get("h_cores_busy_flow_proc") for r in measured if r.get("h_cores_busy_flow_proc")]
            if cb:
                doc["summary"]["flow_process_cores_busy_median"] = round(statistics.median(cb), 3)
                doc["summary"]["pct_of_20_cores_used_by_flow_process"] = round(
                    100 * statistics.median(cb) / 20, 2)
            sub = [r.get("h_submit_s") for r in measured if r.get("h_submit_s") is not None]
            if sub:
                ms = statistics.median(sub)
                doc["summary"]["submit_whole_batch_s_median"] = round(ms, 3)
                doc["summary"]["submission_rate_per_s"] = round(BATCH[(cfg, wl)] / ms, 1)
                doc["summary"]["submission_headroom_x"] = round(
                    (BATCH[(cfg, wl)] / ms) / statistics.median(th), 1)
        else:
            cr = [r.get("h_create_s") for r in measured if r.get("h_create_s") is not None]
            if cr:
                mc = statistics.median(cr)
                doc["summary"]["create_whole_batch_s_median"] = round(mc, 3)
                doc["summary"]["creation_rate_per_s"] = round(BATCH[(cfg, wl)] / mc, 1)
                doc["summary"]["submission_headroom_x"] = round(
                    (BATCH[(cfg, wl)] / mc) / statistics.median(th), 1)
        path = f"{S}/prefect_throughput_{cfg}_{wl}.json"
        json.dump(doc, open(path, "w"), indent=1)
        index[f"{cfg}_{wl}"] = doc["summary"]
        print(f"wrote {os.path.basename(path)}  "
              f"thr={doc['summary']['throughput_median_per_s']}/s "
              f"({doc['summary']['pct_of_ceiling']}% of ceiling) "
              f"jobs={sorted({r['jobs'] for r in reps})} peak={doc['summary']['peak_concurrency_max']}")
    json.dump(index, open(f"{S}/summary_index.json", "w"), indent=1)


if __name__ == "__main__":
    main()
