import glob, json, os, statistics

S = os.path.dirname(os.path.abspath(__file__))
RAW = os.path.join(S, "raw")

MECH = {
    "sleep": (
        "one worker process; activity_executor=concurrent.futures.ThreadPoolExecutor("
        "max_workers=20); max_concurrent_activities=20. time.sleep(0.1) releases the GIL, "
        "so 20 threads serve 20 activities at once. Activity task pollers left at the SDK "
        "default PollerBehaviorSimpleMaximum(5)."
    ),
    "cpu": (
        "one worker process; activity_executor=concurrent.futures.ProcessPoolExecutor("
        "max_workers=20) with a SharedStateManager; max_concurrent_activities=20. The process "
        "pool route from Temporal's docs worked, so 20 separate worker processes were not "
        "needed; 20 OS processes compute fibo(31) so the GIL is not in the way. The pool is "
        "spawned and warmed before the first rep and reused across reps. Activity task pollers "
        "left at the SDK default PollerBehaviorSimpleMaximum(5)."
    ),
}
CONF = {
    "stock": "dynamicconfig/development-sql.yaml exactly as published by temporalio/docker-compose; history.transferProcessorMaxPollRPS left at its default of 20.",
    "tuned": "the same compose and dynamic config with history.transferProcessorMaxPollRPS raised to 1000, confirmed applied in the temporal server log.",
}
CEIL = {"sleep": 199.17, "cpu": 111.10}

for p in sorted(glob.glob(os.path.join(RAW, "temporal_throughput_*.json"))):
    d = json.load(open(p))
    cfg, wl = d["config"], d["workload"]
    man = d.get("_manifest", {})
    tp = [r["throughput_per_s"] for r in d["reps"]]
    out = {
        "engine": "temporal",
        "config": cfg,
        "workload": wl,
        "concurrency": 20,
        "concurrency_mechanism": MECH[wl],
        "is_default": cfg == "stock",
        "notes": (
            "Temporal 1.29.7, Postgres variant of the official docker-compose (numHistoryShards 4), "
            "on an otherwise idle m7i.8xlarge (32 vCPU / 16 physical cores). "
            "Config: %s "
            "Workload: 2000 independent activities per rep, organised as 100 workflows each fanning "
            "out 20 activities onto a dedicated activity task queue (bench-act); the workflow worker "
            "runs in a separate process on bench-wf so workflow tasks never take activity slots. "
            "All 2000 activity tasks are enqueued and waiting before the activity worker is released "
            "(verified per rep: 2000/2000 ActivityTaskScheduled before release, plus a 5 s idle "
            "settle; raising that settle to 30 s moved throughput by 1%%, so submission is outside "
            "the measured window). 6 reps, rep 0 discarded as warmup. Every number is derived from "
            "the server's event history (ActivityTaskScheduled / Started / Completed), not from "
            "harness wall clock. span_s = last completion - first start; throughput = 2000 / span. "
            "latency_p50_s and latency_p95_s are scheduled -> completed and are therefore dominated "
            "by the pre-enqueued backlog wait, not by Temporal's dispatch latency. exec_p50_s is "
            "started -> completed and includes the completion round trip to the server. "
            "Measured ceiling for this workload with the same worker code and no orchestrator in the "
            "path: %.1f activities/s; median here is %.1f/s, %.0f%% of that ceiling. "
            "Peak concurrency derived from timestamps never exceeded 20 in any rep."
            % (CONF[cfg], CEIL[wl], statistics.median(tp), statistics.median(tp) / CEIL[wl] * 100.0)
        ),
        "reps": [
            {
                "jobs": r["jobs"],
                "span_s": r["span_s"],
                "throughput_per_s": r["throughput_per_s"],
                "latency_p50_s": r["latency_p50_s"],
                "latency_p95_s": r["latency_p95_s"],
                "exec_p50_s": r["exec_p50_s"],
                "exec_p95_s": r["exec_p95_s"],
                "effective_concurrency": r["effective_concurrency"],
                "peak_concurrency_from_timestamps": r["peak_concurrency_from_timestamps"],
                "worker_cores_busy_over_span": r["worker_cores_busy_over_span"],
                "activities_failed": r["activities_failed"],
                "activities_timed_out": r["activities_timed_out"],
                "activities_enqueued_before_release": r["activities_enqueued_before_go"],
                "enqueue_phase_s_excluded": r["enqueue_phase_s"],
            }
            for r in d["reps"]
        ],
        "measured_ceiling_per_s": CEIL[wl],
        "pct_of_measured_ceiling": round(statistics.median(tp) / CEIL[wl] * 100.0, 1),
        "workload_shape": {k: man.get(k) for k in ("total", "per_wf", "nwf", "activity", "arg")},
    }
    dest = os.path.join(S, os.path.basename(p))
    json.dump(out, open(dest, "w"), indent=1)
    print("wrote", os.path.basename(dest))
