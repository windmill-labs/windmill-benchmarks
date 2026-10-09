#!/usr/bin/env python3
"""Turn the raw per-rep files pulled off the box into the three deliverables."""
import json
import os
import statistics

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "out")

NOTES = {
    "sleep": (
        "2000 independent single-task workflow runs (no DAG, no parents), bulk-triggered "
        "with the Python SDK's run_many(wait_for_result=False) -> BulkTriggerWorkflow gRPC, "
        "which the SDK splits into 2 chunks of 1000 (its MAX_BULK_WORKFLOW_RUN_BATCH_SIZE). "
        "Pre-enqueue is real: the worker is paused through PATCH /api/v1/workers/{id} "
        "{isPaused:true} before the bulk trigger and unpaused immediately after it returns, "
        "so all 2000 rows are QUEUED before any slot is filled. All 2000 rows of a rep share "
        "one inserted_at per 1000-row chunk; the two chunks are ~0.12s apart and the first "
        "task starts within ~35ms of the insert, so submission is not inside the throughput "
        "window. Hatchet's default slot count is 100; it was set to 20 so every engine in "
        "this study runs at the same concurrency. One discarded warmup + 5 measured reps, "
        "60s idle between reps, 90s settle after the worker connected. Timings are Hatchet's "
        "own: created_at from v1_tasks_olap.inserted_at (byte-identical to the OLTP "
        "v1_task.inserted_at, checked by join), started_at/completed_at from the STARTED and "
        "FINISHED rows in v1_task_events_olap."
    ),
    "cpu": (
        "Same shape as the sleep workload, but the task body is naive recursive fibo(31) "
        "(~114ms of real CPU, measured in the worker image) and the 20 slots are 20 separate "
        "CPython processes with slots=1 each, not 20 slots in one process: 20 slots inside "
        "one interpreter would be serialised by the GIL and would measure CPython rather "
        "than Hatchet. All 20 workers are paused before the bulk trigger and unpaused "
        "concurrently from a thread pool after it returns. docker stats during the reps "
        "confirms the fleet drawing ~1770% CPU (17.7 of 32 cores) with the box at ~20 busy "
        "cores, so 20 cores really were in use."
    ),
}

CFG = {
    "sleep": {"mechanism": "1 worker x 20 slots", "body": "time.sleep(0.1)",
              "ceiling_basis": "20 slots / 0.100s sleep", "compute_s": 0.1},
    "cpu": {"mechanism": "20 workers x 1 slot", "body": "naive recursive fibo(31)",
            "ceiling_basis": "20 cores / 0.115166s, the median of 15 fibo(31) readings "
                             "taken in the worker image on the idle box after the run",
            "compute_s": 0.115166},
}


def load(p):
    with open(p) as f:
        return json.load(f)


def conc_profile(path):
    t = load(path)["tasks"]
    ev = sorted([(x["started"], 1) for x in t] + [(x["completed"], -1) for x in t],
                key=lambda e: (e[0], e[1]))
    t0 = min(x["started"] for x in t)
    cur, prev, peak = 0, ev[0][0], 0
    time_at = {}
    for ts, d in ev:
        if ts > prev:
            time_at[cur] = time_at.get(cur, 0.0) + (ts - prev)
            prev = ts
        cur += d
        peak = max(peak, cur)
    total = sum(time_at.values())
    return {
        "peak_concurrent": peak,
        "mean_concurrent": round(sum(k * v for k, v in time_at.items()) / total, 2),
        "frac_time_above_20": round(sum(v for k, v in time_at.items() if k > 20) / total, 4),
        "frac_time_at_20": round(sum(v for k, v in time_at.items() if k == 20) / total, 4),
    }


def build(workload):
    cfg = CFG[workload]
    interp = load(f"{OUT}/interp_{workload}.json")
    compute = cfg["compute_s"] or statistics.median(interp["fibo31_s"])
    ceiling = 20 / compute

    reps = []
    for i in (1, 2, 3, 4, 5):
        tag = f"{workload}-r{i}"
        r = load(f"{OUT}/res_{tag}.json")
        e = load(f"{OUT}/enq_{tag}.json")
        cp = conc_profile(f"{OUT}/raw_{tag}.json")
        reps.append({
            "rep": i,
            "jobs": r["jobs"],
            "span_s": r["span_s"],
            "throughput_per_s": r["throughput_per_s"],
            "latency_p50_s": r["latency_p50_s"],
            "latency_p95_s": r["latency_p95_s"],
            "exec_p50_s": r["exec_p50_s"],
            "exec_p95_s": r["exec_p95_s"],
            "queue_wait_p50_s": r["queue_wait_p50_s"],
            "span_from_enqueue_s": r["span_from_enqueue_s"],
            "throughput_from_enqueue_per_s": r["throughput_from_enqueue_per_s"],
            "pct_of_ceiling": round(100 * r["throughput_per_s"] / ceiling, 1),
            "peak_concurrent": cp["peak_concurrent"],
            "mean_concurrent": cp["mean_concurrent"],
            "frac_time_at_20_concurrent": cp["frac_time_at_20"],
            "frac_time_above_20_concurrent": cp["frac_time_above_20"],
            "slot_utilisation": r["slot_utilisation"],
            "mean_slot_cycle_s": r["mean_slot_cycle_s"],
            "intra_worker_gap_p50_s": (r["per_worker_gap_p50_s"]
                                       if r["intra_worker_overlaps"] == 0 else None),
            "distinct_workers": r["distinct_workers"],
            "statuses": r["statuses"],
            "max_retry_count": r["max_retry_count"],
            "all_completed": r["all_completed"],
            "enqueue_s": e["enqueue_s"],
            "enqueue_window_s": r["enqueue_window_s"],
            "first_start_after_last_enqueue_s": r["first_start_after_last_enqueue_s"],
        })

    tps = [x["throughput_per_s"] for x in reps]
    return {
        "engine": "hatchet",
        "config": "stock",
        "workload": workload,
        "concurrency": 20,
        "concurrency_mechanism": cfg["mechanism"],
        "is_default": False,
        "notes": NOTES[workload],
        "task_body": cfg["body"],
        "jobs_per_rep": 2000,
        "median_throughput_per_s": round(statistics.median(tps), 3),
        "throughput_spread_per_s": [min(tps), max(tps)],
        "theoretical_ceiling_per_s": round(ceiling, 2),
        "theoretical_ceiling_basis": cfg["ceiling_basis"],
        "pct_of_ceiling": round(100 * statistics.median(tps) / ceiling, 1),
        "median_latency_p50_s": round(statistics.median(
            [x["latency_p50_s"] for x in reps]), 4),
        "median_latency_p95_s": round(statistics.median(
            [x["latency_p95_s"] for x in reps]), 4),
        "median_exec_p50_s": round(statistics.median([x["exec_p50_s"] for x in reps]), 4),
        "reps": reps,
    }


def main():
    for wl in ("sleep", "cpu"):
        d = build(wl)
        with open(f"{HERE}/hatchet_throughput_{wl}.json", "w") as f:
            json.dump(d, f, indent=2)
        print(f"{wl}: median {d['median_throughput_per_s']}/s of "
              f"{d['theoretical_ceiling_per_s']}/s ceiling = {d['pct_of_ceiling']}%, "
              f"spread {d['throughput_spread_per_s']}, "
              f"lat p50 {d['median_latency_p50_s']}s p95 {d['median_latency_p95_s']}s, "
              f"exec p50 {d['median_exec_p50_s']}s")


if __name__ == "__main__":
    main()
