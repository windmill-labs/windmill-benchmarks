#!/usr/bin/env python3
"""Turn the raw per-rep files pulled off the box into the two deliverables."""
import json
import os
import statistics

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "out")
PREFIX = "tssleep"
SLEEP_S = 0.1
CONC = 20

NOTES = (
    "2000 independent single-task workflow runs (no DAG, no parents), bulk-triggered with "
    "the TypeScript SDK's runManyNoWait -> BulkTriggerWorkflow gRPC, which the SDK splits "
    "into 4 chunks of 500 (its default batchSize) sent serially. Pre-enqueue is real: the "
    "worker is paused through PATCH /api/v1/workers/{id} {isPaused:true} before the bulk "
    "trigger and unpaused immediately after it returns, so all 2000 rows are QUEUED before "
    "any slot is filled. Hatchet's default slot count is 100; it was set to 20 (worker "
    "option slots: 20) so every engine in this study runs at the same concurrency. One "
    "discarded warmup + 5 measured reps, 60s idle between reps, 90s settle after the worker "
    "connected. Timings are Hatchet's own: created_at from v1_tasks_olap.inserted_at, "
    "started_at/completed_at from the STARTED and FINISHED rows in v1_task_events_olap. "
    "Only the sleeping workload was measured: a naive fibo in Node is about ten times "
    "faster than in CPython, so a CPU-bound body would compare V8 against CPython rather "
    "than the engine against itself."
)


def load(p):
    with open(p) as f:
        return json.load(f)


def conc_profile(path):
    """Time-weighted occupancy of the 20 slots over the measured span."""
    t = load(path)["tasks"]
    ev = sorted([(x["started"], 1) for x in t] + [(x["completed"], -1) for x in t],
                key=lambda e: (e[0], e[1]))
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
        "peak_concurrency": peak,
        "mean_concurrency": round(sum(k * v for k, v in time_at.items()) / total, 2),
        "frac_time_at_20_concurrent": round(
            sum(v for k, v in time_at.items() if k == CONC) / total, 4),
        "frac_time_above_20_concurrent": round(
            sum(v for k, v in time_at.items() if k > CONC) / total, 4),
    }


def main():
    ceiling = CONC / SLEEP_S
    reps = []
    for i in (1, 2, 3, 4, 5):
        tag = f"{PREFIX}-r{i}"
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
            "mean_concurrency": cp["mean_concurrency"],
            "peak_concurrency": cp["peak_concurrency"],
            "exec_p95_s": r["exec_p95_s"],
            "queue_wait_p50_s": r["queue_wait_p50_s"],
            "span_from_enqueue_s": r["span_from_enqueue_s"],
            "throughput_from_enqueue_per_s": r["throughput_from_enqueue_per_s"],
            "pct_of_ceiling": round(100 * r["throughput_per_s"] / ceiling, 1),
            "frac_time_at_20_concurrent": cp["frac_time_at_20_concurrent"],
            "frac_time_above_20_concurrent": cp["frac_time_above_20_concurrent"],
            "slot_utilisation": r["slot_utilisation"],
            "mean_slot_cycle_s": r["mean_slot_cycle_s"],
            "distinct_workers": r["distinct_workers"],
            "statuses": r["statuses"],
            "max_retry_count": r["max_retry_count"],
            "all_completed": r["all_completed"],
            "enqueue_s": e["enqueue_s"],
            "enqueue_window_s": r["enqueue_window_s"],
            "first_start_after_last_enqueue_s": r["first_start_after_last_enqueue_s"],
        })

    tps = [x["throughput_per_s"] for x in reps]
    occ = [x["mean_concurrency"] for x in reps]
    doc = {
        "engine": "hatchet",
        "config": "stock",
        "language": "typescript",
        "workload": "sleep",
        "concurrency": CONC,
        "concurrency_mechanism": "1 worker x 20 slots",
        "is_default": False,
        "notes": NOTES,
        "task_body": "await new Promise((r) => setTimeout(r, 100))",
        "jobs_per_rep": 2000,
        "median_throughput_per_s": round(statistics.median(tps), 3),
        "throughput_spread_per_s": [min(tps), max(tps)],
        "theoretical_ceiling_per_s": round(ceiling, 2),
        "theoretical_ceiling_basis": "20 slots / 0.100s timer",
        "pct_of_ceiling": round(100 * statistics.median(tps) / ceiling, 1),
        "median_latency_p50_s": round(statistics.median(
            [x["latency_p50_s"] for x in reps]), 4),
        "median_latency_p95_s": round(statistics.median(
            [x["latency_p95_s"] for x in reps]), 4),
        "median_exec_p50_s": round(statistics.median([x["exec_p50_s"] for x in reps]), 4),
        "median_mean_concurrency": round(statistics.median(occ), 2),
        "reps": reps,
    }
    with open(f"{HERE}/hatchet_ts_throughput_sleep.json", "w") as f:
        json.dump(doc, f, indent=2)
    print(f"median {doc['median_throughput_per_s']}/s of {ceiling}/s ceiling "
          f"= {doc['pct_of_ceiling']}%, spread {doc['throughput_spread_per_s']}, "
          f"mean occupancy {doc['median_mean_concurrency']}/20, "
          f"lat p50 {doc['median_latency_p50_s']}s p95 {doc['median_latency_p95_s']}s, "
          f"exec p50 {doc['median_exec_p50_s']}s")


if __name__ == "__main__":
    main()
