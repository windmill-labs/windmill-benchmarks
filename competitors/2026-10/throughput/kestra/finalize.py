#!/usr/bin/env python3
"""Turn raw per-rep harness output into the requested result files."""
import json, sys, statistics, os

SP = os.path.dirname(os.path.abspath(__file__))

MECH = ("worker thread pool size, set with the `--worker-thread` flag on "
        "`kestra server standalone` (one Kestra worker thread runs one task at a time)")

COMBOS = [
    ("process", "sleep"),
    ("process", "cpu"),
    ("docker", "sleep"),
]

NOTES = {
 ("process", "sleep"): None,
 ("process", "cpu"): None,
 ("docker", "sleep"): None,
}

def load(runner, workload):
    p = os.path.join(SP, f"raw_{runner}_{workload}.json")
    if not os.path.exists(p):
        return None
    return json.load(open(p))

KEEP = ["jobs", "span_s", "throughput_per_s", "latency_p50_s", "latency_p95_s",
        "exec_p50_s", "exec_p95_s", "peak_concurrency",
        "seconds_above_20_concurrent", "mean_concurrency", "submit_s",
        "n_exec", "cpu"]

def main():
    shape = json.load(open(os.path.join(SP, "shape.json")))
    summary = {}
    for runner, workload in COMBOS:
        raw = load(runner, workload)
        if raw is None:
            print(f"skip {runner}/{workload}: no raw file")
            continue
        reps = [{k: r[k] for k in KEEP if k in r} for r in raw]
        doc = {
            "engine": "kestra",
            "config": runner,
            "workload": workload,
            "concurrency": 20,
            "concurrency_mechanism": MECH,
            "is_default": False,
            "notes": NOTES[(runner, workload)] or shape["notes"][f"{runner}_{workload}"],
            "sanity": {
                "all_2000_succeeded_every_rep": True,
                "how": "the harness refuses a rep unless all 100 executions report SUCCESS and exactly 2000 unit task runs each carry CREATED, RUNNING and SUCCESS timestamps",
                "peak_concurrency_per_rep": [r["peak_concurrency"] for r in reps],
                "seconds_above_20_concurrent_per_rep": [r.get("seconds_above_20_concurrent") for r in reps],
                "concurrency_derivation": "interval sweep over every unit task run's RUNNING -> SUCCESS window",
            },
            "cpu_sampler_note": "cpu.* is sampled host-wide every 0.5s from /proc/stat and /proc/*/comm. busy_cores_p50 is cores busy across the whole box (Kestra JVM + Postgres + task processes). python_procs counts any process whose comm starts with python3, so it includes the harness itself and overcounts by 1; cpucheck_process_cpu.json has the filtered per-process accounting.",
            "reps": reps,
        }
        out = os.path.join(SP, f"kestra_throughput_{runner}_{workload}.json")
        json.dump(doc, open(out, "w"), indent=1)
        tps = [r["throughput_per_s"] for r in reps]
        summary[f"{runner}/{workload}"] = {
            "median_throughput": round(statistics.median(tps), 2),
            "min": min(tps), "max": max(tps),
            "lat_p50": round(statistics.median(r["latency_p50_s"] for r in reps), 3),
            "lat_p95": round(statistics.median(r["latency_p95_s"] for r in reps), 3),
            "exec_p50": round(statistics.median(r["exec_p50_s"] for r in reps), 3),
            "peak_conc": max(r["peak_concurrency"] for r in reps),
            "over20_s": max(r.get("seconds_above_20_concurrent", 0) for r in reps),
            "cores_p50": [r["cpu"]["busy_cores_p50"] for r in reps],
        }
        print(f"wrote {out}")
    print(json.dumps(summary, indent=1))

if __name__ == "__main__":
    main()
