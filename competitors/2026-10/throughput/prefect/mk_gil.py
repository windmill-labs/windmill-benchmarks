#!/usr/bin/env python3
"""Write the in_process cpu GIL finding, with how the core figure was taken."""
import glob
import json
import os
import statistics

S = os.path.dirname(os.path.abspath(__file__))
FIBO31 = 0.1111
raw = json.load(open(f"{S}/reps_raw.json"))
rows = [r for r in raw["in_process|cpu"] if r["rep"] > 0 and r.get("jobs")]
h = {}
for p in glob.glob(f"{S}/out/ip_cpu_r*.json"):
    d = json.load(open(p))
    h[d["tag"]] = d

cores = [h[r["tag"]]["cores_busy_flow_proc"] for r in rows if r["tag"] in h]
cpu_s = [h[r["tag"]]["flow_proc_cpu_s"] for r in rows if r["tag"] in h]
th = [r["throughput_per_s"] for r in rows]
host = [r["host_cores_busy"] for r in rows if r["host_cores_busy"]]
med_cores = statistics.median(cores)
med_th = statistics.median(th)
ceiling = 20 / FIBO31

doc = {
    "summary": f"The in_process configuration nominally spreads 20-way, but on a "
               f"CPU-bound body it uses {med_cores:.2f} cores, "
               f"{100*med_cores/20:.1f}% of the 20 it spans, and reaches "
               f"{med_th:.2f} units/s against a {ceiling:.1f} units/s ceiling, "
               f"{100*med_th/ceiling:.1f}% of it. The 20 'concurrent' tasks are "
               f"threads of one CPython process, so the GIL serialises them.",
    "why": "ThreadPoolTaskRunner runs each @task in a thread of the flow's own "
           "process. Nothing is dispatched and nothing is isolated, so 20-way "
           "concurrency buys overlap only where the body releases the GIL. "
           "time.sleep does; a naive recursive fibo does not.",
    "throughput_per_s_median": round(med_th, 2),
    "throughput_per_s_reps": th,
    "theoretical_ceiling_per_s": round(ceiling, 1),
    "pct_of_ceiling": round(100 * med_th / ceiling, 2),
    "cores_busy_flow_process_median": round(med_cores, 3),
    "cores_busy_flow_process_reps": cores,
    "fraction_of_20_cores_used": round(med_cores / 20, 4),
    "pct_of_20_cores_used": round(100 * med_cores / 20, 2),
    "pct_of_32_vcpu_box_used": round(100 * med_cores / 32, 2),
    "host_cores_busy_median": round(statistics.median(host), 2) if host else None,
    "how_the_core_figure_was_taken": {
        "primary": "resource.getrusage(RUSAGE_SELF) in the flow process, read "
                   "immediately before the flow call and immediately after it "
                   "returns. ru_utime + ru_stime deltas give the CPU-seconds the "
                   "process consumed; dividing by the wall seconds of the same "
                   "interval gives cores busy. RUSAGE_SELF covers every thread of "
                   "the process, and in this configuration all 2000 task bodies are "
                   "threads of it, so nothing escapes the accounting.",
        "cpu_seconds_consumed_median": round(statistics.median(cpu_s), 1),
        "cpu_seconds_of_fibo_work_expected": round(2000 * FIBO31, 1),
        "orchestration_cpu_overhead_s": round(statistics.median(cpu_s) - 2000 * FIBO31, 1),
        "cross_check": "host-wide /proc/stat sampled once a second and sliced to each "
                       "rep's measured window, which gives the flow process plus the "
                       "Prefect server and Postgres",
        "box": "32 vCPU, nothing else running on it",
    },
    "contrast_with_sleep_workload": "On the sleep body the same configuration reaches "
        "far higher throughput, because time.sleep releases the GIL and 20 threads "
        "do overlap. The gap between the two workloads in this one configuration is "
        "the GIL, not Prefect's orchestration.",
}
json.dump(doc, open(f"{S}/gil_finding.json", "w"), indent=1)
print(json.dumps({k: doc[k] for k in (
    "throughput_per_s_median", "pct_of_ceiling", "cores_busy_flow_process_median",
    "pct_of_20_cores_used", "host_cores_busy_median")}, indent=1))
