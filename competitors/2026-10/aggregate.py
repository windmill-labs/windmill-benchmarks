#!/usr/bin/env python3
"""Print the tables that the docs pages and the blog post quote.

Reads results/measurements.json, which normalise.py builds from the raw files.
Every number published anywhere should appear here; if one does not, it was
not measured in this run.
"""
import json, os

ROOT = os.path.dirname(os.path.abspath(__file__))
R = json.load(open(f"{ROOT}/results/measurements.json"))["records"]

# Ceilings measured on the box with every engine stopped, not derived from the
# slot count: the machine has 16 physical cores behind 32 vCPU, so 20 CPU-bound
# tasks never get 20 cores. See README, "The ceiling is 16 cores".
SLEEP_CEILING = 200.0           # 20 slots / a 100 ms sleep
CPU_CEILING = {                 # effective cores / that image's single-threaded fibo(31)
    "windmill": 138.3, "hatchet": 141.3, "temporal": 124.0,
    "kestra": 141.6, "airflow": 120.5, "prefect": 146.5,
}

def pick(**kw):
    out = [r for r in R if all(r.get(k) == v for k, v in kw.items())]
    return out[0] if len(out) == 1 else None

def row(label, rec_sleep, rec_cpu, engine):
    s = f"{rec_sleep['median_throughput_per_s']:7.1f}" if rec_sleep else "      -"
    sp = f"{rec_sleep['median_throughput_per_s']/SLEEP_CEILING*100:4.0f}%" if rec_sleep else "    -"
    if rec_cpu:
        c = f"{rec_cpu['median_throughput_per_s']:7.1f}"
        cp = f"{rec_cpu['median_throughput_per_s']/CPU_CEILING[engine]*100:4.0f}%"
    else:
        c, cp = "      -", "    -"
    print(f"  {label:34} {s} {sp}   {c} {cp}")

print("\nTHROUGHPUT AT TWENTY CONCURRENT SLOTS, 2026-10-09")
print("  2000 independent 100 ms tasks, one machine per engine, median of 5 reps after a warmup")
print(f"  {'':34} {'sleep/s':>7} {'ceil':>5}   {'cpu/s':>7} {'ceil':>5}")
print("\n  as shipped, nothing changed")
for label, eng, cfg in [("Windmill", "windmill", "stock"), ("Hatchet", "hatchet", "stock"),
                        ("Temporal", "temporal", "stock"), ("Airflow", "airflow", "stock"),
                        ("Prefect, tasks in the flow process", "prefect", "in_process"),
                        ("Kestra, Docker runner", "kestra", "docker")]:
    rs = pick(experiment="throughput", run="throughput", engine=eng, config=cfg, workload="sleep")
    rc = pick(experiment="throughput", run="throughput", engine=eng, config=cfg, workload="cpu")
    if eng == "kestra" and cfg == "docker":
        rc = pick(experiment="throughput", run="language-throughput", engine="kestra", workload="cpu")
    row(label, rs, rc, eng)

print("\n  after the settings each engine's own docs name")
for label, eng, cfg in [("Windmill, dedicated workers (EE)", "windmill", "dedicated"),
                        ("Temporal, poll limit raised", "temporal", "tuned"),
                        ("Kestra, process runner", "kestra", "process"),
                        ("Airflow, scheduler sleep 0.1s", "airflow", "tuned"),
                        ("Prefect, work pool, polls lowered", "prefect", "work_pool_tuned")]:
    row(label, pick(experiment="throughput", run="throughput", engine=eng, config=cfg, workload="sleep"),
        pick(experiment="throughput", run="throughput", engine=eng, config=cfg, workload="cpu"), eng)

print("\n\nWHAT THE LANGUAGE SAVES PER TASK, 2026-10-09")
print("  the same 40 sequential tasks in each language, one machine per engine, as shipped")
print(f"  {'':34} {'python':>8} {'typescript':>11} {'change':>8}")
for label, eng, cfg in [("Windmill", "windmill", "stock"), ("Temporal", "temporal", "stock"),
                        ("Hatchet", "hatchet", "stock"), ("Kestra, Docker runner", "kestra", "docker"),
                        ("Kestra, process runner (configured)", "kestra", "process")]:
    py = pick(experiment="serial", engine=eng, config=cfg, language="python", workload="fibonacci_40_10")
    ts = [r for r in R if r["experiment"] == "serial" and r["engine"] == eng and r["config"] == cfg
          and r["language"] in ("typescript", "javascript") and r["workload"] == "fibonacci_40_10"]
    if py and ts:
        d = ts[0]["per_task_ms"] - py["per_task_ms"]
        print(f"  {label:34} {py['per_task_ms']:7.1f}ms {ts[0]['per_task_ms']:10.1f}ms {d:+7.1f}ms")

print("\n\nTHE LANGUAGE AT TWENTY SLOTS, 2026-10-09")
print("  sleeping task only: a naive fibo runs ~10x faster in Node, so a core-bound")
print("  comparison across languages would measure the runtime, not the engine")
for eng in ("windmill", "hatchet"):
    py = pick(experiment="throughput", run="language-throughput", engine=eng, language="python", workload="sleep")
    ts = pick(experiment="throughput", run="language-throughput", engine=eng, language="typescript", workload="sleep")
    py = py or pick(experiment="throughput", run="throughput", engine=eng, config="stock", workload="sleep")
    if py and ts:
        print(f"  {eng:12} python {py['median_throughput_per_s']:6.1f}/s   typescript {ts['median_throughput_per_s']:6.1f}/s"
              f"   {(20/ts['median_throughput_per_s'] - 20/py['median_throughput_per_s'])*1000:+6.1f} ms per job")

print("\n\nPREFECT THROUGH A PROCESS WORK POOL, 2026-10-08")
print("  each unit a deployment run, so a job like the other engines dispatch")
for r in [r for r in R if r.get("run") == "prefect-work-pool"]:
    print(f"  {r['config']:28} {r['workload']:18} {r['per_task_ms']:9.1f} ms per task")
print()
