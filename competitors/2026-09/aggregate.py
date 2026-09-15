#!/usr/bin/env python3
"""Turn the per-engine benchmark results into the docs-site schema + a summary.

Input:  ./results/<engine>_<steps>_<fib>.json  (one file per engine per workload)
Output: ./site_data/<engine>.json              (the schema windmilldocs src/data/<engine>.json uses)
        the two markdown tables in README.md, on stdout

    python3 aggregate.py
"""
import json
import glob
import os
import statistics
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
RESULTS = os.path.join(HERE, "results")
OUT = os.path.join(HERE, "site_data")

WORKLOADS = {
    "40_10": ("fibonacci_40_10", "40 lightweight tasks", "40 sequential tasks, each computing fibo(10)"),
    "10_33": ("fibonacci_10_33", "10 long running tasks", "10 sequential tasks, each computing fibo(33)"),
}

# How a task is executed, which is the thing that makes totals incomparable across families.
MODEL = {
    "windmill": "process per task",
    "windmill_tuned": "process per task",
    "windmill_script": "process per task",
    "windmill_dedicated": "warm process, one script",
    "airflow_tuned": "process per task",
    "temporal_tuned": "in the worker process",
    "airflow": "process per task",
    "kestra": "container per task",
    "kestra_process": "process per task",
    "prefect": "in the flow process",
    "temporal": "in the worker process",
    "hatchet": "in the worker process",
}


def load():
    data = {}
    for path in sorted(glob.glob(os.path.join(RESULTS, "*.json"))):
        base = os.path.basename(path)[:-5]
        parts = base.split("_")
        key = "_".join(parts[-2:])
        engine = "_".join(parts[:-2])
        if key not in WORKLOADS:
            continue
        with open(path) as fh:
            data.setdefault(engine, {})[key] = json.load(fh)
    return data


def median_run(doc):
    runs, totals = doc["runs"], doc["totals"]
    target = statistics.median_low(totals)
    return runs[totals.index(target)], target


def compute_per_task(doc, run):
    """Median wall time per task, and median time not explained by compute."""
    n = len(run["created_at"])
    total = max(run["completed_at"])
    interp = doc.get("interpreter") or {}
    fib33 = statistics.median(interp["fibo33_s"]) if interp.get("fibo33_s") else None
    return total / n, fib33


def main():
    data = load()
    os.makedirs(OUT, exist_ok=True)
    rows = []
    for engine, workloads in sorted(data.items()):
        usecases = {}
        for key, (uc, title, desc) in WORKLOADS.items():
            if key not in workloads:
                continue
            doc = workloads[key]
            run, total = median_run(doc)
            usecases[uc] = {"title": title, "description": desc, "python": [run]}
            per_task, fib33 = compute_per_task(doc, run)
            steps = len(run["created_at"])
            compute = (fib33 or 0) * steps if key == "10_33" else 0.0
            rows.append({
                "engine": engine,
                "version": doc.get("version", "?"),
                "workload": uc,
                "median_total": round(total, 3),
                "spread": f"{min(doc['totals']):.3f}-{max(doc['totals']):.3f}",
                "per_task_ms": round(per_task * 1000, 1),
                "overhead_total": round(total - compute, 3) if compute else None,
                "overhead_per_task_ms": round((total - compute) / steps * 1000, 1) if compute else round(per_task * 1000, 1),
                "model": MODEL.get(engine, "?"),
                "interpreter": ((doc.get("interpreter") or {}).get("version") or "?").split()[0],
            })
        if usecases:
            version = next(iter(workloads.values())).get("version", "?")
            with open(os.path.join(OUT, f"{engine}.json"), "w") as fh:
                json.dump({"version": version, "usecases": usecases}, fh, indent=1)

    for uc in ("fibonacci_40_10", "fibonacci_10_33"):
        sel = [r for r in rows if r["workload"] == uc]
        if not sel:
            continue
        sel.sort(key=lambda r: r["median_total"])
        print(f"\n### {uc}\n")
        print("| engine | version | median total | spread | per task | overhead/task | a task is | python |")
        print("|---|---|---|---:|---:|---:|---|---|")
        for r in sel:
            print(f"| {r['engine']} | {r['version']} | {r['median_total']}s | {r['spread']} | "
                  f"{r['per_task_ms']}ms | {r['overhead_per_task_ms']}ms | {r['model']} | {r['interpreter']} |")
    print(f"\nwrote {len(glob.glob(os.path.join(OUT, '*.json')))} site data files to {OUT}")


if __name__ == "__main__":
    main()
