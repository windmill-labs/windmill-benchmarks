#!/usr/bin/env python3
"""Reduce every measurement file in this directory to one canonical record.

The per-engine files keep whatever shape their harness produced, because each
was written against a different engine's API. This script is the single place
that knows those shapes, so validate.py and aggregate.py can stay simple.

Writes results/measurements.json.
"""
import glob, json, os, statistics, sys

ROOT = os.path.dirname(os.path.abspath(__file__))

def median(xs):
    return round(statistics.median(xs), 3) if xs else None

def rep_totals(reps):
    """Seconds from the first task starting to the last finishing, per rep."""
    out = []
    for r in reps:
        if "completed_at" in r:
            out.append(max(r["completed_at"]))
        elif "span_s" in r:
            out.append(r["span_s"])
    return out

def rep_tputs(reps):
    return [r["throughput_per_s"] for r in reps if "throughput_per_s" in r]

def load(path):
    with open(path) as fh:
        return json.load(fh)

records = []

# ---- serial study: one worker draining a chain, metric is milliseconds per task
for path in glob.glob(f"{ROOT}/language/*-serial/*.json"):
    name = os.path.basename(path)
    if "runtime" in name or name.startswith("control"):
        continue
    d = load(path)
    if not isinstance(d, dict):
        continue
    reps = d.get("reps") or d.get("runs")
    if not reps or not isinstance(reps, list) or not isinstance(reps[0], dict):
        continue
    totals = rep_totals(reps)
    if not totals:
        continue
    n = len(reps[0].get("completed_at", [])) or d.get("steps")
    if not n:
        continue
    parts = name[:-5].split("_")
    fname_config = "tuned" if "tuned" in parts else ("stock" if "stock" in parts else None)
    lang = d.get("lang", "python")
    lang = {"py": "python", "python3": "python", "bun": "typescript", "ts": "typescript", "js": "javascript"}.get(lang, lang)
    records.append({
        "experiment": "serial", "run": "language-serial", "engine": os.path.basename(os.path.dirname(path)).split("-")[0],
        "config": d.get("config") or d.get("runner") or fname_config or "stock",
        "language": lang,
        "workload": d.get("workload") or f"fibonacci_{d.get('steps')}_{d.get('fib')}",
        "units_per_rep": n, "reps_measured": len(totals),
        "median_total_s": median(totals), "min_total_s": round(min(totals), 3), "max_total_s": round(max(totals), 3),
        "per_task_ms": round(statistics.median(totals) / n * 1000, 1),
        "source": os.path.relpath(path, ROOT),
    })

# ---- throughput study: twenty concurrent slots, metric is jobs per second
for pattern in (f"{ROOT}/throughput/*/*.json", f"{ROOT}/language/*-throughput/*.json"):
    for path in glob.glob(pattern):
        name = os.path.basename(path)
        if any(k in name for k in ("runtime", "probe", "ceiling", "attribution", "smt", "shape",
                                   "cpucheck", "hostinfo", "summary_index", "reps_raw",
                                   "drain_analysis", "gil_finding", "package", "tsconfig")):
            continue
        d = load(path)
        if not isinstance(d, dict):
            continue
        reps = d.get("reps")
        if not reps or not isinstance(reps, list) or not isinstance(reps[0], dict):
            continue
        tputs = rep_tputs(reps)
        if not tputs:
            continue
        engine = d.get("engine") or os.path.basename(os.path.dirname(path)).split("-")[0]
        run = "language-throughput" if "-throughput/" in path.replace(ROOT, "") and "/language/" in path else "throughput"
        cfg = d.get("config", "stock")
        if "control" in name: cfg += " (control)"
        if "diag" in name or cfg == "diag60": cfg = "diagnostic, caps raised to 60"
        records.append({
            "experiment": "throughput", "run": run, "engine": engine,
            "config": cfg,
            "language": d.get("language", "python"),
            "workload": d.get("workload", "unknown"),
            "concurrency": d.get("concurrency", 20),
            "units_per_rep": reps[0].get("jobs", d.get("jobs_per_rep")),
            "reps_measured": len(tputs),
            "median_throughput_per_s": median(tputs),
            "min_throughput_per_s": round(min(tputs), 2), "max_throughput_per_s": round(max(tputs), 2),
            "mean_concurrency": reps[0].get("mean_concurrency") or reps[0].get("avg_concurrency"),
            "source": os.path.relpath(path, ROOT),
        })

# ---- Prefect through a process work pool, measured 2026-10-08, one file per rep
pf = {}
for path in glob.glob(f"{ROOT}/prefect-work-pool/*_r*.json"):
    label = os.path.basename(path)[:-5].rsplit("_r", 1)[0]
    rep = int(os.path.basename(path)[:-5].rsplit("_r", 1)[1])
    pf.setdefault(label, []).append((rep, load(path)))
for label, reps in pf.items():
    reps.sort()
    measured = [d for i, d in reps if i > 0]          # rep 0 is the discarded warmup
    if not measured:
        continue
    totals = [max(d["completed_at"]) for d in measured]
    n = len(measured[0]["created_at"])
    records.append({
        "experiment": "serial", "run": "prefect-work-pool", "engine": "prefect",
        "config": "work pool, polls lowered" if label.startswith("tuned") else "work pool, stock",
        "language": "python",
        "workload": "fibonacci_40_10" if "4010" in label else "fibonacci_10_33",
        "units_per_rep": n, "reps_measured": len(totals),
        "median_total_s": median(totals), "min_total_s": round(min(totals), 3), "max_total_s": round(max(totals), 3),
        "per_task_ms": round(statistics.median(totals) / n * 1000, 1),
        "source": f"prefect-work-pool/{label}_r*.json",
    })

records.sort(key=lambda r: (r["experiment"], r["run"], r["engine"], r["config"], r["language"], r["workload"]))
os.makedirs(f"{ROOT}/results", exist_ok=True)
with open(f"{ROOT}/results/measurements.json", "w") as fh:
    json.dump({"run": "2026-10", "records": records}, fh, indent=1)
print(f"{len(records)} measurements -> results/measurements.json")
for r in records:
    metric = f"{r['per_task_ms']:>8.1f} ms/task" if r["experiment"] == "serial" else f"{r['median_throughput_per_s']:>8.1f} jobs/s"
    print(f"  {r['run'][:20]:20} {r['engine']:9} {r['config'][:22]:22} {r['language'][:10]:10} {r['workload'][:18]:18} {metric}")
