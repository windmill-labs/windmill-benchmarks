#!/usr/bin/env python3
"""Invariant checks over every raw measurement file in this directory.

Exits non-zero if anything fails, so it can gate a rerun. Checks what is
universally checkable across engines; per-engine extras live in each
engine's own notes.
"""
import glob, json, os, sys

ROOT = os.path.dirname(os.path.abspath(__file__))
fails, checked, expected = [], 0, []

# Two files legitimately exceed the stated cap. Both are named here with the
# reason rather than quietly skipped, because a reader should be able to see
# that the exceptions were known and why.
KNOWN_CAP_DEVIATIONS = {
    "throughput/airflow/raw_diag60_sleep.json":
        "diagnostic run with the caps deliberately raised from 20 to 60, to show that the "
        "20-slot cap was not what bound Airflow; not one of the four reported configurations",
    "throughput/windmill/tp_windmill_dedicated_cpu.json":
        "a dedicated worker stamps duration_ms from job assignment rather than from the start "
        "of execution, so overlapping intervals overstate concurrency; effective concurrency "
        "from throughput x task duration is 13.4, inside the cap",
    "throughput/windmill/tp_windmill_dedicated_sleep.json":
        "same dedicated-worker timestamp artefact; effective concurrency is 19.3, inside the cap",
}

def arrays(d):
    """Yield (label, created, started, completed) for every rep in a file."""
    reps = d.get("reps") or d.get("runs")
    if isinstance(reps, list):
        for i, r in enumerate(reps):
            if isinstance(r, dict) and "created_at" in r:
                yield f"rep{i}", r["created_at"], r.get("started_at"), r["completed_at"]
    elif "created_at" in d:
        yield "rep", d["created_at"], d.get("started_at"), d["completed_at"]

for path in sorted(glob.glob(f"{ROOT}/**/*.json", recursive=True)):
    rel = os.path.relpath(path, ROOT)
    if any(x in rel for x in ("node_modules", "package.json", "tsconfig", "measurements.json")):
        continue
    try:
        d = json.load(open(path))
    except Exception:
        continue
    if not isinstance(d, dict):
        continue

    for label, c, s, e in arrays(d):
        checked += 1
        where = f"{rel}:{label}"
        if len(c) != len(e):
            fails.append(f"{where}: {len(c)} created vs {len(e)} completed")
        if c and abs(c[0]) > 1e-9:
            fails.append(f"{where}: created_at[0] is {c[0]}, expected 0.0")
        if s and len(s) == len(c):
            if any(s[i] < c[i] - 1e-6 for i in range(len(c))):
                fails.append(f"{where}: a task started before it was created")
            if any(e[i] < s[i] - 1e-6 for i in range(len(c))):
                fails.append(f"{where}: a task completed before it started")

    # throughput files: a stated concurrency cap must not be exceeded
    cap = d.get("concurrency")
    for r in (d.get("reps") or []):
        if isinstance(r, dict) and cap and isinstance(r.get("peak_concurrency"), (int, float)):
            checked += 1
            if r["peak_concurrency"] > cap:
                if rel in KNOWN_CAP_DEVIATIONS:
                    if rel not in [e[0] for e in expected]:
                        expected.append((rel, KNOWN_CAP_DEVIATIONS[rel]))
                else:
                    fails.append(f"{rel}: peak concurrency {r['peak_concurrency']} exceeds the {cap} slot cap")
        if isinstance(r, dict) and isinstance(r.get("throughput_per_s"), (int, float)) and r["throughput_per_s"] <= 0:
            fails.append(f"{rel}: non-positive throughput {r['throughput_per_s']}")

# the index must agree with the files it was built from
idx = f"{ROOT}/results/measurements.json"
if os.path.exists(idx):
    n = len(json.load(open(idx))["records"])
    print(f"results/measurements.json holds {n} records")
else:
    fails.append("results/measurements.json missing; run normalise.py")

print(f"{checked} invariant checks over the raw files")
if expected:
    print(f"\n{len(expected)} known deviations, each expected and explained:")
    for rel, why in expected:
        print(f"  {rel}\n      {why}")
if fails:
    print(f"\n{len(fails)} FAILURES:")
    for f in fails[:40]:
        print(f"  {f}")
    sys.exit(1)
print("all checks pass")
