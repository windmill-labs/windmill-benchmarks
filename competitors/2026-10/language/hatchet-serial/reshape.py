#!/usr/bin/env python3
"""Reshape extract.py output into the requested hatchet_<lang>_<workload>.json form,
then re-validate every invariant the brief asks for."""
import json
import os
import statistics
import sys

SRC = sys.argv[1]  # dir holding the extract.py outputs
DST = sys.argv[2]

JOBS = [
    ("py", "fibonacci_40_10", 40),
    ("py", "fibonacci_10_33", 10),
    ("ts", "fibonacci_40_10", 40),
    ("ts", "fibonacci_10_33", 10),
    ("pyctl", "fibonacci_40_10", 40),
]

summary = {}
for lang, wl, ntasks in JOBS:
    p = os.path.join(SRC, f"extract_{lang}_{wl}.json")
    if not os.path.exists(p):
        print(f"MISSING {p}")
        continue
    d = json.load(open(p))
    reps = d["runs"]
    errs = []
    if len(reps) != 5:
        errs.append(f"{len(reps)} reps, expected 5")
    for k, r in enumerate(reps):
        for key in ("created_at", "started_at", "completed_at"):
            if len(r[key]) != ntasks:
                errs.append(f"rep{k+1} {key}: {len(r[key])} entries, expected {ntasks}")
        if r["created_at"][0] != 0.0:
            errs.append(f"rep{k+1} created_at[0]={r['created_at'][0]} != 0.0")
        c, s, f = r["created_at"], r["started_at"], r["completed_at"]
        for i in range(ntasks):
            if not (c[i] <= s[i] <= f[i]):
                errs.append(f"rep{k+1} task{i}: not monotonic {c[i]}/{s[i]}/{f[i]}")
        for i in range(1, ntasks):
            if c[i] < c[i - 1]:
                errs.append(f"rep{k+1} created_at not sorted at {i}")
            if s[i] < f[i - 1]:
                errs.append(f"rep{k+1} overlap: started_at[{i}]={s[i]} < completed_at[{i-1}]={f[i-1]}")
    out = {"lang": lang, "workload": wl,
           "reps": [{"workers": r["workers"], "created_at": r["created_at"],
                     "started_at": r["started_at"], "completed_at": r["completed_at"]}
                    for r in reps]}
    name = f"hatchet_{lang}_{wl}.json"
    with open(os.path.join(DST, name), "w") as fh:
        json.dump(out, fh)
    totals = [round(max(r["completed_at"]), 3) for r in reps]
    summary[(lang, wl)] = {
        "file": name, "totals": totals, "median": statistics.median(totals),
        "min": min(totals), "max": max(totals),
        "per_task_ms": round(statistics.median(totals) / ntasks * 1000, 2),
        "errors": errs,
        "interpreter": d.get("interpreter"),
    }
    print(f"{lang:6s} {wl:17s} totals={totals} median={statistics.median(totals):.3f} "
          f"per_task={summary[(lang,wl)]['per_task_ms']}ms errors={len(errs)}")
    for e in errs:
        print("   ERROR", e)

json.dump({f"{k[0]}/{k[1]}": v for k, v in summary.items()},
          open(os.path.join(DST, "_summary.json"), "w"), indent=2)
