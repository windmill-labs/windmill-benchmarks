#!/usr/bin/env python3
"""Pick the median measured rep per set and report the spread.

Rep 0 is the warmup and is discarded, matching the published methodology.
"""
import glob, json, os, statistics, sys

S = os.path.dirname(os.path.abspath(__file__))


def load(label):
    reps = []
    for f in sorted(glob.glob(f"{S}/{label}_r*.json")):
        d = json.load(open(f))
        r = int(f.rsplit("_r", 1)[1].split(".")[0])
        reps.append((r, d))
    reps.sort()
    return reps


def summarize(label):
    reps = load(label)
    measured = [d for r, d in reps if r > 0]
    if not measured:
        return None
    totals = [max(d["completed_at"]) for d in measured]
    order = sorted(range(len(totals)), key=lambda i: totals[i])
    med_i = order[len(order) // 2]
    med = measured[med_i]
    n = len(med["created_at"])
    c, st, e = med["created_at"], med["started_at"], med["completed_at"]
    waits = [st[i] - c[i] for i in range(n)]
    runs = [e[i] - st[i] for i in range(n)]
    gaps = [c[i] - e[i - 1] for i in range(1, n)]
    return {
        "label": label, "units": n, "reps_measured": len(measured),
        "median_total": round(statistics.median(totals), 3),
        "min_total": round(min(totals), 3), "max_total": round(max(totals), 3),
        "per_unit_ms": round(statistics.median(totals) / n * 1000, 1),
        "median_wait_s": round(statistics.median(waits), 3),
        "median_exec_ms": round(statistics.median(runs) * 1000, 1),
        "median_gap_s": round(statistics.median(gaps), 3),
        "rep": med,
    }


if __name__ == "__main__":
    out = {}
    for label in sys.argv[1:]:
        s = summarize(label)
        if not s:
            print(f"{label}: no reps"); continue
        out[label] = s
        print(f"{label:12} units={s['units']:3} median_total={s['median_total']:8.3f}s "
              f"({s['min_total']:.3f}-{s['max_total']:.3f})  per-unit={s['per_unit_ms']:8.1f}ms | "
              f"wait={s['median_wait_s']:.3f}s exec={s['median_exec_ms']:.1f}ms gap={s['median_gap_s']:.3f}s")
    json.dump(out, open(f"{S}/summary.json", "w"), indent=1)
