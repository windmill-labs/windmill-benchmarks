#!/usr/bin/env python3
"""Concurrency over time, derived from the per-task STARTED/FINISHED timestamps."""
import json
import sys


def profile(path, step=0.05):
    d = json.load(open(path))
    t = d["tasks"]
    ev = sorted([(x["started"], 1) for x in t] + [(x["completed"], -1) for x in t],
                key=lambda e: (e[0], e[1]))
    t0 = min(x["started"] for x in t)
    t1 = max(x["completed"] for x in t)
    # step through the timeline accumulating time at each concurrency level
    cur = 0
    prev = ev[0][0]
    time_at = {}
    peak = 0
    first_at_20 = None
    for ts, delta in ev:
        if ts > prev:
            time_at[cur] = time_at.get(cur, 0.0) + (ts - prev)
            prev = ts
        cur += delta
        peak = max(peak, cur)
        if cur >= 20 and first_at_20 is None:
            first_at_20 = ts
    total = sum(time_at.values())
    mean = sum(k * v for k, v in time_at.items()) / total
    at20 = sum(v for k, v in time_at.items() if k >= 20) / total
    below = sum(v for k, v in time_at.items() if k < 20) / total
    over = sum(v for k, v in time_at.items() if k > 20) / total
    return {
        "span_s": round(t1 - t0, 3),
        "peak": peak,
        "mean_concurrency": round(mean, 2),
        "frac_time_at_20": round(at20, 4),
        "frac_time_below_20": round(below, 4),
        "frac_time_above_20": round(over, 4),
        "ramp_to_20_s": round(first_at_20 - t0, 3) if first_at_20 is not None else None,
        "time_at_levels": {k: round(v, 3) for k, v in sorted(time_at.items())
                           if v > 0.05},
    }


if __name__ == "__main__":
    for p in sys.argv[1:]:
        print(p.split("/")[-1], json.dumps(profile(p)))
