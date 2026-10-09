#!/usr/bin/env python3
"""Recorder drain rate and drop onset, from the deliberate-overflow probe."""
import glob
import json
import os
import statistics

S = os.path.dirname(os.path.abspath(__file__))

s = []
for line in open(f"{S}/drain.txt"):
    p = line.split()
    if len(p) == 2 and p[1].isdigit():
        s.append((float(p[0]), int(p[1])))
s.sort()
t0 = s[0][0]
rel = [(t - t0, n) for t, n in s]

prod = {}
for p in sorted(glob.glob(f"{S}/out/probe_drop_r*.json")):
    d = json.load(open(p))
    prod[d["tag"]] = {"units": d["units"], "wall_s": d["wall_s"],
                      "units_per_s": round(d["units"] / d["wall_s"], 1)}
prod_rate = statistics.median([v["units_per_s"] for v in prod.values()])
production_end = sum(v["wall_s"] for v in prod.values()) + 2  # three back-to-back reps

def slope(lo, hi):
    w = [(t, n) for t, n in rel if lo <= t <= hi]
    if len(w) < 3:
        return None
    return round((w[-1][1] - w[0][1]) / (w[-1][0] - w[0][0]), 1)

plateau = next(t for i, (t, n) in enumerate(rel)
               if all(m == n for _u, m in rel[i:]))
final = rel[-1][1]

counts = {}
for line in open(f"{S}/drop_counts.txt"):
    p = line.strip().split("|")
    if len(p) == 3:
        counts[p[0]] = {"rows_recorded": int(p[1]), "completed_recorded": int(p[2])}
drops = [l for l in open(f"{S}/drop_lines.txt") if "Subscription queue is full" in l]

during = slope(2, production_end)
after = slope(production_end + 5, plateau - 5)

doc = {
    "experiment": "three back-to-back 2000-unit in_process sleep reps with no drain "
                  "barrier, 20-way, while sampling once a second how many task_run "
                  "rows the server had persisted for those reps",
    "production_rate_units_per_s": prod_rate,
    "per_rep_production": prod,
    "recorder_insert_rate_while_producing_per_s": during,
    "recorder_insert_rate_after_production_stopped_per_s": after,
    "task_runs_produced": sum(v["units"] for v in prod.values()),
    "task_runs_ever_persisted": final,
    "task_runs_lost": sum(v["units"] for v in prod.values()) - final,
    "seconds_after_production_to_finish_draining": round(plateau - production_end, 1),
    "recorded_after_drain": counts,
    "dropped_messages_logged": len(drops),
    "first_drop_line": drops[0].strip() if drops else None,
    "conclusion": (
        f"The recorder persists task runs at roughly {during} per second while the "
        f"flow process is competing for the box and about {after} per second once it "
        f"has the box to itself. The in_process configuration produces them at about "
        f"{prod_rate:.0f} units/s on the sleep body, two to five times faster. The "
        f"broker's 10000-message queue absorbs the first burst, which is why the "
        f"first one or two reps come out intact, but any run that sustains more than "
        f"roughly {after} task runs/s fills the queue and the server begins "
        f"discarding its own records. In this probe {sum(v['units'] for v in prod.values()) - final} "
        f"of {sum(v['units'] for v in prod.values())} task runs were never written, and the "
        f"recorder needed {round(plateau - production_end, 1)}s after the last rep "
        f"finished to persist what it had kept."),
}
json.dump(doc, open(f"{S}/drain_analysis.json", "w"), indent=1)
print(json.dumps({k: v for k, v in doc.items()
                  if k not in ("per_rep_production", "recorded_after_drain",
                               "first_drop_line", "experiment")}, indent=1))
