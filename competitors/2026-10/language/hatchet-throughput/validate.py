#!/usr/bin/env python3
"""Sanity checks on the measured reps, printed so they can be read at a glance."""
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
d = json.load(open(f"{HERE}/hatchet_ts_throughput_sleep.json"))
print(f"{'rep':>4} {'jobs':>5} {'status':>12} {'peak':>5} {'mean':>6} {'>20':>5} "
      f"{'tp/s':>8} {'span':>7} {'exec50':>7} {'retries':>8} {'started<last_insert':>20}")
for r in d["reps"]:
    raw = json.load(open(f"{HERE}/out/raw_tssleep-r{r['rep']}.json"))["tasks"]
    last_created = max(x["created"] for x in raw)
    early = sum(1 for x in raw if x["started"] < last_created)
    print(f"{r['rep']:>4} {r['jobs']:>5} {','.join(r['statuses']):>12} "
          f"{r['peak_concurrency']:>5} {r['mean_concurrency']:>6} "
          f"{r['frac_time_above_20_concurrent']:>5} {r['throughput_per_s']:>8} "
          f"{r['span_s']:>7} {r['exec_p50_s']:>7} {r['max_retry_count']:>8} {early:>20}")
tps = [r["throughput_per_s"] for r in d["reps"]]
occ = [r["mean_concurrency"] for r in d["reps"]]
print()
print(f"median throughput {d['median_throughput_per_s']}/s   spread {min(tps)}-{max(tps)}/s")
print(f"mean occupancy    median {d['median_mean_concurrency']}/20   per rep {occ}")
print(f"all reps 2000/2000 COMPLETED: "
      f"{all(r['all_completed'] and r['jobs'] == 2000 for r in d['reps'])}")
print(f"peak never above 20: "
      f"{all(r['peak_concurrency'] <= 20 and r['frac_time_above_20_concurrent'] == 0 for r in d['reps'])}")
