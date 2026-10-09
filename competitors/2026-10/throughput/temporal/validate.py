import glob, json, os, statistics, sys

S = os.path.dirname(os.path.abspath(__file__))
CEIL = {"sleep": 199.17, "cpu": 111.10}   # measured no-orchestrator steady state
ok = True
rows = []
for path in sorted(glob.glob(os.path.join(S, "raw", "temporal_throughput_*.json"))):
    d = json.load(open(path))
    cfg, wl = d["config"], d["workload"]
    errs = []
    if len(d["reps"]) != 5:
        errs.append("expected 5 reps, got %d" % len(d["reps"]))
    for r in d["reps"]:
        if r["jobs"] != 2000:
            errs.append("rep%d: only %d/2000 activities completed" % (r["rep"], r["jobs"]))
        if r.get("jobs_scheduled") != 2000:
            errs.append("rep%d: %d activities scheduled" % (r["rep"], r.get("jobs_scheduled")))
        if r["activities_failed"] or r["activities_timed_out"]:
            errs.append("rep%d: %d failed / %d timed out" % (r["rep"], r["activities_failed"], r["activities_timed_out"]))
        if r["peak_concurrency_from_timestamps"] > 20:
            errs.append("rep%d: PEAK CONCURRENCY %d > 20" % (r["rep"], r["peak_concurrency_from_timestamps"]))
        if r["activities_enqueued_before_go"] != 2000:
            errs.append("rep%d: only %d enqueued before release" % (r["rep"], r["activities_enqueued_before_go"]))
    tp = [r["throughput_per_s"] for r in d["reps"]]
    lat50 = [r["latency_p50_s"] for r in d["reps"]]
    lat95 = [r["latency_p95_s"] for r in d["reps"]]
    ex50 = [r["exec_p50_s"] for r in d["reps"]]
    eff = [r["effective_concurrency"] for r in d["reps"]]
    cores = [r["worker_cores_busy_over_span"] for r in d["reps"]]
    peak = [r["peak_concurrency_from_timestamps"] for r in d["reps"]]
    enq = [r["enqueue_phase_s"] for r in d["reps"]]
    rows.append({
        "config": cfg, "workload": wl,
        "tp_med": statistics.median(tp), "tp_min": min(tp), "tp_max": max(tp),
        "tp_all": tp,
        "lat50": statistics.median(lat50), "lat95": statistics.median(lat95),
        "ex50": statistics.median(ex50),
        "eff": statistics.median(eff), "cores": statistics.median(cores),
        "peak": max(peak), "enq_med": statistics.median(enq), "enq_all": enq,
        "pct_ceiling": statistics.median(tp) / CEIL[wl] * 100.0,
    })
    if errs:
        ok = False
        print("FAIL %s:" % os.path.basename(path))
        for e in errs[:10]:
            print("   ", e)
    else:
        print("ok   %-44s 5 reps, 2000/2000 each, peak<=20, all pre-enqueued" % os.path.basename(path))

print()
h = "%-6s %-6s %8s %8s %8s %7s %9s %9s %9s %7s %7s %7s" % (
    "config", "wl", "tp_med", "tp_min", "tp_max", "%ceil", "lat_p50", "lat_p95", "exec_p50", "effcon", "cores", "peak")
print(h); print("-" * len(h))
for r in rows:
    print("%-6s %-6s %8.1f %8.1f %8.1f %6.1f%% %9.2f %9.2f %9.4f %7.2f %7.2f %7d" % (
        r["config"], r["workload"], r["tp_med"], r["tp_min"], r["tp_max"], r["pct_ceiling"],
        r["lat50"], r["lat95"], r["ex50"], r["eff"], r["cores"] or 0, r["peak"]))
print()
for r in rows:
    print("%-6s %-6s throughput reps: %s  | enqueue phase: %s" % (
        r["config"], r["workload"], [round(x, 1) for x in r["tp_all"]], [round(x, 2) for x in r["enq_all"]]))
print()
print("VALIDATION:", "ALL PASS" if ok else "FAILURES ABOVE")
