"""Invariant checks on every raw result file.

    python3 validate.py [results/*.json]

Asserts, per run: created_at[0] == 0, the three timestamp arrays are the same
length, tasks are ordered by created_at, no two tasks overlap (so the workload
really ran sequentially), and the reported total equals max(completed_at).
Prints the per-task medians the README quotes.
"""
import glob, json, os, statistics, sys

paths = sys.argv[1:] or sorted(
    glob.glob(os.path.join(os.path.dirname(os.path.abspath(__file__)), "results", "*.json"))
)
for p in paths:
    d = json.load(open(p))
    if "runs" not in d:  # e.g. results/temporal_interpreter.json, a probe, not a run
        continue
    print("==", os.path.basename(p), "| engine", d["engine"], "ver", d["version"], "workload", d["workload"])
    print("   runs:", len(d["runs"]), "| totals:", d["totals"])
    assert all(r["created_at"][0] == 0.0 for r in d["runs"]), "created_at[0] != 0"
    assert all(len(r["created_at"]) == len(r["started_at"]) == len(r["completed_at"]) for r in d["runs"])
    assert all(r["created_at"] == sorted(r["created_at"]) for r in d["runs"]), "not ordered by created_at"
    assert all(round(max(r["completed_at"]), 3) == t for r, t in zip(d["runs"], d["totals"]))
    n = len(d["runs"][0]["created_at"])
    dur, gap, sched, seq = [], [], [], []
    for r in d["runs"]:
        assert len(r["created_at"]) == n
        for i in range(n):
            dur.append(r["completed_at"][i] - r["started_at"][i])
            sched.append(r["started_at"][i] - r["created_at"][i])
            if i:
                gap.append(r["created_at"][i] - r["completed_at"][i - 1])
                assert r["created_at"][i] >= r["completed_at"][i-1] - 1e-9, "tasks overlap -> not sequential"
    print(f"   tasks/run: {n}")
    print(f"   per-task median duration (completed-started): {statistics.median(dur):.4f}s  "
          f"min {min(dur):.4f} max {max(dur):.4f}")
    print(f"   per-task median pending->running: {statistics.median(sched):.4f}s")
    print(f"   median gap between consecutive tasks: {statistics.median(gap):.4f}s")
    print(f"   median total: {statistics.median(d['totals']):.3f}s   "
          f"orchestration overhead (total - sum(durations)) median: "
          f"{statistics.median([t - sum(r['completed_at'][i]-r['started_at'][i] for i in range(n)) for r,t in zip(d['runs'],d['totals'])]):.3f}s")
    print(f"   notes length: {len(d['notes'])} chars")
