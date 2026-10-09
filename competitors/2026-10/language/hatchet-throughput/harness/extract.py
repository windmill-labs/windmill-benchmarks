#!/usr/bin/env python3
"""Pull per-task timings for one repetition out of Hatchet's own Postgres.

Field mapping (same as the 2026-09 sequential run in this study):
  created_at   <- v1_tasks_olap.inserted_at
  started_at   <- max(v1_task_events_olap.event_timestamp) where event_type='STARTED'
  completed_at <- max(v1_task_events_olap.event_timestamp) where event_type='FINISHED'

Rows are selected by the `rep` key the harness writes into additional_metadata.
"""
import argparse
import json
import subprocess
import sys
import time

PG = ["docker", "exec", "hatchet-lite-postgres-1", "psql", "-U", "hatchet",
      "-d", "hatchet", "-t", "-A", "-F", "|", "-c"]

SQL = """
SELECT t.external_id,
       EXTRACT(EPOCH FROM t.inserted_at)::text,
       EXTRACT(EPOCH FROM (SELECT max(e.event_timestamp) FROM v1_task_events_olap e
                           WHERE e.task_id = t.id AND e.task_inserted_at = t.inserted_at
                             AND e.event_type = 'STARTED'))::text,
       EXTRACT(EPOCH FROM (SELECT max(e.event_timestamp) FROM v1_task_events_olap e
                           WHERE e.task_id = t.id AND e.task_inserted_at = t.inserted_at
                             AND e.event_type = 'FINISHED'))::text,
       t.readable_status::text,
       coalesce(t.latest_worker_id::text, ''),
       t.latest_retry_count::text
FROM v1_tasks_olap t
WHERE t.additional_metadata->>'rep' = '{tag}'
"""


def q(sql):
    r = subprocess.run(PG + [sql], capture_output=True, text=True, check=True)
    return [ln.split("|") for ln in r.stdout.strip().splitlines() if ln.strip()]


COUNT_SQL = """
SELECT count(*)::text,
       count(*) FILTER (WHERE readable_status = 'COMPLETED')::text,
       count(*) FILTER (WHERE readable_status NOT IN ('COMPLETED','QUEUED','RUNNING'))::text
FROM v1_tasks_olap WHERE additional_metadata->>'rep' = '{tag}'
"""


def fetch(tag, expected, timeout_s=900, poll_s=2.0):
    """Poll a cheap count query, then run the per-task query exactly once."""
    deadline = time.time() + timeout_s
    last = None
    while True:
        total, done, bad = (int(x) for x in q(COUNT_SQL.format(tag=tag))[0])
        if total == expected and done == expected:
            break
        if (total, done, bad) != last:
            print(f"    waiting: {total}/{expected} rows, {done} completed, {bad} other",
                  file=sys.stderr)
            last = (total, done, bad)
        if time.time() > deadline:
            raise RuntimeError(f"{tag}: {total} rows / {done} completed / {bad} other "
                               f"after {timeout_s}s (expected {expected})")
        time.sleep(poll_s)
    # the OLAP event rows can land a beat after the status flip
    for _ in range(60):
        rows = q(SQL.format(tag=tag))
        if len(rows) == expected and all(r[2] and r[3] for r in rows):
            return rows
        time.sleep(1.0)
    raise RuntimeError(f"{tag}: STARTED/FINISHED events never fully materialised")


def pct(xs, p):
    xs = sorted(xs)
    if not xs:
        return None
    k = (len(xs) - 1) * p
    lo, hi = int(k), min(int(k) + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def peak_concurrency(started, finished):
    ev = [(s, 1) for s in started] + [(f, -1) for f in finished]
    # a finish at the same instant as a start frees the slot first
    ev.sort(key=lambda x: (x[0], x[1]))
    cur = peak = 0
    for _, d in ev:
        cur += d
        peak = max(peak, cur)
    return peak


def worker_gaps(rows, started, finished):
    """For single-slot workers each timeline is strictly sequential, so the gap from
    one task finishing to the next starting on the same worker is the per-task
    scheduling cost that is not inside a task body."""
    by_worker = {}
    for i, r in enumerate(rows):
        by_worker.setdefault(r[5], []).append(i)
    gaps, overlaps = [], 0
    for idxs in by_worker.values():
        idxs.sort(key=lambda i: started[i])
        for a, b in zip(idxs, idxs[1:]):
            g = started[b] - finished[a]
            if g < -0.001:
                overlaps += 1
            else:
                gaps.append(g)
    return gaps, overlaps, len(by_worker)


def analyse(tag, rows, n, concurrency):
    created = [float(r[1]) for r in rows]
    started = [float(r[2]) for r in rows]
    finished = [float(r[3]) for r in rows]
    statuses = {r[4] for r in rows}
    retries = max(int(r[6]) for r in rows)
    workers = sorted({r[5] for r in rows if r[5]})

    span = max(finished) - min(started)
    span_enq = max(finished) - min(created)
    gaps, overlaps, nworkers = worker_gaps(rows, started, finished)
    lat = [finished[i] - created[i] for i in range(len(rows))]
    ex = [finished[i] - started[i] for i in range(len(rows))]
    wait = [started[i] - created[i] for i in range(len(rows))]
    return {
        "tag": tag,
        "jobs": len(rows),
        "span_s": round(span, 4),
        "throughput_per_s": round(len(rows) / span, 3),
        "span_from_enqueue_s": round(span_enq, 4),
        "throughput_from_enqueue_per_s": round(len(rows) / span_enq, 3),
        "latency_p50_s": round(pct(lat, 0.50), 4),
        "latency_p95_s": round(pct(lat, 0.95), 4),
        "exec_p50_s": round(pct(ex, 0.50), 4),
        "exec_p95_s": round(pct(ex, 0.95), 4),
        "queue_wait_p50_s": round(pct(wait, 0.50), 4),
        "enqueue_window_s": round(max(created) - min(created), 4),
        "first_start_after_last_enqueue_s": round(min(started) - max(created), 4),
        "peak_concurrent": peak_concurrency(started, finished),
        "slot_utilisation": round(sum(ex) / (span * concurrency), 4),
        "mean_slot_cycle_s": round(span * concurrency / len(rows), 4),
        "per_worker_gap_p50_s": round(pct(gaps, 0.50), 4) if gaps else None,
        "per_worker_gap_p95_s": round(pct(gaps, 0.95), 4) if gaps else None,
        "intra_worker_overlaps": overlaps,
        "distinct_workers": len(workers),
        "statuses": sorted(statuses),
        "max_retry_count": retries,
        "all_completed": statuses == {"COMPLETED"} and len(rows) == n and retries == 0,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", required=True)
    ap.add_argument("--n", type=int, default=2000)
    ap.add_argument("--concurrency", type=int, default=20)
    ap.add_argument("--raw-out")
    args = ap.parse_args()
    rows = fetch(args.tag, args.n)
    res = analyse(args.tag, rows, args.n, args.concurrency)
    if args.raw_out:
        t0 = min(float(r[1]) for r in rows)
        with open(args.raw_out, "w") as f:
            json.dump({"tag": args.tag, "t0_epoch": t0, "tasks": [
                {"id": r[0], "created": round(float(r[1]) - t0, 6),
                 "started": round(float(r[2]) - t0, 6),
                 "completed": round(float(r[3]) - t0, 6),
                 "worker": r[5]} for r in rows]}, f)
    print(json.dumps(res))


if __name__ == "__main__":
    main()
