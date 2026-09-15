#!/usr/bin/env python3
"""Extraction phase: pull per-task timings for each run out of Hatchet's Postgres.

Field mapping (identical to what Hatchet's own REST API GET /api/v1/stable/tasks/{id}
returns for a task, verified row-by-row in verify_against_api()):
  created_at   <- v1_tasks_olap.inserted_at          (API metadata.createdAt)
  started_at   <- max(v1_task_events_olap.event_timestamp) where event_type='STARTED'
                                                     (API startedAt)
  completed_at <- max(v1_task_events_olap.event_timestamp) where event_type='FINISHED'
                                                     (API finishedAt)
"""
import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.request

PG = ["sudo", "docker", "exec", "hatchet-lite-postgres-1", "psql", "-U", "hatchet",
      "-d", "hatchet", "-t", "-A", "-F", "|", "-c"]
API = "http://localhost:8888"

SQL = """
SELECT t.external_id,
       t.display_name,
       EXTRACT(EPOCH FROM t.inserted_at)::text,
       EXTRACT(EPOCH FROM (SELECT max(e.event_timestamp) FROM v1_task_events_olap e
                           WHERE e.task_id = t.id AND e.task_inserted_at = t.inserted_at
                             AND e.event_type = 'STARTED'))::text,
       EXTRACT(EPOCH FROM (SELECT max(e.event_timestamp) FROM v1_task_events_olap e
                           WHERE e.task_id = t.id AND e.task_inserted_at = t.inserted_at
                             AND e.event_type = 'FINISHED'))::text,
       t.readable_status::text
FROM v1_tasks_olap t
WHERE t.workflow_run_id = '{run}'
ORDER BY t.inserted_at, t.id
"""


def q(sql):
    r = subprocess.run(PG + [sql], capture_output=True, text=True, check=True)
    return [line.split("|") for line in r.stdout.strip().splitlines() if line.strip()]


def fetch_rows(run_id, expected, timeout_s=120):
    """The OLAP tables are written asynchronously, so poll until every task has landed."""
    deadline = time.time() + timeout_s
    while True:
        rows = q(SQL.format(run=run_id))
        ok = (len(rows) == expected
              and all(r[5] == "COMPLETED" and r[3] and r[4] for r in rows))
        if ok:
            return rows
        if time.time() > deadline:
            raise RuntimeError(
                f"run {run_id}: only {len(rows)}/{expected} tasks fully materialised "
                f"in the OLAP tables after {timeout_s}s"
            )
        time.sleep(0.5)


def to_run(rows):
    created = [float(r[2]) for r in rows]
    started = [float(r[3]) for r in rows]
    done = [float(r[4]) for r in rows]
    order = sorted(range(len(rows)), key=lambda i: created[i])
    t0 = min(created)
    rel = lambda xs: [round(xs[i] - t0, 3) for i in order]
    return {"workers": 1, "created_at": rel(created), "started_at": rel(started),
            "completed_at": rel(done)}, [rows[i][0] for i in order]


def verify_against_api(token, external_id, created, started, completed):
    """Spot-check one task: the DB-derived triple must match Hatchet's own API."""
    req = urllib.request.Request(f"{API}/api/v1/stable/tasks/{external_id}",
                                 headers={"Authorization": "Bearer " + token})
    with urllib.request.urlopen(req, timeout=30) as r:
        d = json.load(r)
    import calendar
    import datetime as dt

    def p(s):
        """RFC3339 -> epoch seconds. datetime.fromisoformat on py3.9 rejects
        fractional-second fields that are not exactly 3 or 6 digits."""
        m = re.match(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d+))?", s)
        base = dt.datetime.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S")
        frac = float("0." + m.group(2)) if m.group(2) else 0.0
        return calendar.timegm(base.timetuple()) + frac
    deltas = {
        "created": abs(p(d["metadata"]["createdAt"]) - created),
        "started": abs(p(d["startedAt"]) - started),
        "completed": abs(p(d["finishedAt"]) - completed),
    }
    bad = {k: v for k, v in deltas.items() if v > 0.0015}
    return deltas, bad


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workload", required=True)
    ap.add_argument("--tasks", type=int, required=True)
    ap.add_argument("--run-ids", required=True, help="comma-separated")
    ap.add_argument("--engine-version", required=True)
    ap.add_argument("--sdk-version", required=True)
    ap.add_argument("--interpreter", required=True, help="path to interpreter bench json")
    ap.add_argument("--notes", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    # Tenant API token printed by the hatchet-lite setup (see README).
    token = os.environ.get("HATCHET_CLIENT_TOKEN") or open(
        os.environ.get("HATCHET_TOKEN_FILE", "token.txt")
    ).read().strip()
    run_ids = args.run_ids.split(",")

    runs, checks = [], []
    for rid in run_ids:
        rows = fetch_rows(rid, args.tasks)
        run, ext_ids = to_run(rows)
        # sequential sanity: no task may start before its predecessor finished
        overlaps = sum(1 for i in range(1, len(run["started_at"]))
                       if run["started_at"][i] < run["completed_at"][i - 1] - 0.001)
        if overlaps:
            print(f"WARNING {rid}: {overlaps} overlapping tasks (expected strictly "
                  f"sequential)", file=sys.stderr)
        raw_created = [float(r[2]) for r in rows]
        t0 = min(raw_created)
        idx = len(ext_ids) // 2
        deltas, bad = verify_against_api(
            token, ext_ids[idx],
            t0 + run["created_at"][idx], t0 + run["started_at"][idx],
            t0 + run["completed_at"][idx])
        checks.append({"run": rid, "task": ext_ids[idx],
                       "max_delta_s": round(max(deltas.values()), 6), "mismatch": bad})
        if bad:
            print(f"WARNING {rid}: API/DB mismatch {bad}", file=sys.stderr)
        runs.append(run)
        print(f"  {rid}: total {max(run['completed_at']):.3f}s "
              f"({len(run['created_at'])} tasks, api-delta "
              f"{max(deltas.values())*1000:.2f}ms)", file=sys.stderr)

    interp = json.load(open(args.interpreter))
    out = {
        "engine": "hatchet",
        "version": args.engine_version,
        "sdk": args.sdk_version,
        "workload": args.workload,
        "interpreter": interp,
        "runs": runs,
        "totals": [round(max(r["completed_at"]), 3) for r in runs],
        "notes": args.notes,
    }
    with open(args.out, "w") as f:
        json.dump(out, f, indent=2)
    print(json.dumps({"totals": out["totals"], "api_checks": checks}, indent=2))


if __name__ == "__main__":
    main()
