#!/usr/bin/env python3
"""Host-side timing extraction for the Prefect benchmark.

Reads per-task-run state timestamps straight out of the Prefect server's Postgres
database (task_run_state.timestamp) and emits the benchmark JSON.
"""
import argparse
import json
import subprocess
import sys
import time

DB = "prefect-db"


def psql(sql):
    out = subprocess.run(
        ["sudo", "docker", "exec", DB, "psql", "-U", "prefect", "-d", "prefect",
         "-t", "-A", "-F", "|", "-c", sql],
        capture_output=True, text=True, check=True,
    ).stdout.strip()
    return [r.split("|") for r in out.splitlines() if r.strip()]


def reconciled(flow_run_id, expected):
    """True once the flow run and all its task runs are terminal and fully written."""
    rows = psql(
        "SELECT (SELECT state_type::text FROM flow_run WHERE id='%s'), "
        "(SELECT count(*) FROM task_run WHERE flow_run_id='%s'), "
        "(SELECT count(*) FROM task_run WHERE flow_run_id='%s' AND end_time IS NOT NULL "
        "AND state_type IN ('COMPLETED','FAILED','CRASHED','CANCELLED')), "
        "(SELECT count(*) FROM task_run_state s JOIN task_run tr ON tr.id=s.task_run_id "
        "WHERE tr.flow_run_id='%s')" % ((flow_run_id,) * 4)
    )
    st, n_tr, n_term, n_states = rows[0]
    return (st == "COMPLETED" and int(n_tr) == expected and int(n_term) == expected
            and int(n_states) >= expected * 3), (st, n_tr, n_term, n_states)


def wait_reconciled(flow_run_id, expected, timeout_s=300):
    deadline = time.time() + timeout_s
    stable = 0
    last = None
    while time.time() < deadline:
        ok, info = reconciled(flow_run_id, expected)
        if ok and info == last:
            stable += 1
            if stable >= 2:
                return
        else:
            stable = 1 if ok else 0
        last = info
        time.sleep(1.0)
    raise TimeoutError(f"flow run {flow_run_id} not reconciled in {timeout_s}s (last={last})")


def timings(flow_run_id, expected):
    """One row per task run: PENDING / RUNNING / terminal state timestamps.

    task_run_state.timestamp is the moment the state was created by the orchestrating
    engine; task_run.created is the (batched, lagging) server row-insert time and is
    deliberately not used.
    """
    sql = (
        "SELECT tr.id::text, "
        " EXTRACT(EPOCH FROM min(s.timestamp) FILTER (WHERE s.type='PENDING'))::text, "
        " EXTRACT(EPOCH FROM min(s.timestamp) FILTER (WHERE s.type='RUNNING'))::text, "
        " EXTRACT(EPOCH FROM max(s.timestamp) FILTER (WHERE s.type IN "
        "   ('COMPLETED','FAILED','CRASHED','CANCELLED')))::text, "
        " EXTRACT(EPOCH FROM tr.start_time)::text, EXTRACT(EPOCH FROM tr.end_time)::text, "
        " tr.state_type::text, tr.run_count::text, (tr.cache_key IS NOT NULL)::text "
        "FROM task_run tr JOIN task_run_state s ON s.task_run_id = tr.id "
        f"WHERE tr.flow_run_id = '{flow_run_id}' "
        "GROUP BY tr.id, tr.start_time, tr.end_time, tr.state_type, tr.run_count, tr.cache_key "
        "ORDER BY 2, 3"
    )
    rows = psql(sql)
    if len(rows) != expected:
        raise RuntimeError(f"{flow_run_id}: expected {expected} task runs, got {len(rows)}")
    for r in rows:
        if r[6] != "COMPLETED":
            raise RuntimeError(f"{flow_run_id}: task run {r[0]} in state {r[6]}")
        if r[7] != "1":
            raise RuntimeError(f"{flow_run_id}: task run {r[0]} ran {r[7]} times (retry?)")
        if r[8] not in ("f", "false"):
            raise RuntimeError(f"{flow_run_id}: task run {r[0]} has a cache key (cache hit?)")
        if not r[1] or not r[2] or not r[3]:
            raise RuntimeError(f"{flow_run_id}: task run {r[0]} missing a state timestamp: {r}")

    created = [float(r[1]) for r in rows]
    started = [float(r[2]) for r in rows]
    done = [float(r[3]) for r in rows]
    # cross-check state timestamps against the denormalised task_run columns
    drift = max(max(abs(float(r[4]) - s), abs(float(r[5]) - d))
                for r, s, d in zip(rows, started, done))
    t0 = min(created)
    return {
        "workers": 1,
        "created_at": [round(c - t0, 3) for c in created],
        "started_at": [round(s - t0, 3) for s in started],
        "completed_at": [round(d - t0, 3) for d in done],
    }, drift


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ids", required=True)
    ap.add_argument("--steps", type=int, required=True)
    ap.add_argument("--workload", required=True)
    ap.add_argument("--version", required=True)
    ap.add_argument("--notes-file", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    ids = json.load(open(args.ids))["flow_run_ids"]
    runs, drifts = [], []
    for fr in ids:
        wait_reconciled(fr, args.steps)
        t, drift = timings(fr, args.steps)
        drifts.append(drift)
        runs.append(t)
        print(f"  {fr}: {len(t['created_at'])} task runs, total {max(t['completed_at']):.3f}s "
              f"(start/end cross-check drift {drift * 1000:.3f} ms)", file=sys.stderr)

    result = {
        "engine": "prefect",
        "version": args.version,
        "workload": args.workload,
        "runs": runs,
        "totals": [round(max(r["completed_at"]), 3) for r in runs],
        "notes": open(args.notes_file).read().strip(),
    }
    with open(args.out, "w") as f:
        json.dump(result, f, indent=2)
    print(f"wrote {args.out}; totals={result['totals']}; max drift "
          f"{max(drifts) * 1000:.3f} ms", file=sys.stderr)


if __name__ == "__main__":
    main()
