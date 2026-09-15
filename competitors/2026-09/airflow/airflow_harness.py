#!/usr/bin/env python3
"""Airflow side of the orchestrator benchmark.

Triggers a DAG of N sequential TaskFlow tasks (each computing fibo(k)) and emits
per-task-instance timings pulled from the Airflow metadata DB (task_instance),
in the same schema as the Windmill reference harness.
"""
import argparse
import json
import subprocess
import sys
import time
import uuid

PG = "airflow-postgres-1"
SCHED = "airflow-airflow-scheduler-1"
WORKER = "airflow-airflow-worker-1"


def psql(sql):
    out = subprocess.run(
        ["sudo", "docker", "exec", PG, "psql", "-U", "airflow", "-d", "airflow",
         "-t", "-A", "-F", "|", "-c", sql],
        capture_output=True, text=True, check=True,
    ).stdout.strip()
    return [r.split("|") for r in out.splitlines() if r.strip()]


def trigger(dag_id, run_id):
    r = subprocess.run(
        ["sudo", "docker", "exec", SCHED, "airflow", "dags", "trigger", dag_id, "--run-id", run_id],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        raise RuntimeError(f"trigger failed: {r.stdout[-2000:]} {r.stderr[-2000:]}")


def wait_for_run(dag_id, run_id, timeout_s=3600):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        rows = psql(
            f"SELECT state FROM dag_run WHERE dag_id='{dag_id}' AND run_id='{run_id}'"
        )
        if rows:
            state = rows[0][0]
            if state in ("success", "failed"):
                if state != "success":
                    raise RuntimeError(f"dag run {run_id} ended in state {state}")
                return
        time.sleep(2)
    raise TimeoutError(f"dag run {run_id} did not finish in {timeout_s}s")


def timings(dag_id, run_id, expected_tasks):
    rows = psql(
        "SELECT task_id, EXTRACT(EPOCH FROM queued_dttm)::text, "
        "EXTRACT(EPOCH FROM start_date)::text, EXTRACT(EPOCH FROM end_date)::text, "
        "state, try_number::text "
        f"FROM task_instance WHERE dag_id='{dag_id}' AND run_id='{run_id}' "
        "ORDER BY queued_dttm, start_date"
    )
    if len(rows) != expected_tasks:
        raise RuntimeError(f"{run_id}: expected {expected_tasks} task instances, got {len(rows)}")
    for r in rows:
        if r[4] != "success":
            raise RuntimeError(f"{run_id}: task {r[0]} state={r[4]}")
        if r[5] != "1":
            raise RuntimeError(f"{run_id}: task {r[0]} try_number={r[5]} (retried)")
        if not r[1]:
            raise RuntimeError(f"{run_id}: task {r[0]} has NULL queued_dttm")
    created = [float(r[1]) for r in rows]
    started = [float(r[2]) for r in rows]
    ended = [float(r[3]) for r in rows]
    t0 = created[0]
    return {
        "workers": 1,
        "created_at": [round(c - t0, 3) for c in created],
        "started_at": [round(s - t0, 3) for s in started],
        "completed_at": [round(e - t0, 3) for e in ended],
    }


def check_sequential(dag_id, run_id):
    """Confirm no two task instances overlapped (true sequential execution)."""
    rows = psql(
        "SELECT EXTRACT(EPOCH FROM start_date)::text, EXTRACT(EPOCH FROM end_date)::text "
        f"FROM task_instance WHERE dag_id='{dag_id}' AND run_id='{run_id}' ORDER BY start_date"
    )
    spans = [(float(a), float(b)) for a, b in rows]
    overlaps = sum(1 for i in range(1, len(spans)) if spans[i][0] < spans[i - 1][1] - 1e-9)
    return overlaps


def xcom_values(dag_id, run_id):
    rows = psql(
        "SELECT DISTINCT value::text FROM xcom "
        f"WHERE dag_id='{dag_id}' AND run_id='{run_id}'"
    )
    return sorted({r[0] for r in rows})


def interpreter_bench():
    """Measure raw CPython speed inside the worker container, where tasks execute."""
    code = (
        "import sys, time, json\n"
        "sys.setrecursionlimit(10000)\n"
        "def fibo(n):\n"
        "    return n if n <= 1 else fibo(n-1) + fibo(n-2)\n"
        "r33 = []\n"
        "for _ in range(3):\n"
        "    t = time.perf_counter(); v33 = fibo(33); r33.append(round(time.perf_counter()-t, 4))\n"
        "t = time.perf_counter(); v10 = fibo(10); r10 = round(time.perf_counter()-t, 6)\n"
        "print(json.dumps({'version': sys.version, 'fibo33_s': r33, 'fibo10_s': r10,\n"
        "                  'fibo33_value': v33, 'fibo10_value': v10}))\n"
    )
    out = subprocess.run(
        ["sudo", "docker", "exec", WORKER, "python", "-c", code],
        capture_output=True, text=True, check=True,
    ).stdout.strip()
    return json.loads(out.splitlines()[-1])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dag", required=True)
    ap.add_argument("--tasks", type=int, required=True)
    ap.add_argument("--workload", required=True)
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--warmup", type=int, default=1)
    ap.add_argument("--version", required=True)
    ap.add_argument("--executor", required=True)
    ap.add_argument("--notes", default="")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    interp = interpreter_bench()
    print(f"interpreter: {interp['version'].splitlines()[0]} "
          f"fibo33={interp['fibo33_s']} fibo10={interp['fibo10_s']}", file=sys.stderr)

    runs = []
    checks = []
    for i in range(args.warmup + args.reps):
        label = "warmup" if i < args.warmup else f"rep {i - args.warmup + 1}"
        run_id = f"bench_{uuid.uuid4().hex[:12]}"
        t_wall = time.time()
        trigger(args.dag, run_id)
        wait_for_run(args.dag, run_id)
        t = timings(args.dag, run_id, args.tasks)
        overlaps = check_sequential(args.dag, run_id)
        xs = xcom_values(args.dag, run_id)
        total = max(t["completed_at"])
        print(f"  {label}: {len(t['created_at'])} tasks, total {total:.3f}s, "
              f"overlaps={overlaps}, xcom={xs}, wall={time.time()-t_wall:.1f}s", file=sys.stderr)
        if i >= args.warmup:
            runs.append(t)
            checks.append({"run_id": run_id, "overlaps": overlaps, "xcom": xs})

    result = {
        "engine": "airflow",
        "version": args.version,
        "executor": args.executor,
        "workload": args.workload,
        "interpreter": {
            "version": interp["version"],
            "fibo33_s": interp["fibo33_s"],
            "fibo10_s": interp["fibo10_s"],
        },
        "runs": runs,
        "totals": [round(max(r["completed_at"]), 3) for r in runs],
        "notes": args.notes,
        "_checks": checks,
    }
    with open(args.out, "w") as f:
        json.dump(result, f, indent=2)
    print(f"wrote {args.out}: totals={result['totals']}", file=sys.stderr)


if __name__ == "__main__":
    main()
