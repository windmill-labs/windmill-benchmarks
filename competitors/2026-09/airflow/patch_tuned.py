import json, os, statistics as st

R = os.environ.get("RESULTS_DIR", "../results") + "/"

INTERP = {
    "version": "3.13.14 (main, Aug 12 2026, 08:57:09) [GCC 12.2.0]",
    "fibo33_s": [0.3859, 0.3792, 0.3993],
    "fibo10_s": 1.2e-05,
}

HEAD = (
    "Apache Airflow 3.3.1, same box and same official docker-compose as the default-config run "
    "(https://airflow.apache.org/docs/apache-airflow/3.3.1/docker-compose.yaml), on a dedicated "
    "m7i.xlarge (4 vCPU, 15 GiB, Amazon Linux 2023, us-east-2), all services on the one box: "
    "postgres 16, redis 7.2, api-server, scheduler, dag-processor, triggerer, one celery worker. "
    "Same DAG shape as the default run: TaskFlow API (`from airflow.sdk import dag, task`), {n} tasks "
    "chained strictly sequentially (t0 >> t1 >> ... >> t{last}), each computing fibo({k}) with the "
    "naive recursion."
)

CHANGED = (
    " EXACTLY TWO SETTINGS were changed from stock, applied as a docker-compose.override.yaml on the "
    "six running airflow services: (1) core.load_examples false, stock default in the official compose "
    "is true (AIRFLOW__CORE__LOAD_EXAMPLES: 'true'), which otherwise ships 115 bundled example DAGs "
    "that the dag-processor re-parses every 30s alongside the benchmark DAGs; (2) "
    "scheduler.scheduler_idle_sleep_time 0.1, stock Airflow default is 1 (one second). Nothing else "
    "was touched: core.executor=CeleryExecutor, core.parallelism=32, core.max_active_tasks_per_dag=16, "
    "core.max_active_runs_per_dag=16, celery.worker_concurrency=16, scheduler.scheduler_heartbeat_sec=5, "
    "scheduler.max_tis_per_query=16, scheduler.job_heartbeat_sec=5, "
    "dag_processor.min_file_process_interval=30 and dag_processor.parsing_processes=2 all remain at "
    "their shipped defaults. The metadata database was NOT reset; after the change the 115 example DAGs "
    "were marked is_stale=true and only the 2 benchmark DAGs remained in an active bundle."
)

TIMING = (
    " Timings come from the Airflow metadata database (task_instance table) via psql, not from "
    "wall-clock prints inside the tasks: created_at = queued_dttm, started_at = start_date, "
    "completed_at = end_date, shifted so the first task's queued_dttm is 0.0 and rounded to 3 decimals. "
    "queued_dttm was non-null for all {tis} measured task instances, so no fallback was needed. "
    "Validation identical to the default run: every task instance state=success with try_number=1, an "
    "overlap check (start_date[i] < end_date[i-1]) found 0 overlaps across all 5 runs, and the XCom "
    "return value was {xcom} for every task. 1 warmup DAG run preceded the 5 measured runs."
)

CAUSAL = (
    " The inter-task gap does track the knob causally, it is not merely correlated. Gap here means "
    "previous task's end_date to next task's queued_dttm. On the 40-task workload it moved from a "
    "median of 1.019s (p5 0.229, p95 1.038) at scheduler_idle_sleep_time=1 to a median of 0.125s "
    "(p5 0.111, p95 0.147) at 0.1, i.e. gap is approximately idle_sleep plus a fixed ~0.025s of actual "
    "scheduler loop work. The 10x knob change produced an 8.2x gap change precisely because that ~0.025s "
    "floor does not scale. On the 10-task fibo(33) workload the default gap distribution was wide and "
    "roughly uniform (median 0.637, p5 0.240, p95 0.886) rather than pinned near 1.0s, because a 0.46s "
    "task finishes at a random phase inside the 1s poll window; at 0.1 it collapses to median 0.159 "
    "(p95 0.186). As a control, the two components the knob should NOT affect did not move: dispatch "
    "(queued_dttm to start_date) stayed at a 0.019-0.020s median and in-task execution stayed at "
    "0.085-0.092s (fibo(10)) and 0.463-0.480s (fibo(33)) in both configurations."
)

INTERP_NOTE = (
    " The `interpreter` field was re-measured with examples disabled, via `docker exec` inside the "
    "airflow-worker container (the exact environment task instances execute in), CPython 3.13.14, a "
    "PGO+LTO build (--enable-optimizations, --with-lto), 2 warmup iterations discarded. Disabling the "
    "example DAGs did NOT meaningfully move the compute floor: min of 15 consecutive samples was 0.3735s "
    "with examples off versus 0.3635s with examples on, so ~0.37s is the fibo(33) compute cost per task "
    "in both configurations. The right tail (occasional samples to ~0.66s) persists with examples "
    "disabled, so it comes from Airflow's own background loops (scheduler polling, 5s heartbeats, 30s "
    "parse of the benchmark DAG file) on a 4-vCPU box, not from example-DAG parsing."
)


def patch(path, n, k, xcom, config, extra):
    d = json.load(open(path))
    d["config"] = config
    if config == "tuned":
        d["interpreter"] = INTERP
        d["notes"] = (
            HEAD.format(n=n, last=n - 1, k=k) + CHANGED
            + TIMING.format(tis=n * 5, xcom=xcom) + CAUSAL + extra + INTERP_NOTE
        )
    order = ["engine", "version", "executor", "config", "workload", "interpreter",
             "runs", "totals", "notes", "_checks"]
    out = {key: d[key] for key in order if key in d}
    json.dump(out, open(path, "w"), indent=2)
    print("%-44s config=%-8s totals=%s median=%.3f" % (path.split("/")[-1], config, out["totals"], st.median(out["totals"])))


patch(R + "airflow_tuned_40_10.json", 40, 10, "55", "tuned",
      " Result: median total 9.106s versus 40.453s for the same workload at stock defaults, i.e. 0.228s "
      "per task versus 1.011s. fibo(10) compute is ~8 microseconds, so this remains essentially pure "
      "orchestration overhead.")
patch(R + "airflow_tuned_10_33.json", 10, 33, "3524578", "tuned",
      " Result: median total 6.325s versus 10.965s at stock defaults, i.e. 0.633s per task versus "
      "1.097s. With ~0.37s of that being fibo(33) compute, orchestration overhead per task falls from "
      "~0.725s to ~0.26s.")
# label the default-config pair so the two columns are self-describing
patch(R + "airflow_40_10.json", 40, 10, "55", "default", "")
patch(R + "airflow_10_33.json", 10, 33, "3524578", "default", "")
