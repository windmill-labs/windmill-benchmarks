#!/usr/bin/env python3
"""Assemble prefect_runtime.json from the probes and the recorded settings."""
import json
import os
import statistics

S = os.path.dirname(os.path.abspath(__file__))

probes = json.load(open(f"{S}/probes.json"))
fib = probes["plain_process"]["fibo31_s"]
med = statistics.median(fib)

doc = {
    "engine": "prefect",
    "prefect_version": "3.8.6",
    "image": "prefecthq/prefect:3.8.6-python3.12",
    "server": "prefect server start, official image, against postgres:16",
    "host": {"instance": "m7i.8xlarge", "vcpu": 32, "memory_gib": 123,
             "cpu": "Intel(R) Xeon(R) Platinum 8488C", "region": "us-east-2",
             "os": "Ubuntu 24.04.5 LTS", "kernel": "7.0.0-1014-aws",
             "dedicated": True, "docker": "29.1.3", "compose": "2.40.3"},
    "python": probes["plain_process"]["python_version"],
    "python_version": probes["plain_process"]["python_version_short"],
    "python_implementation": probes["plain_process"]["implementation"],
    "fibo31_measurements": probes,
    "fibo31_median_s": med,
    "theoretical_ceiling_per_s": {
        "cpu": {"formula": "20 cores / fibo(31) compute time",
                "value": round(20 / med, 1), "compute_time_s": med,
                "sensitivity": "using the slowest body measurement (0.1249s) the "
                               "ceiling would be 160.1/s instead of 180.0/s"},
        "sleep": {"formula": "20 concurrent / 0.1s sleep", "value": 200.0,
                  "compute_time_s": 0.1},
    },
    "concurrency_mechanism_per_configuration": {
        "in_process": {
            "mechanism": "ThreadPoolTaskRunner(max_workers=20); 2000 @task .submit() "
                         "calls in one flow run, executed as threads of the flow process",
            "unit_isolation": "none - all 2000 units share one CPython process and its GIL",
            "dispatched": False,
            "is_prefect_default_path": True,
        },
        "work_pool": {
            "mechanism": "process work pool 'bench', pool concurrency limit 20, one "
                         "prefect worker; 2000 deployment runs, each executed as its "
                         "own subprocess",
            "unit_isolation": "one OS process per unit",
            "dispatched": True,
            "is_prefect_default_path": True,
        },
        "work_pool_tuned": {
            "mechanism": "same as work_pool with the worker poll loop at 0.1s",
            "unit_isolation": "one OS process per unit",
            "dispatched": True,
            "is_prefect_default_path": False,
        },
    },
    "settings_changed_from_default": [
        {"setting": "work pool concurrency limit (pool 'bench')", "default": "None (unlimited)",
         "used": "20", "applies_to": ["work_pool", "work_pool_tuned"],
         "why": "the experiment fixes concurrency at 20"},
        {"setting": "ThreadPoolTaskRunner max_workers", "default": "None, which resolves to sys.maxsize (one thread per submitted task)",
         "used": "20", "applies_to": ["in_process"],
         "why": "the experiment fixes concurrency at 20; Prefect's own default would spawn 2000 threads"},
        {"setting": "PREFECT_WORKER_QUERY_SECONDS", "default": "10.0",
         "used": "10.0 (unchanged)", "applies_to": ["work_pool"], "why": "stock measurement"},
        {"setting": "PREFECT_WORKER_QUERY_SECONDS", "default": "10.0",
         "used": "0.1", "applies_to": ["work_pool_tuned"],
         "why": "the 10s default caps the dispatch rate at one pool-worth (20 runs) per 10s"},
        {"setting": "run_deployment poll_interval", "default": "5",
         "used": "not used - run_deployment is never called; the 2000 runs are created "
                 "directly with PrefectClient.create_flow_run_from_deployment so that "
                 "submission is not the bottleneck",
         "applies_to": ["work_pool", "work_pool_tuned"],
         "why": "run_deployment blocks waiting for its own run, which would serialise submission"},
        {"setting": "@task cache_policy", "default": "DEFAULT (inputs + task source), inert unless results are persisted",
         "used": "None", "applies_to": ["in_process"],
         "why": "2000 identical calls must not be served from cache"},
        {"setting": "@task / @flow persist_result", "default": "False (PREFECT_RESULTS_PERSIST_BY_DEFAULT=False)",
         "used": "False (explicit, same as default)", "applies_to": ["in_process"], "why": "explicitness"},
        {"setting": "postgres max_connections", "default": "100",
         "used": "600", "applies_to": ["all"],
         "why": "20 concurrent subprocesses plus the server's pools exhaust 100"},
        {"setting": "postgres shared_buffers", "default": "128MB", "used": "2GB",
         "applies_to": ["all"], "why": "the box has 123 GiB and the default starves the server"},
    ],
    "settings_left_at_default": [
        {"setting": "PREFECT_WORKER_PREFETCH_SECONDS", "value": "10.0"},
        {"setting": "PREFECT_LOGGING_LEVEL", "value": "INFO"},
        {"setting": "PREFECT_LOGGING_TO_API_ENABLED", "value": "True"},
        {"setting": "worker --limit", "value": "unset; bounded by the pool's concurrency limit of 20"},
        {"setting": "task runner for the in_process configuration", "value": "ThreadPoolTaskRunner, Prefect's default"},
    ],
    "measurement": {
        "source": "Prefect's own database: task_run_state for in_process, "
                  "flow_run_state for the work pool configurations. No harness clock "
                  "is used for any reported figure.",
        "throughput": "jobs / (max COMPLETED timestamp - min RUNNING timestamp)",
        "latency": "COMPLETED timestamp - submission timestamp, p50 and p95",
        "execution": "COMPLETED timestamp - RUNNING timestamp, p50",
        "submission_anchor_in_process": "the uuid7 task run id minted by "
            "ThreadPoolTaskRunner.submit(); task_run.created is unusable because the "
            "server's task-run recorder inserts the row tens of seconds after the run",
        "submission_anchor_work_pool": "flow_run.created, written synchronously by "
            "create_flow_run_from_deployment",
        "cpu_accounting": "host-wide /proc/stat sampled once a second, sliced to each "
            "rep's measured window; for in_process also getrusage(RUSAGE_SELF) of the "
            "flow process itself",
        "reps": "1 warmup discarded, then 5 measured, per configuration and workload",
    },
}

drain = json.load(open(f"{S}/drain_analysis.json")) if os.path.exists(f"{S}/drain_analysis.json") else {}
gil = json.load(open(f"{S}/gil_finding.json")) if os.path.exists(f"{S}/gil_finding.json") else {}

doc["batch_sizes"] = {
    "in_process_sleep": 500, "in_process_cpu": 2000,
    "work_pool_sleep": 200, "work_pool_cpu": 200,
    "work_pool_tuned_sleep": 400, "work_pool_tuned_cpu": 400,
    "note": "Batch sizes differ per configuration. Throughput is reported as "
            "units/second in every case, so the figures are comparable, but the "
            "runs are not equal-sized. The poll-bound work-pool configurations "
            "run at a stable rate, so a smaller batch measures that rate in a "
            "fraction of the time; in_process sleep is capped at 500 by the "
            "event-broker finding below.",
}

doc["findings"] = {
    "event_broker_overflow": {
        "summary": "A single-node Prefect server silently loses its own task-run "
                   "records once in-process task throughput exceeds what the "
                   "task-run recorder can drain. Above that rate the server's "
                   "database is an incomplete record of what ran.",
        "mechanism": "In-process @task state changes are not written by synchronous "
                     "API calls. The task engine emits events, the server's "
                     "in-memory broker queues them, and a task_run_recorder service "
                     "consumes the queue and inserts task_run and task_run_state "
                     "rows. The broker's per-subscription queue is a hardcoded "
                     "asyncio.Queue(maxsize=10000) in "
                     "prefect/server/utilities/messaging/memory.py (line 113, "
                     "Prefect 3.8.6); there is no setting that raises it. When it "
                     "fills, the broker discards the message.",
        "log_line": "server-1  | 16:01:22.874 | WARNING | "
                    "prefect.server.utilities.messaging.memory - Subscription queue "
                    "is full, dropping message for topic='events' queue_size=10000 "
                    "retry_queue_size=0",
        "silent": "The only signal is that WARNING in the server's own log. The "
                  "client reports success, every flow run finishes COMPLETED, the "
                  "flow's futures all resolve, and nothing in the API or the UI "
                  "indicates that records are missing. A consumer reading "
                  "task_run / task_run_state sees fewer units than ran, with no "
                  "error.",
        "observed_loss": {
            "setup": "six back-to-back in_process sleep reps of 2000 @task units "
                     "each, 20-way, roughly 15s per rep, about 130 task/s",
            "per_rep_rows_recorded_of_2000": {
                "rep0": 2000, "rep1": 2000, "rep2": 1404,
                "rep3": 973, "rep4": 1023, "rep5": 1008},
            "per_rep_completed_recorded_of_2000": {
                "rep0": 2000, "rep1": 2000, "rep2": 1371,
                "rep3": 725, "rep4": 767, "rep5": 717},
            "total_task_runs_run": 12000,
            "total_task_runs_recorded": 9408,
            "total_completed_recorded": 7580,
            "fraction_of_completions_lost": 0.368,
            "dropped_messages_logged": 13139,
            "first_drop": "16:01:22.874, 13.9s into rep 2, i.e. after roughly 2.5 "
                          "reps (about 5000 task runs) had been produced",
            "note": "Rows that were recorded can also be incomplete: rep 3 recorded "
                    "973 task runs but only 725 of them with a COMPLETED state, so "
                    "both the run and its state history can go missing.",
        },
        "rate_at_which_dropping_starts": drain.get("conclusion",
            "not measured"),
        "drain_measurement": drain,
        "consequence_for_this_benchmark": "in_process sleep had to be measured at "
            "500 units per rep instead of 2000. At 500 all six reps recorded "
            "500/500 and no drop warnings were logged. in_process cpu runs at 8.4 "
            "units/s, two orders of magnitude below the drain limit, so its 2000-unit "
            "reps all recorded 2000/2000 and were kept.",
    },
    "gil_bound_in_process_cpu": gil,
}

json.dump(doc, open(f"{S}/prefect_runtime.json", "w"), indent=1)
print(json.dumps(doc["theoretical_ceiling_per_s"], indent=1))
print("wrote prefect_runtime.json")
