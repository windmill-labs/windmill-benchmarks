#!/usr/bin/env python3
import json, os, statistics
SP = os.path.dirname(os.path.abspath(__file__))
L = lambda n: json.load(open(os.path.join(SP, n)))
rp = L("runtime_probe.json")
smt = L("smt.json")
shape = L("shape.json")

mean = lambda xs: round(statistics.mean(xs), 4)

def tp(runner, workload):
    p = os.path.join(SP, f"raw_{runner}_{workload}.json")
    if not os.path.exists(p):
        return None
    return round(statistics.median(r["throughput_per_s"] for r in L(f"raw_{runner}_{workload}.json")), 2)

f31_proc = mean(rp["process"]["fibo31_s"])
f31_dock = mean(rp["docker"]["fibo31_s"])
par20 = next(x for x in smt["fibo31_under_parallelism"] if x["parallelism"] == 20)
cc_cpu = L("cpucheck2_process_cpu.json")
cc_slp = L("cpucheck2_process_sleep.json")

out = {
 "engine": "kestra",
 "kestra_version": "2.0.2",
 "kestra_edition": "OSS",
 "kestra_commit_id": "2ba6e90",
 "kestra_commit_date": "2026-09-15T13:59:52Z",
 "image": "kestra/kestra:v2.0.2",
 "experiment": "throughput at a fixed concurrency of 20, 2000 independent tasks per rep",
 "run": "2026-10-09",
 "host": "AWS EC2 m7i.8xlarge (32 vCPU, 123 GiB, us-east-2), nothing else running",
 "host_cpu": {
  "model": smt["topology"]["model"],
  "vcpus": smt["topology"]["vcpus"],
  "physical_cores": smt["topology"]["distinct_sibling_groups"],
  "threads_per_core": smt["topology"]["threads_per_core"],
  "why_it_matters": "32 vCPU is 16 physical cores with 2 SMT threads each, so 20 concurrent CPU-bound tasks do not get 20 independent cores"
 },
 "docker_version": "29.1.3",
 "deployment": "official docker-compose from https://kestra.io/docs/installation/docker-compose (kestra/kestra:v2.0.2 `server standalone` + postgres:18, postgres repository and postgres queue), with basic auth enabled because Kestra 2.x refuses API calls without it",

 "concurrency": {
  "target": 20,
  "setting": "--worker-thread=20 on `kestra server standalone` (compose `command: server standalone --worker-thread=20`)",
  "mechanism": "the worker's thread pool; one Kestra worker thread runs one task at a time, so the pool size is the task concurrency of a single worker",
  "default": 256,
  "default_rule": "`--worker-thread` help text: \"the number of worker threads, defaults to eight times the number of available processors\"; `kestra server standalone --help` prints `Default: 256` on this 32-vCPU box",
  "is_default": False,
  "relative_to_default": "20 is far BELOW the default of 256",
  "confirmation_logs": [
   "io.kestra.worker.AbstractWorker  Worker started with 20 thread(s)",
   "i.k.worker.fetchers.WorkerJobFetcher  Connected to controller: workerId=..., workerGroup=default, maxConcurrency=40, initialPermits=20"
  ],
  "note_on_maxConcurrency_40": "The worker/controller handshake advertises maxConcurrency=40 (2x the thread count) as a job prefetch budget, not an execution limit. Derived from task-run timestamps, concurrency never exceeded 20 in any rep of any combination: peak_concurrency=20 and seconds_above_20_concurrent=0.0 in all 15 measured reps.",
  "other_concurrency_controls_left_unset": [
   "flow-level `concurrency: {limit, behavior}` (per-flow execution cap)",
   "io.kestra.plugin.core.flow.Parallel `concurrent` (per-task-group cap), so unlimited",
   "a second in-process pool, the SystemWorker, starts with 32 threads and is not used by these flows"
  ]
 },

 "stock_settings_left_at_default": {
  "kestra.jdbc.queues.min-poll-interval": "25ms",
  "kestra.jdbc.queues.max-poll-interval": "500ms",
  "kestra.jdbc.queues.poll-switch-interval": "60s",
  "note": "read out of application.yml inside the kestra image; only --worker-thread was changed from stock, plus basic auth, which 2.x requires"
 },

 "shape": shape["shape"],
 "pre_enqueue": "All 100 executions of a rep are POSTed up front from a 24-thread pool (0.47-1.2s). Every one of the 2000 unit task runs reaches CREATED within 0.24-0.69s of the first RUNNING, i.e. inside the first 0.2-4.8% of the span, so submission is not the bottleneck and nothing is excluded from the throughput figure.",

 "runtimes": {
  "python_process": {
   "configuration": "io.kestra.plugin.core.runner.Process (primary configuration)",
   "measured_in": "the Kestra worker itself (kestra/kestra:v2.0.2, Ubuntu 24.04); each task is its own OS process",
   "interpreter": "CPython",
   "version": rp["process"]["version"],
   "executable": rp["process"]["executable"],
   "command_kestra_runs": "/bin/sh -c 'set -e; if [ -n \"$VIRTUAL_ENV\" ]; then \"$VIRTUAL_ENV/bin/python\" <script>.py; else \"python3.12\" <script>.py; fi'",
   "fibo31_s": rp["process"]["fibo31_s"],
   "fibo31_mean_s": f31_proc,
   "fibo10_s": rp["process"]["fibo10_s"],
   "cpu_count_visible": rp["process"]["cpu_count"],
   "sched_affinity": rp["process"]["sched_affinity"]
  },
  "python_docker": {
   "configuration": "default Docker task runner (io.kestra.plugin.scripts.runner.docker.Docker), containerImage python:slim (secondary data point, sleep workload only)",
   "measured_in": "a python:slim container, one started per task",
   "interpreter": "CPython",
   "version": rp["docker"]["version"],
   "executable": rp["docker"]["executable"],
   "command_kestra_runs": "/bin/sh -c 'set -e; ... \"python\" <script>.py ...' inside a fresh container",
   "fibo31_s": rp["docker"]["fibo31_s"],
   "fibo31_mean_s": f31_dock,
   "fibo10_s": rp["docker"]["fibo10_s"],
   "cpu_count_visible": rp["docker"]["cpu_count"],
   "sched_affinity": rp["docker"]["sched_affinity"]
  }
 },

 "theoretical_ceiling": {
  "formula": "20 concurrent slots / compute time of one task body",
  "sleep_per_s": 200.0,
  "sleep_basis": "the body is time.sleep(0.1), so a slot can retire 10 tasks/s and 20 slots 200/s",
  "cpu_naive_per_s": round(20 / f31_proc, 1),
  "cpu_naive_basis": f"20 / fibo(31) = {f31_proc}s, measured three times in the process runner's own interpreter on an idle box",
  "cpu_realistic_per_s": par20["aggregate_calls_per_s"],
  "cpu_realistic_basis": ("20 bare Python processes each looping fibo(31) on this box, no Kestra involved: "
    f"{par20['aggregate_calls_per_s']} calls/s aggregate, per-call p50 {par20['fibo31_p50_s']}s. "
    "The naive figure assumes 20 independent cores; the box has 16 physical cores, so 20-way CPU parallelism "
    "yields only this much. Use the realistic figure when judging the cpu workload."),
  "cpu_scaling_curve": smt["fibo31_under_parallelism"]
 },

 "measured_median_throughput_per_s": {
  "process_sleep": tp("process", "sleep"),
  "process_cpu": tp("process", "cpu"),
  "docker_sleep": tp("docker", "sleep")
 },

 "cpu_attribution": {
  "method": ("One extra, non-counted rep per workload. Short-lived task processes cannot be sampled directly, so the "
             "Kestra JVM's cutime+cstime is read instead: the kernel rolls a reaped child's CPU (and its own reaped "
             "descendants') into its parent, and every Process-runner task is a `/bin/sh -c python ...` child of the JVM. "
             "Cores are averages over the rep's wall time, which includes a brief idle head and tail."),
  "process_sleep": dict(cc_slp["cpu_attribution_cores"], throughput_per_s=cc_slp["rep"]["throughput_per_s"],
                        wall_s=cc_slp["wall_s"]),
  "process_cpu": dict(cc_cpu["cpu_attribution_cores"], throughput_per_s=cc_cpu["rep"]["throughput_per_s"],
                      wall_s=cc_cpu["wall_s"]),
  "reading": ("sleep: the task bodies burn no CPU, so the 3.8 cores of task-process CPU is pure per-task process and "
              "interpreter startup, and 5.2 cores go to the Kestra JVM plus Postgres - about 9 of 32 cores to retire "
              "137 sleeps/s. cpu: 17.3 cores land in task processes against an ideal "
              f"2000 x {f31_proc}s / span = ~10 cores of fibo work, so roughly 7 cores are per-task startup and SMT "
              "contention, with the JVM and Postgres adding ~4.5. The 20 worker slots were genuinely CPU-saturated; "
              "less than two thirds of that CPU was the workload.")
 },

 "serial_docker_baseline": {
  "note": "measured on this box with the benchmark idle, mean of 20 runs, for comparison with the Docker task runner's ~3.1-3.5s per-task window at concurrency 20",
  "docker_run_rm_python_slim_true_mean_ms": 380.9,
  "docker_run_rm_python_slim_true_p50_ms": 380.2,
  "docker_run_rm_python_slim_sleep_0_1_mean_ms": 490.4,
  "docker_run_rm_python_slim_sleep_0_1_p50_ms": 488.4,
  "docker_volume_create_plus_rm_mean_ms": 23.5
 },

 "execution_size_sensitivity": {
  "note": "Kestra keeps an execution, its full taskRunList and every state history in one row and rewrites it on each transition, so throughput depends on how many of the 2000 units are packed into a single execution. 20 units per execution was used for the measured runs.",
  "units_per_execution_sleep_process": {
   "20": "138.96 / 136.75 per_s (two runs)",
   "50": "135.81 / 133.04 per_s",
   "100": "132.44 / 101.70 per_s",
   "2000": "~2.75 per_s - abandoned after 155s with 426/2000 done and a ~890 KB execution document"
  }
 },

 "flow_shapes_in_2_0_2": {
  "io.kestra.plugin.core.flow.ForEach": "does not exist in 2.0.2 (Invalid type)",
  "io.kestra.plugin.core.flow.Loop": "exists, takes values + concurrencyLimit, but runs each iteration as a sub-execution that is NOT retained as a searchable execution: a 199-iteration Loop left one execution record with one `fanout` task run and no child task runs, so there are no per-unit timings to read. Unusable for this measurement.",
  "io.kestra.plugin.core.flow.Parallel": "creates one real task run per child inside the parent execution with CREATED / RUNNING / SUCCESS histories. This is the shape used."
 },

 "harness": "th.py (flow generation, submission, polling, timing extraction), kapi.py (REST client), cpu_check2.py (CPU attribution), smt.py (topology and parallel fibo). Timings come from each execution's taskRunList[].state.histories CREATED / RUNNING / SUCCESS, never from harness wall clock.",
 "harness_gotcha": "GET /executions/search in 2.0.2 silently ignores the flowId and labels query parameters, so polling is done per execution id."
}
json.dump(out, open(os.path.join(SP, "kestra_runtime.json"), "w"), indent=1)
print("wrote kestra_runtime.json")
