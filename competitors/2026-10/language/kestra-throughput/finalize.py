#!/usr/bin/env python3
"""Build the two requested result files from the raw harness output."""
import json, os, statistics

SP = os.path.dirname(os.path.abspath(__file__))
L = lambda n: json.load(open(os.path.join(SP, n)))

raw = L("raw_docker_cpu.json")
ceil = L("ceiling.json")
probe = L("runtime_probe.json")["docker"]
host = L("hostinfo.json")
attr = L("attribution.json")

KEEP = ["jobs", "span_s", "throughput_per_s", "latency_p50_s", "latency_p95_s",
        "exec_p50_s", "exec_p95_s", "mean_concurrency", "peak_concurrency",
        "cores_busy", "seconds_above_20_concurrent", "submit_s", "n_exec", "cpu"]
reps = [{k: r[k] for k in KEEP if k in r} for r in raw]

tps = [r["throughput_per_s"] for r in reps]
med = round(statistics.median(tps), 3)

fib_k = round(statistics.mean(probe["fibo31_s"]), 4)                      # in Kestra's container
fib_b = round(statistics.mean(ceil["in_container"]["fibo31_serial_s"]), 5)  # bare, same image
par = {p["procs"]: p for p in ceil["in_container"]["parallel"]}
cpu_ceiling_measured = par[20]["calls_per_s"]
cpu_ceiling_naive = round(20 / fib_k, 2)
lifecycle = round(statistics.mean(ceil["serial_docker_run_fibo31_s"]), 4)
ceiling_lifecycle = round(20 / lifecycle, 2)

thr = {
 "engine": "kestra",
 "config": "docker runner",
 "workload": "cpu",
 "concurrency": 20,
 "units_per_rep": 500,
 "reps": reps,

 "batch_size_note": (
   "500 units per rep, not the 2000 used for the process-runner rows of this study. "
   "The Docker runner was already measured at 5.8 tasks/s on the sleeping workload, so 2000 units "
   "would be ~6 minutes of wall clock per rep. At 500 units a rep takes ~80s and the rate is flat "
   "across the span, so the smaller batch measures the same steady-state rate. "
   "Shape kept identical to the rest of the study: 25 executions x 20 independent Python script "
   "tasks in one io.kestra.plugin.core.flow.Parallel block."),

 "config_detail": (
   "Kestra's DEFAULT task runner for io.kestra.plugin.scripts.python.Script: the flow carries NO "
   "taskRunner block, only `containerImage: python:slim`, so io.kestra.plugin.scripts.runner.docker.Docker "
   "starts one python:slim container per task against the host Docker daemon over the mounted "
   "/var/run/docker.sock, and removes it afterwards."),

 "concurrency_mechanism": (
   "worker thread pool size, set with --worker-thread=20 on `kestra server standalone`; one Kestra "
   "worker thread runs one task at a time. The default is 8 x availableProcessors (256 on this box), "
   "so 20 is far below the default."),
 "is_default_concurrency": False,

 "median_throughput_per_s": med,
 "throughput_spread_per_s": [min(tps), max(tps)],
 "mean_occupancy_of_20": round(statistics.median(r["mean_concurrency"] for r in reps), 2),
 "median_cores_busy": round(statistics.median(r["cores_busy"] for r in reps), 2),

 "ceilings": {
  "cpu_20way_measured_per_s": cpu_ceiling_measured,
  "cpu_20way_measured_basis": (
    f"20 forked processes inside ONE python:slim container, each looping fibo(31) for 8s with Kestra "
    f"and per-task container start out of the picture: {cpu_ceiling_measured} calls/s. The box is "
    f"32 vCPU over 16 PHYSICAL cores, so this is well under the naive 20/{fib_k}s."),
  "cpu_20way_naive_per_s": cpu_ceiling_naive,
  "cpu_20way_naive_basis": f"20 slots / fibo(31) = {fib_k}s measured in Kestra's own python:slim container",
  "container_lifecycle_20way_per_s": ceiling_lifecycle,
  "container_lifecycle_basis": (
    f"`docker run --rm python:slim python fibo31.py` takes {lifecycle}s serially on this idle box, so "
    f"20 slots would retire {ceiling_lifecycle}/s if the Docker daemon parallelised container starts "
    f"perfectly. This is the ceiling that actually binds a container-per-task runner."),
  "share_of_cpu_ceiling": round(100 * med / cpu_ceiling_measured, 1),
  "share_of_naive_cpu_ceiling": round(100 * med / cpu_ceiling_naive, 1),
  "share_of_container_lifecycle_ceiling": round(100 * med / ceiling_lifecycle, 1),
 },

 "sanity": {
  "all_500_succeeded_every_rep": True,
  "how": ("the harness refuses a rep unless all 25 executions report SUCCESS and exactly 500 unit task "
          "runs each carry CREATED, RUNNING and SUCCESS timestamps"),
  "failures": 0,
  "jobs_per_rep": [r["jobs"] for r in reps],
  "peak_concurrency_per_rep": [r["peak_concurrency"] for r in reps],
  "seconds_above_20_concurrent_per_rep": [r["seconds_above_20_concurrent"] for r in reps],
  "mean_concurrency_per_rep": [r["mean_concurrency"] for r in reps],
  "concurrency_derivation": "interval sweep over every unit task run's RUNNING -> SUCCESS window",
 },

 "timing_source": ("every span, latency and concurrency figure is derived from Kestra's own "
                   "taskRunList[].state.histories CREATED / RUNNING / SUCCESS timestamps, never from "
                   "harness wall clock"),

 "little_law_check": {
  "statement": ("mean concurrency x per-task RUNNING->SUCCESS window should equal the throughput, and it "
                "does, which is what makes the container count the decisive number"),
  "mean_concurrency": round(statistics.median(r["mean_concurrency"] for r in reps), 2),
  "exec_p50_s": round(statistics.median(r["exec_p50_s"] for r in reps), 3),
  "containers_alive_p50": int(statistics.median(r["cpu"]["container_shims_p50"] for r in reps)),
  "reading": ("all 20 worker threads are occupied all the time, but only 6-8 containers exist at any "
              "instant, so ~two thirds of the occupied slots are waiting on the Docker daemon before "
              "their container is even created"),
 },

 "cpu_cost_per_task": {
  "core_seconds_per_task": [round(r["cores_busy"] / r["throughput_per_s"], 3) for r in reps],
  "median_core_seconds_per_task": round(statistics.median(
      r["cores_busy"] / r["throughput_per_s"] for r in reps), 3),
  "useful_core_seconds_per_task": fib_k,
  "useful_share_pct": round(100 * fib_k / statistics.median(
      r["cores_busy"] / r["throughput_per_s"] for r in reps), 1),
  "reading": ("Host busy-cores divided by throughput is near-constant across reps even though "
              "throughput itself varies 1.5x, so each completed task costs a fixed amount of host CPU. "
              "Only ~107ms of that is the fibo body; the rest is the container lifecycle. This is the "
              "cleanest statement of the overhead because it does not depend on attributing CPU to "
              "processes."),
 },

 "finding": (
   f"The Docker runner does not get real CPU parallelism. Median {med} tasks/s, against 5.78 tasks/s for "
   f"the same runner on a pure-sleep body: the spreads overlap (5.69-8.47 here, 5.71-6.43 there), so "
   f"adding ~{int(fib_k*1000)}ms of real CPU per task leaves the rate unchanged within run-to-run "
   f"variation. The rate is not set by CPU. Over the measured window the box averages "
   f"{round(statistics.median(r['cores_busy'] for r in reps), 2)} of 32 cores busy - nowhere near the 16 "
   f"physical cores twenty genuinely parallel containers would use, and barely above the 4.61 the "
   f"sleeping workload burned. Each completed task costs "
   f"{round(statistics.median(r['cores_busy'] / r['throughput_per_s'] for r in reps), 2)} core-seconds of "
   f"host CPU of which only {fib_k} is the fibo body, i.e. "
   f"{round(100 * fib_k / statistics.median(r['cores_busy'] / r['throughput_per_s'] for r in reps), 0):.0f}% "
   f"useful work. All 20 worker threads stay occupied (mean concurrency "
   f"{round(statistics.median(r['mean_concurrency'] for r in reps), 2)}/20) while only "
   f"{int(statistics.median(r['cpu']['container_shims_p50'] for r in reps))} containers exist at the "
   f"median instant, so roughly two thirds of the occupied slots are blocked inside the Docker daemon "
   f"rather than computing. The daemon bottlenecks it exactly as it did at 5.8 tasks/s on sleeping work."),

 "spread_caveat": (
   "The 5.69-8.47 tasks/s spread is wider than the 5.71-6.43 the sleeping workload showed at 2000 units "
   "per rep. Reps run in order 5.84, 5.69, 8.47, 6.96, 6.49, so it is not a monotonic drift; the likely "
   "cause is the 4x smaller batch, which gives Docker daemon state and ramp/drain proportionally more "
   "weight per rep. Host busy-cores track throughput almost exactly across the five reps "
   "(0.68-0.74 core-seconds per task throughout), so whatever moves the rate moves container churn with "
   "it, which is itself evidence that container handling and not CPU is the binding resource."),
}
json.dump(thr, open(os.path.join(SP, "kestra_docker_cpu_throughput.json"), "w"), indent=1)

rt = {
 "engine": "kestra",
 "kestra_version": host["kestra_version"],
 "kestra_commit_id": host["kestra_commit_id"],
 "kestra_commit_date": host["kestra_commit_date"],
 "image": "kestra/kestra:v2.0.2",
 "kestra_edition": "OSS",
 "run": host["run_date"],
 "experiment": ("throughput of Kestra's default Docker task runner on a core-bound body "
                "(naive recursive fibo(31)) at a fixed concurrency of 20"),

 "host": "AWS EC2 m7i.8xlarge (32 vCPU, 123 GiB, us-east-2), nothing else running",
 "host_cpu": host["host_cpu"],
 "docker_version": host["docker_version"],
 "deployment": ("official docker-compose (kestra/kestra:v2.0.2 `server standalone --worker-thread=20` "
                "+ postgres:18), postgres repository and postgres queue, basic auth enabled "
                "(2.x refuses the API without it), /var/run/docker.sock mounted into the Kestra container"),

 "concurrency": {
  "target": 20,
  "setting": "--worker-thread=20 on `kestra server standalone` (compose `command: server standalone --worker-thread=20`)",
  "mechanism": ("the worker's thread pool; one Kestra worker thread runs one task at a time, so the "
                "pool size is the task concurrency of a single worker"),
  "default": 256,
  "default_rule": "8 x availableProcessors; `--worker-thread --help` prints Default: 256 on this 32-vCPU box",
  "is_default": False,
  "relative_to_default": "20 is far BELOW the default of 256",
  "confirmation_logs": host["worker_logs"],
  "note_on_maxConcurrency_40": ("the worker/controller handshake advertises maxConcurrency=40 (2x the "
                                "thread count) as a job prefetch budget, not an execution limit; derived "
                                "concurrency never exceeded 20 in any rep"),
  "second_pool_unused": ("a SystemWorker starts with 32 threads in the same process and is not used by "
                         "these flows"),
  "other_controls_left_unset": ["flow-level `concurrency: {limit, behavior}`",
                                "io.kestra.plugin.core.flow.Parallel's own `concurrent` cap",
                                "Docker runner `cpu` / `memory` limits (none set, containers see all 32 vCPU)"],
 },

 "task_runner": {
  "configuration": ("DEFAULT: the flow declares no taskRunner block, only `containerImage: python:slim`, "
                    "so io.kestra.plugin.scripts.runner.docker.Docker is selected"),
  "class": "io.kestra.plugin.scripts.runner.docker.Docker",
  "per_task_work": ("creates a Docker volume and a python:slim container per task, runs the script inside "
                    "it, then removes both"),
  "image": "python:slim",
  "image_digest": host["image_digest"],
  "image_created": host["image_created"],
  "pulled_before_the_run": True,
  "command_kestra_runs": probe["kestra_log_cmd_lines"][0] if probe.get("kestra_log_cmd_lines") else None,
 },

 "container_python": {
  "measured_in": "a python:slim container started by Kestra's Docker task runner, through a Kestra flow",
  "interpreter": probe["implementation"],
  "version": probe["version"],
  "executable": probe["executable"],
  "cpu_count_visible": probe["cpu_count"],
  "sched_affinity": probe["sched_affinity"],
 },

 "fibo31_timings": {
  "body": "naive recursive fibo(31), the task body of the core-bound workload",
  "in_kestra_launched_container_s": probe["fibo31_s"],
  "in_kestra_launched_container_mean_s": fib_k,
  "bare_same_image_s": ceil["in_container"]["fibo31_serial_s"],
  "bare_same_image_mean_s": fib_b,
  "fibo10_s": probe["fibo10_s"],
  "note": ("python:slim ships CPython 3.14.8, which runs fibo(31) in ~107ms; the host venv Kestra's "
           "Process runner uses is CPython 3.12.3 at ~108ms, so the two runners' bodies cost the same CPU"),
 },

 "cpu_parallel_scaling_in_container": {
  "method": ("k forked processes inside ONE python:slim container, each looping fibo(31) for 8s; raw "
             "os.fork because CPython 3.14's multiprocessing defaults to forkserver, which does not come "
             "up in this container"),
  "curve": ceil["in_container"]["parallel"],
  "reading": (f"16, 20 and 32 processes all land at {par[16]['calls_per_s']}-{par[32]['calls_per_s']} "
              f"calls/s, i.e. SMT buys nothing for this body and the box behaves as 16 cores. "
              f"{cpu_ceiling_measured} calls/s is the CPU ceiling for a 20-slot pool, not "
              f"{cpu_ceiling_naive}."),
 },

 "serial_docker_baselines_idle_box": {
  "docker_run_true_s": ceil["serial_docker_run_true_s"],
  "docker_run_sleep100ms_s": ceil["serial_docker_run_sleep100ms_s"],
  "docker_run_fibo31_s": ceil["serial_docker_run_fibo31_s"],
  "docker_run_fibo31_mean_s": lifecycle,
  "reading": (f"one container lifecycle for this task body costs {lifecycle}s serially, of which only "
              f"{fib_b}s is the fibo work; the rest is container create / start / wait / remove"),
 },

 "cores_busy_method": (
   "Host-wide CPU utilisation sampled from /proc/stat once per second by a thread in the harness, which "
   "runs on the host alongside the containers. Each sample is the delta over the preceding second: "
   "busy_cores = (1 - (idle+iowait delta) / (total jiffies delta)) * 32. Samples are timestamped and then "
   "SLICED to the measured window, i.e. only samples whose covered second has its midpoint between the "
   "first unit task run's RUNNING and the last unit task run's SUCCESS (both from Kestra's own histories) "
   "are kept. cores_busy per rep is the mean of those samples. The figure is whole-box: Kestra JVM + "
   "Postgres + the Docker daemon and containerd + the task containers themselves. Alongside it, "
   "container_shims counts live containerd-shim processes in /proc at 1 Hz, which is how many containers "
   "are actually up at that instant."),

 "cpu_attribution_extra_rep": {
  "rep_throughput_per_s": attr["rep_throughput_per_s"],
  "rep_span_s": attr["rep_span_s"],
  "host_cores_busy": attr["host_cores_busy_whole_sampler_window"],
  "docker_daemon_cores": attr["docker_daemon_cores"],
  "kestra_jvm_cores": attr["kestra_jvm_cores"],
  "postgres_cores": attr["postgres_cores"],
  "task_containers_cores_direct": attr["task_containers_cores"],
  "containers_alive_p50": attr["shims_p50"],
  "containers_alive_max": attr["shims_max"],
  "real_fibo_work_cores": round(attr["rep_throughput_per_s"] * fib_k, 2),
  "method": attr["note"],
  "caveat": ("This attribution is PARTIAL and should be read only as a direction. A task container's "
             "python is a child of its containerd-shim, so its CPU lands in the shim's cutime and is "
             "folded into docker_daemon_cores rather than task_containers_cores; and the short-lived runc "
             "processes plus the kernel work of namespace, cgroup, overlayfs and veth setup are largely "
             "missed by a 2 Hz /proc sampler, which is why the attributed groups sum to roughly half the "
             "host total. The robust figure needs no attribution at all: throughput x fibo(31) time is "
             "the cores of real work, and it is a small fraction of the cores busy."),
  "reading": (f"During a {attr['rep_throughput_per_s']}/s rep the box had "
              f"{attr['host_cores_busy_whole_sampler_window']} of 32 cores busy while the task bodies' own "
              f"fibo work accounted for only {round(attr['rep_throughput_per_s'] * fib_k, 2)} cores. The "
              f"rest is container plumbing: dockerd alone sat at roughly 0.7-1.8 cores and only "
              f"{attr['shims_p50']} containers were alive at the median instant against 20 occupied worker "
              f"threads."),
 },

 "measured_median_throughput_per_s": med,
 "comparison_point_docker_sleep_per_s": 5.78,
}
json.dump(rt, open(os.path.join(SP, "kestra_docker_cpu_runtime.json"), "w"), indent=1)

print("median tp/s      ", med)
print("spread           ", min(tps), "-", max(tps))
print("mean occupancy   ", thr["mean_occupancy_of_20"], "/ 20")
print("cores busy       ", thr["median_cores_busy"], "/ 32")
print("shims p50        ", [r["cpu"]["container_shims_p50"] for r in reps])
print("ceilings         cpu20=%s naive=%s lifecycle=%s" % (cpu_ceiling_measured, cpu_ceiling_naive, ceiling_lifecycle))
print("share of ceiling cpu=%.1f%% lifecycle=%.1f%%" % (thr["ceilings"]["share_of_cpu_ceiling"], thr["ceilings"]["share_of_container_lifecycle_ceiling"]))
print("wrote kestra_docker_cpu_throughput.json, kestra_docker_cpu_runtime.json")
