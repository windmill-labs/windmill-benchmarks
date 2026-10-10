# Orchestrator benchmark, 2026-10 runs

Four experiments, run on 2026-10-08 and 2026-10-09, that the
[2026-09 run](../2026-09) did not cover. That run measured one worker draining a chain of
tasks, which isolates the cost of dispatching a single task and is the configuration nobody
deploys. These runs answer the questions it could not:

| Experiment | Question | Engines |
|---|---|---|
| [`throughput/`](throughput) | At twenty concurrent slots, how much work does each engine retire, and what stops it retiring more? | Windmill, Hatchet, Temporal, Kestra, Airflow, Prefect |
| [`language/`](language) | How much of the per-task cost is the language runtime starting, rather than the engine? | Windmill, Hatchet, Temporal, Kestra |
| [`prefect-work-pool/`](prefect-work-pool) | What does Prefect cost when asked to dispatch a task, rather than call it in the flow process? | Prefect |

Everything is here: the compose files, the flow and workflow definitions, the drivers, the
extractors, the raw per-task timings, and the scripts that turn them into the published
tables. The layout and the standard follow the 2026-09 run.

```
python3 normalise.py    # reduce every engine's output to one canonical record
python3 validate.py     # invariant checks over the raw files
python3 aggregate.py    # print every table the docs and the blog quote
```

If a number appears in our documentation or in a blog post and `aggregate.py` does not print
it, it was not measured in these runs.

## The ceiling is 16 cores, not 20 slots

An `m7i.8xlarge` reports 32 vCPU, but that is **16 physical cores with two SMT threads each**
(Xeon Platinum 8488C). Twenty CPU-bound tasks cannot each have a core, so the naive ceiling of
`20 / task duration` is unreachable and quoting it would understate every engine.

Ceilings here are measured, not derived. With every engine stopped, 20 bare Python processes
looping `fibo(31)` retire 138.3 calls per second, and 16 processes retire 137.1: SMT is worth
about 1% on this workload. Two other engines' harnesses measured the same thing independently
and landed at 141.6 and 111.1 calls per second, the spread being the Python version in each
image. The CPU ceiling for each engine is therefore the effective core count divided by the
single-threaded `fibo(31)` time of its own image, which is why the percentages differ.

For a sleeping task there is no CPU contention and the ceiling is simply `20 / 0.1s` = 200 per
second for everyone.

## Method

Two workloads, deliberately different in what they stress:

- **a sleeping task**, `time.sleep(0.1)`, which holds a slot without needing a core. Engines
  reach concurrency in different ways, and twenty slots inside one interpreter genuinely serve
  twenty sleeping tasks because `time.sleep` releases the GIL.
- **a task that needs a core**, naive recursive `fibo(31)`, 107 to 135 ms of real CPU depending
  on the image. Here twenty slots in one interpreter serve **one core**, so each engine was
  given twenty slots by whatever route its own documentation names, and where that route is
  "more worker processes" or "a process pool executor", that is what was used.

2000 independent tasks per run, all enqueued before any becomes runnable so submission is never
the bottleneck. Smaller batches where a configuration is so slow that 2000 would take hours;
the batch size is recorded in every such file. One warmup discarded, then five measured runs.
Throughput is counted from the first task starting to the last completing, read from each
engine's own database or event history, never from a wall-clock timer in the harness.

## What each engine needed, and whether it was a default

Recorded per file in `concurrency_mechanism` and `is_default`. Summary:

| Engine | Sleeping workload | Core-bound workload |
|---|---|---|
| Windmill | 20 worker containers, a process per job | same |
| Hatchet | 1 worker x 20 slots (default is 100 slots) | 20 workers x 1 slot, since 20 slots in one CPython process is one core |
| Temporal | 1 worker, 20 activity slots | `ProcessPoolExecutor`, the route Temporal's own docs name for using more than one core |
| Kestra | `--worker-thread=20` (default is 8 x CPUs) | same |
| Airflow | Celery concurrency 20, plus `parallelism` and `max_active_tasks_per_dag` aligned so one binds | same, CeleryExecutor forks natively |
| Prefect | `ThreadPoolTaskRunner(max_workers=20)`, or pool concurrency 20 for the work pool | same; the in-process configuration is capped at one core and that is the finding |

## Caveats

**One machine per engine.** Every engine had an identical instance type to itself, but not the
same physical machine. Re-running the same container image on two instances moved `fibo(33)` by
12.5%, so a few percent between engines is not meaningful. Read each row against its own
ceiling, and treat the per-row changes in the language experiment, where both languages ran on
one box minutes apart, as the trustworthy comparisons.

**Prefect's server drops its own records above roughly 60 task runs per second.** In-process
task states are queued through a hardcoded `asyncio.Queue(maxsize=10000)` and written by a
recorder draining at 30 to 59 rows per second. Six back-to-back 2000-unit runs recorded 2000,
2000, 1404, 973, 1023 and 1008 rows. Nothing surfaces in the API or the UI; the only signal is a
warning in the server log. Prefect runs here were sized to stay below that rate. Evidence in
`throughput/prefect/drop_lines.sample.txt` and `drain_analysis.json`.

**Kestra 2.0.2 shapes.** `io.kestra.plugin.core.flow.ForEach` does not exist, and `Loop` runs
iterations as sub-executions that are not retained, so the flows use 100 executions of 20
parallel tasks. An execution and its whole task-run list live in one row rewritten on every
state transition, so 2000 units in a single execution collapses to 2.75 units per second.

**Hatchet timestamps.** `started_at` and `completed_at` are stamped by the SDK when it sends the
event, not when the body entered or left. On a sleeping body the error is about a millisecond.
On a core-bound body the SDK's own work shares the GIL with the task, so `exec_p50` reads 176 ms
for a 115 ms body.

**Windmill dedicated workers.** `duration_ms` covers assignment as well as execution, so
interval-overlap concurrency reads 82 rather than 20. Effective concurrency, throughput times
task duration, is 19.3 of 20. `validate.py` names this deviation rather than skipping it.

**Kestra's process-runner ceiling was revised.** An earlier harness measured the 20-way CPU
ceiling at 113.3 calls per second by spawning a process per call; looping in-process gives
141.6, which is the right figure for pure compute. The percentages here use the revised ceiling,
so Kestra's process runner reads 62% rather than the 78% an earlier draft quoted.

**Airflow's diagnostic run.** `raw_diag60_sleep.json` deliberately raises the caps from 20 to 60
to show that the 20-slot cap was not what bound Airflow. It is not one of the reported
configurations and `validate.py` names it.

## Results

Everything below is printed by `aggregate.py`.

```
THROUGHPUT AT TWENTY CONCURRENT SLOTS, 2026-10-09
  2000 independent 100 ms tasks, one machine per engine, median of 5 reps after a warmup
                                     sleep/s  ceil     cpu/s  ceil

  as shipped, nothing changed
  Windmill                             152.2   76%     106.5   77%
  Hatchet                              115.6   58%      93.8   66%
  Temporal                             112.5   56%      90.5   73%
  Airflow                               14.2    7%      10.2    8%
  Prefect, tasks in the flow process   160.3   80%       8.4    6%
  Kestra, Docker runner                  5.8    3%       6.5    5%

  after the settings each engine's own docs name
  Windmill, dedicated workers (EE)     193.3   97%     114.0   82%
  Temporal, poll limit raised          120.3   60%      91.2   74%
  Kestra, process runner               138.4   69%      87.8   62%
  Airflow, scheduler sleep 0.1s         16.8    8%      14.9   12%
  Prefect, work pool, polls lowered      3.5    2%       3.5    2%


WHAT THE LANGUAGE SAVES PER TASK, 2026-10-09
  the same 40 sequential tasks in each language, one machine per engine, as shipped
                                       python  typescript   change
  Windmill                              79.9ms       68.4ms   -11.5ms
  Temporal                              74.8ms       74.8ms    +0.0ms
  Hatchet                               33.4ms       33.8ms    +0.4ms
  Kestra, Docker runner                462.4ms      463.7ms    +1.3ms
  Kestra, process runner (configured)    97.9ms       82.4ms   -15.5ms


THE LANGUAGE AT TWENTY SLOTS, 2026-10-09
  sleeping task only: a naive fibo runs ~10x faster in Node, so a core-bound
  comparison across languages would measure the runtime, not the engine
  windmill     python  147.8/s   typescript  164.1/s    -13.4 ms per job
  hatchet      python  115.6/s   typescript  119.2/s     -5.2 ms per job


PREFECT THROUGH A PROCESS WORK POOL, 2026-10-08
  each unit a deployment run, so a job like the other engines dispatch
  work pool, polls lowered     fibonacci_10_33       2860.4 ms per task
  work pool, polls lowered     fibonacci_40_10       2498.5 ms per task
  work pool, stock             fibonacci_10_33       9605.6 ms per task
  work pool, stock             fibonacci_40_10       9791.7 ms per task
```
