# Kestra 2.0.2 throughput experiment - setup notes and dead ends

Box: AWS EC2 m7i.8xlarge (32 vCPU, 123 GiB, us-east-2), nothing else on it.
Deployment: official docker-compose (kestra/kestra:v2.0.2 `server standalone` + postgres:18),
postgres repository and postgres queue, basic auth enabled (2.x refuses the API without it).

## Concurrency mechanism

`--worker-thread=20` on `kestra server standalone`. One worker thread runs one task at a
time, so the pool size is the task concurrency of a single worker. Its default is
8 x availableProcessors, and `kestra server standalone --help` prints `Default: 256` on this
32-vCPU box, so 20 is far BELOW the default.

Startup logs confirm it:

    io.kestra.worker.AbstractWorker  Worker started with 20 thread(s)
    i.k.worker.fetchers.WorkerJobFetcher  Connected to controller: maxConcurrency=40, initialPermits=20

`maxConcurrency=40` (2x the thread count) is a job prefetch budget, not an execution limit:
derived from task-run timestamps, concurrency never exceeded 20 in any rep of any combination.

A second in-process pool, the SystemWorker, starts with 32 threads and is not used by these flows.
Left unset: flow-level `concurrency: {limit, behavior}`, and `Parallel`'s own `concurrent` cap.

## Workload shape - what works and what does not

`io.kestra.plugin.core.flow.ForEach` does not exist in 2.0.2 (confirms the earlier sequential run).

`io.kestra.plugin.core.flow.Loop` exists and takes `values` + `concurrencyLimit`, but in 2.0.2 it
runs each iteration as a SUB-EXECUTION, and those sub-executions are not retained as searchable
executions: a 199-iteration Loop left exactly one execution record behind, with one `fanout` task
run and no child task runs. There is therefore nothing to read per-unit timings out of, so Loop is
unusable for this measurement. (With a `Log` child and `{{ taskrun.value }}` the sub-executions also
all failed.)

`io.kestra.plugin.core.flow.Parallel` does create one real task run per child inside the parent
execution, with CREATED / RUNNING / SUCCESS histories. That is the shape used.

### 2000 units in ONE execution: does not work

An execution, its whole taskRunList and every state history live in one row that is rewritten on
each transition. With 2000 units in a single `Parallel` block the document reached ~890 KB at 2001
task runs and the run was still at 426/2000 SUCCESS after 155s (~2.75 units/s against 138/s for the
same work split up). Quadratic; abandoned.

### Units per execution sweep (process runner, sleep workload, 2000 units)

    20 per execution (100 executions):  138.96 / 136.75 units/s
    50 per execution ( 40 executions):  135.81 / 133.04 units/s
   100 per execution ( 20 executions):  132.44 / 101.70 units/s
  2000 per execution (  1 execution ):  ~2.75 units/s, abandoned

20 per execution was used for the measured runs.

## Pre-enqueue

All 100 executions of a rep are POSTed from a 24-thread pool before polling starts (0.47-0.79s).
Every one of the 2000 unit task runs reaches CREATED within 0.24-0.41s of the first RUNNING, i.e.
inside the first ~3% of the span, so nothing is excluded from the throughput figure.

## Harness bug worth remembering

`GET /executions/search` in 2.0.2 silently IGNORES the `flowId` and `labels` query parameters (and
returns an unrelated page), which made an early version of the harness measure stale executions from
a previous rep. Polling is done per execution id instead.

## Results (2000 units per rep, 1 warmup discarded + 5 measured reps)

    combo          median tp/s  spread         lat p50 s  lat p95 s  exec p50 s  ceiling/s                % ceiling
    process/sleep  138.44       118.81-139.88  7.05       13.94      0.136       200.0 (20/0.1s)          69.2%
    process/cpu     87.81        84.70-92.29  11.01      21.74      0.195       185.2 (20/fibo31 idle)    47.4%
    process/cpu     87.81        84.70-92.29  11.01      21.74      0.195       113.3 (20-way measured)   77.5%
    docker/sleep     5.78         5.71-6.43  172.07     330.00      3.487       200.0 (20/0.1s)            2.9%

Every rep: 2000/2000 task runs SUCCESS, peak concurrency exactly 20, 0.0s ever above 20.
mean concurrency 16.2-19.4 (process) and 19.94 (docker).

## The 20-core ceiling is not 20 cores

m7i.8xlarge is 32 vCPU over 16 physical cores (Xeon Platinum 8488C, 2 SMT threads per core).
20 bare Python processes looping fibo(31) with Kestra out of the picture retire 113.3 calls/s,
not the 185/s that 20 / 0.108s suggests. Both ceilings are reported.

## CPU attribution (one extra non-counted rep, JVM cutime+cstime for task processes)

    sleep workload, 137 units/s:  host 9.2 cores busy = 3.8 task processes + 2.7 JVM + 2.5 Postgres
    cpu   workload,  93 units/s:  host 22.2 cores busy = 17.3 task processes + 2.3 JVM + 2.2 Postgres

On the sleep workload the task bodies burn no CPU at all, so the 3.8 cores of task-process CPU is
pure per-task process and interpreter startup, and 5.2 cores are Kestra and Postgres.
On the cpu workload the ideal fibo work is 2000 x 0.108s / 21.6s span = ~10 cores, so of the 17.3
cores landing in task processes roughly 7 are startup and SMT contention.
The 20 worker slots were genuinely CPU-saturated; under two thirds of that CPU was the workload.

## Docker task runner

Serial baseline on the same idle box: `docker run --rm python:slim true` 380.9ms,
with the 0.1s sleep 490.4ms, `docker volume create` + `rm` 23.5ms.
At concurrency 20 the per-task RUNNING -> SUCCESS window is 3.13-3.53s, 6-7x the serial lifecycle:
the single Docker daemon is the bottleneck, not container start as such. Concurrency stays at
exactly 20 with mean 19.94, i.e. all 20 worker threads sit blocked on the daemon.
