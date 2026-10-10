# Orchestrator benchmark, 2026-09 run

Six engines - Windmill, Hatchet, Temporal, Prefect, Airflow, Kestra - on the same two
workloads, the same instance type, and one engine per box. Run on 2026-09-15.

This directory is the whole run: the compose files, the flow/DAG/workflow definitions, the
runner and extractor scripts, and the raw per-task timings every number below comes from.
The 2025 material in [`../`](../) is untouched and still describes how that run was done; it
was measured on a t2.medium with engine versions that are now several majors old.

The 2025 README says assembling the results was "a very manual task", and that is why its
numbers went stale without anyone noticing. Everything here is scripted end to end: each
engine has one command per workload that writes a JSON file into [`results/`](results), and
two scripts turn those files into the tables below. If you follow this README you should get
these numbers, or find out exactly where you diverge.

```
python3 validate.py     # invariant checks on all 23 run files
python3 aggregate.py    # prints the two results tables below
```

## The two workloads

Both are a single workflow of sequential tasks, each task computing a naive recursive
Fibonacci in Python. They are the same two shapes the 2025 run used:

```python
def fibo(n):
    return n if n <= 1 else fibo(n - 1) + fibo(n - 2)
```

| workload | shape | what it measures |
|---|---|---|
| `fibonacci_40_10` | 40 sequential tasks, each `fibo(10)` | per-task orchestration overhead. `fibo(10)` is ~8 microseconds, so the total is essentially all orchestrator. |
| `fibonacci_10_33` | 10 sequential tasks, each `fibo(33)` | overhead on top of real compute. `fibo(33)` is ~0.33-0.39s depending on the CPython build, so ~3.3-3.9s of the total is work. |

Sequential on purpose: the question is what it costs an engine to hand one task to the next,
which is the cost a DAG of any shape pays on its critical path. Nothing here measures
throughput, parallel fan-out, or scheduling under contention.

## Hardware

One AWS EC2 **m7i.xlarge** (4 vCPU, 16 GiB) per engine, all in **us-east-2**, Amazon Linux
2023, Docker 25 with compose 2.39.2. Each box ran exactly one engine and nothing else: no
two engines ever shared a CPU, and no box ran a second engine before or after. Every
component of an engine (server, database, queue, worker, and the harness process itself)
ran on that one box, so no number here includes a network hop between availability zones.

CPU was ~98% idle during the measured runs on every box except Airflow's, where the 115
bundled example DAGs the official compose ships keep the dag-processor busy (see
[Default vs tuned](#default-vs-tuned)).

## Versions

| engine | version | image / package | task runtime |
|---|---|---|---|
| Windmill | 1.811.1 (CE and EE) | `ghcr.io/windmill-labs/windmill:1.811.1`, Postgres 18 | CPython 3.13.5, in the worker container |
| Hatchet | v0.107.0 | `ghcr.io/hatchet-dev/hatchet/hatchet-lite@sha256:db8fb8e009a5c892859e52e4aa50b17aa11ab1dd23801cd320492f2942db30d7`, Postgres 15.6, `hatchet-sdk` 1.40.1 | CPython 3.12.14, `python:3.12-slim` worker image |
| Temporal | 1.29.7 | `temporalio/auto-setup:1.29.7`, Postgres 16, admin-tools 1.29.1, UI 2.34.0, `temporalio` SDK 1.33.0 | CPython 3.11.16, host Python |
| Prefect | 3.8.6 | `prefecthq/prefect:3.8.6-python3.12`, Postgres 16 | CPython 3.12.14, in the client container |
| Airflow | 3.3.1 | official 3.3.1 compose, Postgres 16, Redis 7.2, CeleryExecutor | CPython 3.13.14, in the celery worker container |
| Kestra | 2.0.2 (OSS, standalone) | `kestra/kestra` (`latest`, resolved to 2.0.2), Postgres 18 | CPython 3.13.15 in `python:3.13-slim` (Docker runner) / 3.12.3 in the Kestra image (Process runner) |

Temporal server is 1.29.7 because `temporalio/auto-setup` publishes no image past 1.29.x;
1.30-1.32 exist but are not reachable through the official docker-compose path.

The interpreter each engine actually runs matters for `fibonacci_10_33`: CPython 3.13 is
about 24% slower than 3.12 on this recursion. Every result file carries an `interpreter`
field measured inside the exact container or process the tasks execute in, using the snippet
in [`hatchet/app/interpreter_bench.py`](hatchet/app/interpreter_bench.py) (`fibo(33)` three
times, then `fibo(10)`, `time.perf_counter`, warmed first). That is why the long workload is
reported as overhead *above measured compute* rather than as a raw total.

## Results

Median of 5 measured runs, after one discarded warmup. `per task` is median total / task
count. `overhead/task` subtracts the measured `fibo(33)` compute for `fibonacci_10_33`; on
`fibonacci_40_10` compute is ~8 microseconds so the two columns coincide. Rows marked
`_tuned`, `_process`, `_dedicated` and `_script` are not default configurations - see below.

These tables are the output of `python3 aggregate.py`, with Hatchet's image digest
abbreviated.

### fibonacci_40_10

| engine | version | median total | spread | per task | overhead/task | a task is | python |
|---|---|---|---:|---:|---:|---|---|
| prefect | 3.8.6 | 0.137s | 0.125-0.141 | 3.4ms | 3.4ms | in the flow process | 3.12.14 |
| windmill_dedicated | 1.811.1 EE | 0.393s | 0.375-0.415 | 9.8ms | 9.8ms | warm process, one script | 3.13.5 |
| temporal_tuned | 1.29.7 | 0.76s | 0.749-0.773 | 19.0ms | 19.0ms | in the worker process | 3.11.16 |
| hatchet | v0.107.0 | 1.237s | 1.113-1.339 | 30.9ms | 30.9ms | in the worker process | 3.12.14 |
| windmill_script | 1.811.1 EE | 1.504s | 1.428-1.535 | 37.6ms | 37.6ms | process per task | 3.13.5 |
| windmill_tuned | 1.811.1 | 1.594s | 1.583-1.601 | 39.9ms | 39.9ms | process per task | 3.13.5 |
| temporal | 1.29.7 | 2.995s | 2.994-2.996 | 74.9ms | 74.9ms | in the worker process | 3.11.16 |
| windmill | 1.811.1 | 3.443s | 3.347-3.447 | 86.1ms | 86.1ms | process per task | 3.13.5 |
| kestra_process | 2.0.2 | 3.473s | 3.405-3.609 | 86.8ms | 86.8ms | process per task | 3.12.3 |
| airflow_tuned | 3.3.1 | 9.106s | 8.844-9.988 | 227.6ms | 227.6ms | process per task | 3.13.14 |
| kestra | 2.0.2 | 27.008s | 26.653-27.162 | 675.2ms | 675.2ms | container per task | 3.13.15 |
| airflow | 3.3.1 | 40.453s | 39.609-41.801 | 1011.3ms | 1011.3ms | process per task | 3.13.14 |

### fibonacci_10_33

| engine | version | median total | spread | per task | overhead/task | a task is | python |
|---|---|---|---:|---:|---:|---|---|
| prefect | 3.8.6 | 2.997s | 2.965-3.024 | 299.7ms | 16.3ms | in the flow process | 3.12.14 |
| temporal | 1.29.7 | 3.468s | 3.455-3.491 | 346.8ms | 11.8ms | in the worker process | 3.11.16 |
| hatchet | v0.107.0 | 3.518s | 3.510-3.534 | 351.8ms | 35.8ms | in the worker process | 3.12.14 |
| windmill_dedicated | 1.811.1 EE | 3.857s | 3.844-3.900 | 385.7ms | 31.8ms | warm process, one script | 3.13.5 |
| kestra_process | 2.0.2 | 4.006s | 3.971-4.156 | 400.6ms | 89.6ms | process per task | 3.12.3 |
| windmill_tuned | 1.811.1 | 4.133s | 4.109-4.151 | 413.3ms | 59.4ms | process per task | 3.13.5 |
| windmill_script | 1.811.1 EE | 4.16s | 4.145-4.180 | 416.0ms | 62.1ms | process per task | 3.13.5 |
| windmill | 1.811.1 | 4.544s | 4.523-4.562 | 454.4ms | 100.5ms | process per task | 3.13.5 |
| airflow_tuned | 3.3.1 | 6.325s | 5.954-7.009 | 632.5ms | 246.6ms | process per task | 3.13.14 |
| kestra | 2.0.2 | 10.104s | 10.025-10.303 | 1010.4ms | 659.4ms | container per task | 3.13.15 |
| airflow | 3.3.1 | 10.965s | 9.633-11.361 | 1096.5ms | 725.4ms | process per task | 3.13.14 |

The `a task is` column is the one that makes totals incomparable across families, so read it
before the numbers. An engine that calls a function in the flow process is not doing the
same work as one that starts a process, and neither is doing what one that starts a
container is doing. What is comparable within a row family is the overhead, and what is
comparable across all of them is what you get for it: isolation, a per-task record, and
per-task retry.

In particular, Prefect's 3.4ms is not an orchestration cost that can be set against the
others. Prefect 3 tasks are function calls inside the flow process: no job record dispatched
to a worker, no isolation, no per-task retry dispatch. The state writes that back them are
asynchronous and batched.

## Default vs tuned

Every engine ships a default that dominates its result, and in four cases the default is not
what anyone would run in production. Both columns are published, and each `_tuned` row
differs from its default row by exactly one knob:

| row | knob | default | tuned | effect |
|---|---|---|---|---|
| `airflow_tuned` | `scheduler.scheduler_idle_sleep_time` + `core.load_examples` | `1` and `true` (115 example DAGs re-parsed every 30s) | `0.1` and `false` | median inter-task gap 1.019s -> 0.125s |
| `temporal_tuned` | `history.transferProcessorMaxPollRPS` | `20` | `1000` | 2.995s -> 0.760s on the 40-task workload |
| `kestra_process` | `taskRunner` on `io.kestra.plugin.scripts.python.Script` | Docker runner, one container per task | `io.kestra.plugin.core.runner.Process` | 27.0s -> 3.47s |
| `windmill_tuned` | `SLEEP_QUEUE` | `50` (ms between queue polls) | `5` | 3.443s -> 1.594s |

Two of these deserve the detail:

Temporal's stock compose leaves `history.transferProcessorMaxPollRPS` at 20, the per-shard
rate at which the history transfer queue processor loads tasks. Each sequential activity
costs two transfer tasks (the activity task, then the workflow task after it completes), so
a workflow issuing activities faster than ~10/s is throttled once the limiter's burst is
spent. It is visible in the raw data: the first ~13 activities take ~19ms each, then every
one costs ~100ms. Raising the limit removes the effect entirely and the same workload runs
at ~19ms/activity throughout. The dynamic config was reverted to stock afterwards.

Airflow's gap tracks its knob causally, not just correlationally. At
`scheduler_idle_sleep_time=1` the median gap from one task ending to the next being queued
is 1.019s (p5 0.229, p95 1.038); at 0.1 it is 0.125s (p5 0.111, p95 0.147) - i.e. the gap is
the idle sleep plus a fixed ~0.025s of scheduler loop work, which is why a 10x knob change
gives an 8.2x gap change. The two components the knob should not touch did not move:
dispatch stayed at a 0.019-0.020s median and in-task execution at 0.085-0.092s.

The two Windmill EE rows are a pair, not a tuning knob. Dedicated workers bind a warm
process to one deployed script, so the flow has to call a script by path rather than inline
code - that is a second change on top of "dedicated". `windmill_script` is the control: the
same flow, the same deployed script, run on the normal worker. The only difference between
the two rows is which worker group executes the steps, which is what isolates the dedicated
worker's effect (37.6ms -> 9.8ms per task).

## How timings are extracted

No number in this directory is a wall-clock measurement taken in the harness. Every engine
is asked for its own record of when each task was created, started and finished, so what is
compared is what each engine itself believes it did:

| engine | created_at | started_at | completed_at | source |
|---|---|---|---|---|
| Windmill | `created_at` | `started_at` | `started_at + duration_ms` | `v2_as_completed_job`, children of the flow job |
| Airflow | `queued_dttm` | `start_date` | `end_date` | `task_instance` in the metadata DB |
| Temporal | `ActivityTaskScheduled` | `ActivityTaskStarted` | `ActivityTaskCompleted` | workflow event history, server-assigned timestamps |
| Prefect | `PENDING` state | `RUNNING` state | terminal state | `task_run_state.timestamp` |
| Kestra | `CREATED` history entry | `RUNNING` | `SUCCESS` | `taskRunList[].state.histories` from the REST API |
| Hatchet | `v1_tasks_olap.inserted_at` | `STARTED` event | `FINISHED` event | `v1_task_events_olap` |

Each file in `results/` stores, per run, `created_at` / `started_at` / `completed_at` arrays
in seconds relative to the first task's creation, plus a `notes` field recording the exact
semantics and every caveat found while measuring that engine. Read the notes before quoting
a number.

Four extraction details worth knowing, all recorded in the relevant `notes`:

- Prefect's `task_run.created` column is a batched server-side insert time that lags the
  real orchestration by 40-130ms; `task_run_state.timestamp` is the moment the engine
  created the state, and is what is used.
- Temporal does not persist `ActivityTaskStarted` at dispatch time - it flushes it with the
  terminal event - so history is fetched after the workflow completes. The timestamp is
  still the real dispatch time.
- Hatchet materialises a DAG lazily: a downstream task row is inserted only once its parent
  finishes, so `created_at` for task i>0 tracks the critical path rather than a submit time.
  It is the closest true queued timestamp Hatchet has.
- Hatchet's SDK result listener resolves on a ~1s grid, so client wall-clock is useless for
  this workload (a flat ~2.13s regardless of the real duration). Another reason not to time
  from the harness.

## Validation

`python3 validate.py` re-runs these checks over every file in `results/`:

- the task count per run is the expected one (40 or 10)
- `created_at[0] == 0` and tasks are ordered by creation
- no two tasks overlap - `started_at[i] >= completed_at[i-1]` - so the workload really ran
  sequentially and nothing was silently parallelised
- the reported total equals `max(completed_at)`

The harnesses additionally assert, at collection time, what each engine can tell them: every
task succeeded on attempt 1 (no retries), no cache hits (Prefect), no `ActivityTaskFailed` /
`TimedOut` events (Temporal), the XCom return value is the correct Fibonacci number for every
task (Airflow: `55` and `3524578`), and one task per Hatchet run is cross-checked against
Hatchet's own REST API (agrees within 0.5ms). Prefect runs are additionally polled until all
task runs are terminal, `end_time` is set on each, and the counts are stable across two
consecutive polls, so nothing is read mid-flush.

## Reproducing a row

Every engine follows the same shape: bring up the compose file, run the workload, write a
JSON into `results/`. Run one engine per box. Commands below assume you are in this
directory's engine subfolder; `../results` is where files land.

Credentials are never in these files. Each engine has a `.env.example` to copy and fill; the
`.gitignore` here keeps the filled-in copies out of git.

### Windmill

```bash
cd windmill
cp .env.example .env                       # set POSTGRES_PASSWORD and DATABASE_URL
docker compose -f docker-compose.yml -f docker-compose.override.yml up -d
export WM_TOKEN=...                        # a token for the `admins` workspace
python3 wm_bench.py --steps 40 --fib 10 --warmup 1 --reps 5 --out ../results/windmill_40_10.json
python3 wm_bench.py --steps 10 --fib 33 --warmup 1 --reps 5 --out ../results/windmill_10_33.json
```

`docker-compose.yml` is Windmill's published compose; the override runs exactly one worker
and scales the native worker, the indexer and the extra services to 0. Swap in
`docker-compose.override.tuned.yml` for the `windmill_tuned` rows (the only difference is
`SLEEP_QUEUE=5`).

The two EE rows need `WM_IMAGE=ghcr.io/windmill-labs/windmill-ee:1.811.1`, a `LICENSE_KEY`
in `.env`, and `docker-compose.override.dedicated.yml`, which adds a second worker in the
`dedicated` worker group:

```bash
python3 wm_dedicated_bench.py --steps 40 --fib 10 --mode dedicated --out ../results/windmill_dedicated_40_10.json
python3 wm_dedicated_bench.py --steps 40 --fib 10 --mode control   --out ../results/windmill_script_40_10.json
```

`wm_bench.py` emits `{steps, fib, runs, totals}`. The `engine` / `version` / `interpreter` /
`notes` fields in `results/windmill_*.json` were added after the run; the `interpreter` figure
comes from running the probe snippet with `docker exec` inside the worker container, which is
where steps execute for every Windmill row.

### Hatchet

```bash
cd hatchet
docker compose up -d
# create the tenant and an API token in the dashboard on :8888, then:
cp env.list.example env.list               # paste the token into HATCHET_CLIENT_TOKEN
docker build -t bench-worker app
docker run -d --name bench-worker --network hatchet-lite_default --env-file env.list bench-worker python worker.py
docker exec bench-worker python interpreter_bench.py > interpreter.json
docker run --rm --network hatchet-lite_default --env-file env.list bench-worker \
  python run_bench.py --workload fibonacci_40_10 --warmup 1 --reps 5      # prints the run ids
HATCHET_CLIENT_TOKEN=... python3 extract.py --workload fibonacci_40_10 --tasks 40 \
  --run-ids <comma-separated ids> --engine-version v0.107.0 --sdk-version 'hatchet-sdk 1.40.1' \
  --interpreter interpreter.json --notes '...' --out ../results/hatchet_40_10.json
```

Let the engine settle ~90s before the measured reps. Hatchet's per-task latency is not
stationary: under sustained back-to-back load it degrades from ~37 to ~52ms/task over ~25s
and recovers after an idle gap, which correlates with Postgres autovacuum on its Dispatcher
and Ticker heartbeat tables. 30.9ms is the settled state; a saturated-engine run of the
40-task workload can be ~40% slower.

### Temporal

The compose is Temporal's own, not vendored here:

```bash
git clone https://github.com/temporalio/docker-compose temporal-compose
cd temporal-compose                        # its .env pins TEMPORAL_VERSION=1.29.7
docker compose -f docker-compose-postgres.yml up -d
cd ..
pip install 'temporalio==1.33.0'
python3 bench_temporal.py worker &
python3 bench_temporal.py interp --out ../results/temporal_interpreter.json
python3 bench_temporal.py run --steps 40 --fib 10 --workload fibonacci_40_10 \
  --interp ../results/temporal_interpreter.json --out ../results/temporal_40_10.json
```

That repository is archived upstream (the compose files moved to
[temporalio/samples-server](https://github.com/temporalio/samples-server/tree/main/compose));
it is still what the 1.29.7 docs point at, and it is what was used. Check `.env` pins
`TEMPORAL_VERSION=1.29.7` and leave `dynamicconfig/development-sql.yaml` stock for the
default row. For `temporal_tuned`, copy
[`temporal/dynamicconfig/development-sql-tuned.yaml`](temporal/dynamicconfig/development-sql-tuned.yaml)
over it, restart, and wait more than 2 minutes before measuring: the per-shard limiters
refresh on a ~1 minute timer, so earlier runs still show the old throttle on some shards.

Activities are sync `def`s, so the worker gets `activity_executor=ThreadPoolExecutor(
max_workers=100)`, sized to the default `max_concurrent_activities=100`. The workflow awaits
each activity before scheduling the next, so exactly one is ever in flight.
`dump_history.py <workflow-id>` prints a full event history with relative timestamps, which
is how the rate-limit behaviour above was found.

### Prefect

```bash
cd prefect
cp .env.example .env                       # set POSTGRES_PASSWORD
docker compose up -d
docker exec prefect-client python /bench/run_bench.py --steps 40 --fib 10 \
  --warmup 1 --reps 5 --out /bench/ids_40_10.json
python3 extract.py --ids ids_40_10.json --steps 40 --workload fibonacci_40_10 \
  --version 3.8.6 --notes-file notes.txt --out ../results/prefect_40_10.json
```

The flow runs as a plain Python process in the client container - no deployment, no work
pool - and each task is invoked synchronously (no `.submit()`), so each is a server-tracked
task run and they execute in order. Runs are spaced 3s apart to let Prefect drain its
background state and log writers.

### Airflow

```bash
cd airflow
cp .env.example .env
./fetch-compose.sh                         # official 3.3.1 compose, unmodified
docker compose up -d
python3 airflow_harness.py --dag bench_40_10 --tasks 40 --workload fibonacci_40_10 \
  --version 3.3.1 --executor CeleryExecutor --warmup 1 --reps 5 --out ../results/airflow_40_10.json
python3 patch_results.py                   # adds the interpreter and notes fields
```

`dags/bench_dags.py` defines both DAGs (`bench_40_10`, `bench_10_33`) with the TaskFlow API,
chained strictly sequentially. For the tuned rows:

```bash
docker compose -f docker-compose.yaml -f docker-compose.override.tuned.yaml up -d
python3 airflow_harness.py --dag bench_40_10 --tasks 40 --workload fibonacci_40_10 \
  --version 3.3.1 --executor CeleryExecutor --warmup 1 --reps 5 --out ../results/airflow_tuned_40_10.json
python3 patch_tuned.py
```

The override changes exactly two settings and nothing else; the metadata DB is not reset
between the two configurations (the example DAGs are simply marked stale).

### Kestra

```bash
cd kestra
cp .env.example .env                       # set POSTGRES_PASSWORD
docker compose up -d
export KESTRA_PASSWORD=...                 # the admin password set at first-run setup
python3 -c "import json,kestra_bench; json.dump(kestra_bench.interpreter_probe('docker'), open('interp_docker.json','w'))"
python3 kestra_bench.py --steps 40 --fib 10 --workload fibonacci_40_10 --runner docker \
  --warmup 1 --reps 5 --interp interp_docker.json --out ../results/kestra_40_10.json
```

`--runner docker` submits the flow with no `taskRunner` set, which is what Kestra 2.0.2
resolves to its plugin default: `io.kestra.plugin.scripts.runner.docker.Docker`, one
`python:3.13-slim` container per task run. The image was pre-pulled; container start and
teardown are inside the numbers and are the bulk of the per-task cost. `--runner process`
sets `io.kestra.plugin.core.runner.Process` explicitly (use `interpreter_probe('process')`
for its interpreter file) and runs the script as a subprocess of the worker, inside the
Kestra image and therefore on a different CPython.

## What this does not measure

This run measures one thing: sequential critical-path latency, on one worker. Explicitly out
of scope, and not answered anywhere in this directory:

- **Parallel fan-out.** No workload here has two tasks runnable at once. An engine that
  dispatches a wide fan-out well and an engine that does not would score identically.
- **Multi-worker scaling.** One worker per engine throughout. Nothing here says how any of
  these engines behave across 10 workers, which is where dispatch fairness, slot accounting
  and queue contention start to matter.
- **Throughput and sustained load.** Runs are one workflow at a time, spaced out, with the
  engine settled. Hatchet's own notes record that its per-task latency degrades under
  sustained back-to-back load; no engine here was measured saturated.
- **Anything non-Python.** JS and Go were not re-measured, see below.

These are all worth measuring. They are a different benchmark.

## Caveats

- One worker / one flow process per engine. Concurrency settings are left at the default
  where they cannot change the result: the workload is strictly sequential, so at most one
  task is ever runnable, and the validation asserts that is what happened.
- Prefect's tasks are function calls inside the flow process. Its 3.4ms is not comparable to
  an engine that dispatches a job.
- Hatchet's number is the settled state; see the Hatchet section above.
- Temporal's server is 1.29.7, the newest reachable through the official compose path.
- Kestra's `latest` tag resolved to 2.0.2 on the run date. Pin `v2.0.2` to reproduce.
- The interpreter differs per engine and is recorded per file. Compare `overhead/task` on
  `fibonacci_10_33`, not raw totals, across engines on different CPython builds.
- **The JS and Go rows on windmill.dev are still the 2025 t2.medium run.** This refresh is
  Python only; those rows were not re-measured and should not be read alongside these.

## Files

```
README.md          this file
aggregate.py       results/*.json -> the two tables above, and the docs-site data shape
validate.py        invariant checks over results/*.json
results/           24 raw files: 23 runs (engine x workload x config) + one interpreter probe
<engine>/          compose file(s), workflow definition, runner and extractor
```
