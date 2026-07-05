Windmill for Data Pipelines — TPC-H-derived benchmark
=====================================================

This repo compares single-node **Polars**, **DuckDB** and **Spark** on a subset
of TPC-H queries, run the way a Windmill data pipeline runs them: each engine is
a plain script/worker reading Parquet from S3-compatible storage. The point is
to show that for the overwhelming majority of ETL workloads a single modern node
beats a distributed cluster on both wall-clock time and memory.

> ### This is a "TPC-H-derived" benchmark, not an audited TPC-H result
>
> It uses the TPC-H schema, data generator and a **9-query subset**, but it is
> **not** an official, audited TPC-H benchmark and the numbers here **must not**
> be compared with published TPC-H results or used as an official TPC metric.
> "TPC-H" is a trademark of the [Transaction Processing Performance Council](https://www.tpc.org/tpch/).
> The full specification is [here](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp).

---

## History & honesty note (updated 2026-07)

The original 2023 run (blog: *Data pipeline orchestrator*, launch week 1) was a
**marketing artifact**, and several of its choices no longer hold. This refresh
re-runs everything on current engine versions and is deliberately critical of
the old numbers. What changed and why:

- **Hardware label was wrong.** The 2023 caption said "m4.xlarge (8 vCPU, 32 GB
  RAM)". An `m4.xlarge` is **4 vCPU / 16 GiB** — the 8 vCPU / 32 GB box is an
  `m4.2xlarge`. We could not recover which instance actually ran, so rather than
  repeat an unverifiable AWS label we re-ran on a **known local machine** and
  state its exact spec below.
- **Query count was inconsistent** — the blog said "8 queries", the old README
  said "9". It is **9** (see the query table). We kept the same 9.
- **No variance was reported** — a single run per config. We now run each config
  multiple times and report **median + min–max spread**, plus a cold run.
- **"Windmill + Polars" is Polars on one worker.** This benchmarks the *library*
  single-node. Windmill contributes orchestration, retries, scheduling and the
  zero-config S3 glue — **not** a compute speed-up. The honest framing is
  "Polars/DuckDB on a Windmill worker vs a Spark cluster-of-one".
- **Engine versions moved a lot** (Polars 0.20 → 1.x streaming rewrite; DuckDB
  pre-1.0 → 1.x larger-than-memory/spill; Spark 3.x → 4.0 with AQE). Several
  2023 conclusions are now obsolete — see `results/RESULTS.md`.
- **Correctness fixes.** The 2023 Polars/Spark translations had real bugs that
  we fixed so all three engines return identical results (verified at SF1):
  - **Q5** (`query_3`) was missing the `c_nationkey = s_nationkey` join.
  - **Q16** (`query_8`) used `contains("Customer") OR contains("Complaints")`
    instead of the spec's ordered `LIKE '%Customer%Complaints%'`.
  - **Q6** (`query_4`) `l_discount between 0.06-0.01 and 0.06+0.01` silently
    dropped the `0.07` rows under float arithmetic; bounds are now exact.

---

## Environment (this refresh)

| | |
|---|---|
| Machine | Local workstation — **AMD Ryzen 9 9900X (12 cores / 24 threads)**, **60 GiB** RAM, NVMe SSD |
| OS | Arch Linux, kernel 7.0 |
| Storage | **MinIO** (S3 API) on the same box; every engine reads Parquet over `s3://` — matching the original "read from S3" methodology, on localhost |
| Python | 3.12 |
| Polars | 1.42.1 |
| DuckDB | 1.5.4 |
| Spark | 4.0.0 (Scala 2.13, Hadoop 3.4.1, `hadoop-aws` 3.4.1), single node `local[*]`, AQE on |
| Java | OpenJDK 17.0.19 |

> Single-machine, S3-on-localhost. This is **not** AWS and not directly
> comparable to the 2023 absolute numbers; the value is in the *relative*
> engine comparison and in which 2023 conclusions still hold. See
> `VERSIONS.txt` for the exact pins.

## The 9 queries

The subset renumbers 9 standard TPC-H queries as `query_1..query_9`:

| here | TPC-H | what it exercises |
|---|---|---|
| query_1 | Q1  | full scan + big group-by/agg on `lineitem` |
| query_2 | Q3  | 3-way join, group-by, top-N |
| query_3 | Q5  | 6-way join, group-by |
| query_4 | Q6  | filtered scan + sum |
| query_5 | Q10 | 4-way join, wide group-by, top-N |
| query_6 | Q12 | 2-way join, conditional aggregation |
| query_7 | Q14 | 2-way join, conditional ratio |
| query_8 | Q16 | anti-join + count-distinct |
| query_9 | Q18 | correlated subquery (large-volume customers) |

Each engine loads the 8 tables and runs the 9 queries **sequentially in one
process**, mirroring a Windmill flow. The reported "duration" is the in-process
total (table load + 9 queries), excluding interpreter/JVM bootstrap.

## Methodology

- **Scale factors**: SF 1 / 10 / 50 / 100 (SF100 ≈ 100 GB of raw TPC-H data;
  Parquet inputs are smaller). The old set was 1/5/10/25; on a 60 GiB box the
  larger-than-memory regime only shows up past SF25, hence SF50/100.
- **Data**: generated with DuckDB's `tpch` extension (`CALL dbgen`), written to
  Parquet (zstd) and uploaded to MinIO under `tpc-h/<sf>/raw/<table>.parquet`.
  Numeric `DECIMAL` columns are stored as **double** so all three engines read
  an identical float representation (the 2023 run inferred float from CSV);
  keys stay integer. See `bench/gen_data.py`.
- **Variants**:
  - DuckDB — `direct` (query the Parquet directly via views with pushdown, no
    upfront materialization — the idiomatic DuckDB-on-S3 path), `spill`
    (materialize tables with a generous `memory_limit` + `temp_directory`),
    `memory` (materialize, pure in-memory, no spill) and `disk` (materialize
    with a deliberately tight `memory_limit` — the frugal, heavy-spill extreme).
    The `memory`/`spill`/`disk` trio load every table up front (matching the
    2023 setup); `direct` is how you'd actually write a DuckDB step.
  - Polars — `eager` (`read_parquet`, full load into RAM), `lazy`
    (`scan_parquet` + in-memory engine) and `streaming` (`scan_parquet` +
    streaming engine, out-of-core).
  - Spark — single-node `local[*]` with Adaptive Query Execution enabled and
    driver heap tuned per SF (not a strawman).
- **Repetitions**: 1 **cold** run (Linux page cache dropped via
  `drop_caches`, so MinIO reads from disk) + N **warm** runs (N = 5 for SF 1/10,
  3 for SF 50, 2 for SF 100 — larger SF runs are expensive; this cap is
  reported, not hidden). Headline = **median of warm runs**, with min–max spread
  and the cold value shown alongside.
- **Memory**: **peak RSS** of the whole engine process, captured with
  `/usr/bin/time -v` ("Maximum resident set size"). One process runs all 9
  queries, so this is the peak across the sequential run — the same "RSS-peak"
  metric the 2023 run used.

## Reproduce

Everything is driven from `pipelines/` via a `Makefile`; a **JDK 17** must be
installed with `JAVA_HOME` set (Spark needs it). The rest is downloaded by
`make setup`.

```bash
cd pipelines
make setup      # venv + deps + Spark 4.0.0 + MinIO into ~/tpch-bench
make minio      # start MinIO and create the bucket
make data       # generate + upload TPC-H data for every scale factor
make run        # run the benchmark (resumable — skips completed configs)
make validate   # assert all engines return identical results (SF1)
make analyze    # aggregate results.json -> results/tables.md
```

`results/RESULTS.md` is the curated analysis + 2023 comparison; `make analyze`
refreshes the raw aggregated tables in `results/tables.md`.

Or run a single stage/scale by hand, e.g. `cd bench && PYTHONPATH=. python
orchestrate.py 10`. Individual engine scripts are runnable directly (and as
Windmill scripts) purely via environment variables — see the header of each
`*/tpc_h.py`.

## Maintaining & extending

**`bench/config.py` is the single source of truth** — scale factors, engine
variants, repetition counts, timeouts, per-engine memory, S3 connection and all
paths live there. Common changes:

- **Different scale factors / more repetitions** → edit `SCALE_FACTORS`, `WARM`,
  `TIMEOUT` in `config.py`.
- **New engine variant** (e.g. a DuckDB PRAGMA sweep) → add it to `CONFIGS` and
  handle the `<ENGINE>_VARIANT` env var in that engine's `tpc_h.py`.
- **A whole new engine** (DataFusion, chDB, …) → add `pipelines/<engine>/tpc_h.py`
  honoring the same contract (read `S3_*` env, run the 9 queries sequentially,
  write `BENCH_OUT` JSON `{engine,variant,sf,timings:{load,query_1..9,total}}`),
  wire its command into `orchestrate.build_cmd()`, and list it in `CONFIGS`.
- **Run on real cloud/S3 instead of local MinIO** → export `S3_ENDPOINT`,
  `AWS_*` (and drop `S3_USE_SSL=false`); no code changes.
- **Change a query** → edit it in all three engine files (the SQL in
  `duckdb/tpc_h.py` is canonical) and re-run `make validate` to confirm the
  engines still agree.

## Files

- `duckdb/tpc_h.py`, `polars/tpc_h.py`, `spark/tpc_h.py` — the benchmarked engines.
- `airflow/tpc_h.py` — legacy Polars-on-Airflow DAG kept for the orchestration
  comparison; **not** re-run in this refresh (it shares the old query
  translations).
- `Makefile` — one target per stage (`setup`/`minio`/`data`/`run`/`validate`/`analyze`).
- `bench/` — `config.py` (**the knobs**), `setup.sh`, `run_minio.sh`,
  `gen_data.py`, `orchestrate.py`, `validate.py`, `analyze.py`,
  `requirements.txt`, `VERSIONS.txt` (exact pins).
- `results/` — `results.json` (raw per-run), `summary.json`, `RESULTS.md`
  (analysis + 2023 comparison).
