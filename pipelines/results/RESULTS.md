# TPC-H-derived benchmark — results & analysis (2026-07)

Single-node **DuckDB / Polars** vs single-node **Spark 4.0**, 9 TPC-H queries run
sequentially against Parquet on S3 (MinIO), on one **AMD Ryzen 9 9900X
(12c/24t), 60 GiB** box. Full method, versions and disclaimers in
[`../README.md`](../README.md). Raw per-run data in [`results.json`](results.json);
regenerate the tables with `make analyze`.

> **Not an audited TPC-H result.** TPC-H-derived, unaudited, single machine,
> S3-on-localhost. Read the *relative* engine story, not the absolute seconds.

## TL;DR

1. **A single desktop chews through 100 GB of TPC-H without a cluster.** DuckDB
   querying the Parquet directly runs all 9 queries over **SF100 in ~20 s using
   ~20 GB RAM**; Polars' streaming engine does it in **~55 s / 34 GB**. No Spark
   cluster, no distributed anything.
2. **How you use the engine matters more than which engine.** The *same* DuckDB
   is **~20 s at SF100 when it queries Parquet directly** and **~230–247 s (or
   OOM) when it first materializes every table into memory** — a 10×+ swing from
   one line of code. The 2023 benchmark used the slow materialize pattern.
3. **Spark, tuned single-node with AQE, never wins here** and is **20–50× slower
   at small/mid scale** (9 s at SF1 where DuckDB is 0.3 s) because of fixed
   startup + scheduling overhead. At SF100 it's respectable (~150 s) but still
   3–7× behind the single-node engines — and it needed careful heap tuning to
   avoid getting OOM-killed on a shared box.
4. **The 2023 "Polars eager OOMs" result still holds — it just scales with RAM.**
   Eager and lazy-in-memory Polars (and pure in-memory DuckDB) OOM once the
   working set exceeds RAM: at **SF50 on this 60 GB box** (was SF25 on the old
   16 GB box). The fix is the **new streaming/spill engines**, which is the real
   change since 2023.

## Duration — median of warm runs (s), [min–max], cold = first (page-cache-cold) run

| Config | SF1 | SF10 | SF50 | SF100 |
|---|---|---|---|---|
| spark:aqe | **9.02** [8.88–9.24] c=9.09 | **23.96** [22.33–24.25] c=25.39 | **77.86** [74.32–91.19] c=75.99 | **149.12** [147.81–150.44] c=158.24 |
| duckdb:direct | **0.32** [0.31–0.32] c=0.35 | **1.97** [1.96–1.99] c=2.10 | **9.84** [9.84–9.84] c=10.39 | **20.27** [20.23–20.32] c=21.55 |
| duckdb:memory | **0.51** [0.49–0.51] c=0.54 | **3.33** [3.31–3.36] c=3.47 | OOM | OOM |
| duckdb:spill | **0.52** [0.51–0.52] c=0.54 | **4.25** [4.14–4.59] c=4.30 | **84.26** [79.26–84.47] c=77.78 | **232.87** [222.77–242.96] c=239.29 |
| duckdb:disk | **0.52** [0.51–0.53] c=0.53 | **5.85** [5.78–5.93] c=5.91 | **100.92** [72.06–112.47] c=45.18 | **247.00** [231.40–262.59] c=173.93 |
| polars:eager | **0.49** [0.48–0.49] c=0.58 | **4.64** [4.58–4.75] c=4.89 | OOM | OOM |
| polars:lazy | **0.48** [0.48–0.49] c=0.58 | **4.99** [4.97–5.05] c=5.33 | OOM† | OOM |
| polars:streaming | **0.45** [0.45–0.46] c=0.54 | **4.33** [4.30–4.38] c=4.56 | **27.28** [26.69–27.29] c=30.48 | **55.43** [55.35–55.51] c=57.30 |

## Peak RSS — median of warm runs (GB)

| Config | SF1 | SF10 | SF50 | SF100 |
|---|---|---|---|---|
| spark:aqe | 7.12 | 21.5 | 24.57 | 29.32 |
| duckdb:direct | 0.5 | 2.44 | 10.21 | 19.78 |
| duckdb:memory | 2.25 | 17.93 | OOM | OOM |
| duckdb:spill | 2.27 | 12.93 | 32.11 | 24.90 |
| duckdb:disk | 2.25 | 5.75 | 9.23 | 13.35 |
| polars:eager | 2.83 | 21.75 | OOM | OOM |
| polars:lazy | 1.64 | 8.67 | OOM† | OOM |
| polars:streaming | 0.93 | 4.12 | 19.04 | 34.19 |

`OOM` = OS-killed (signal 9) because the working set exceeded the 60 GB box.
`†` polars:lazy at SF50 peaks ~40 GB and completed in ~35 s when it happened to
fit warm, but OOM'd on the controlled cold run — it is right at the memory
ceiling and should be treated as unreliable at SF50+; use streaming.

## What the numbers say

**DuckDB, querying Parquet directly (`duckdb:direct`), is the standout.** Fastest
at every scale and the most memory-efficient config that still scales — SF100 in
~20 s at ~20 GB. It wins because it pushes projections and filters into the
Parquet scan and never materializes tables it doesn't need. This is the idiomatic
way to write a DuckDB step in a Windmill pipeline, and it is what the 2023
benchmark did **not** do.

**Materializing first is the trap.** `duckdb:memory/spill/disk` all
`CREATE TABLE AS SELECT *` every table before querying (the 2023 approach). That
loads columns no query touches and forces spilling once the data exceeds the
memory limit — hence 84–247 s at SF50/100, versus ~10–20 s for `direct`. Pure
in-memory (`duckdb:memory`, no spill dir) simply OOMs past RAM. The frugal
`duckdb:disk` (tight 8–12 GB limit) is the only *materialize* mode that survives
SF100, and it does so at a remarkable **13 GB RSS** — but pays ~247 s for the
heavy spilling.

**Polars' 1.x streaming engine is the big change since 2023.** In 2023 the
"lazy" path (old streaming) was pathologically slow — 2480 s at SF10. The new
streaming engine does SF10 in **4.3 s** and scales cleanly to **SF100 in 55 s at
34 GB**. Eager (`read_parquet` full load) and lazy (in-memory engine) are fine up
to SF10 but OOM at SF50+ on 60 GB — as expected when you ask them to hold more
than fits. Streaming is the answer, and it now works.

**Spark, single-node with AQE, is the honest "no-cluster Spark" baseline — and it
loses.** At SF1/SF10 its fixed overhead (JVM + Catalyst + scheduler) makes it
20–50× slower than the single-node engines (9 s vs 0.3–0.5 s). At SF100 it closes
the gap in relative terms (~150 s) but is still 3–7× behind DuckDB-direct and
Polars-streaming, and it was the fussiest to run: an oversized driver heap got it
OOM-killed on the shared box, so we tuned the heap down (26–28 g at SF50/100) and
let it spill. On a real multi-node cluster Spark would pull ahead eventually —
but the whole point is that for ≤100 GB you don't need one.

## What changed vs 2023 (and what didn't)

| 2023 claim | 2026 verdict |
|---|---|
| Single node beats a Spark cluster-of-one for these workloads | **Still true, more so.** DuckDB-direct/Polars-streaming beat single-node Spark at every scale tested. |
| Polars eager OOMs at large scale (25 GB then) | **Still true, scaled to the box.** OOMs at SF50 on 60 GB. Not obsolete — the *fix* (streaming) is what's new. |
| Polars "lazy" is slow | **Obsolete.** That was the old streaming engine; the 1.x streaming engine is ~500× faster at SF10 and scales to SF100. |
| DuckDB in-memory is fast but memory-hungry | **Half-true and beside the point.** Don't materialize — query the Parquet directly and it's both fastest and lean. |
| Absolute seconds (285 s Spark @ SF1, …) | **Not comparable.** Different hardware, storage, versions and corrected queries. Use the relative story only. |

## Fairness, caveats & honest limitations

- **This benchmarks the libraries single-node.** "Windmill + DuckDB/Polars" means
  the library on one Windmill worker; Windmill supplies orchestration, retries,
  scheduling and zero-config S3 — **not** a compute speed-up. The numbers here are
  properties of DuckDB / Polars / Spark, not of Windmill.
- **Local desktop, not cloud.** A Ryzen 9 9900X with 60 GB and NVMe, reading from
  MinIO on `localhost`. S3-on-localhost has near-zero network latency; real S3
  would add latency roughly uniformly, but would penalize chatty access patterns
  more. This is **not** the 2023 AWS setup and the absolute numbers are not
  comparable to it.
- **We fixed real bugs in the 2023 query translations** (Q5 missing join, Q16
  wrong `LIKE`, Q6 float boundary) so all three engines return identical results
  (verified at SF1 via `make validate`). The old numbers were computed on
  slightly wrong queries.
- **Numeric columns are stored as `double`** across all engines for cross-engine
  parity (the 2023 run inferred float from CSV; TPC-H spec is decimal). Keys stay
  integer.
- **Peak memory = process RSS** (`/usr/bin/time -v`). For Spark's JVM this
  includes committed heap, so its memory figures are heap-tuning-dependent, not a
  minimal footprint.
- **OOMs are the OS killer on a shared 60 GB box.** With more RAM, or a spill dir
  configured, the OOM thresholds move; they are not hard engine limits. We report
  them because they're the real behavior a user on a comparable box would hit.
- **Reduced repetition at large SF** (5 warm runs at SF1/10, 3 at SF50, 2 at
  SF100) — expensive runs, stated not hidden. Spread is shown per cell.
- **DuckLake / lakehouse not benchmarked.** Windmill has native DuckLake support;
  a versioned lakehouse table format over the same S3 data is the obvious next
  step and would change the read pattern (metadata-pruned scans) — out of scope
  here, worth a follow-up.

## Failed / OOM configs (expected)

`duckdb:memory`, `polars:eager` and `polars:lazy` OOM at SF50/SF100 — inherent to
loading/materializing more than fits in 60 GB, not bugs. Every other config
completed. See the "Failed" section of `make analyze` output for the raw list.
