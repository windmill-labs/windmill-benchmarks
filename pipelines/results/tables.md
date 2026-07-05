## Duration — median of warm runs (s), [min–max], cold=first-run

| Config | SF1 | SF10 | SF50 | SF100 |
|---|---|---|---|---|
| spark:aqe | **9.02** [8.88–9.24] c=9.088 | **23.96** [22.33–24.25] c=25.388 | **77.86** [74.32–91.19] c=75.985 | **149.12** [147.81–150.44] c=158.241 |
| duckdb:direct | **0.32** [0.31–0.32] c=0.35 | **1.97** [1.96–1.99] c=2.103 | **9.84** [9.84–9.84] c=10.389 | **20.27** [20.23–20.32] c=21.554 |
| duckdb:memory | **0.51** [0.49–0.51] c=0.537 | **3.33** [3.31–3.36] c=3.468 | — | — |
| duckdb:spill | **0.52** [0.51–0.52] c=0.542 | **4.25** [4.14–4.59] c=4.3 | **84.26** [79.26–84.47] c=77.783 | **232.87** [222.77–242.96] c=239.291 |
| duckdb:disk | **0.52** [0.51–0.53] c=0.531 | **5.85** [5.78–5.93] c=5.914 | **100.92** [72.06–112.47] c=45.177 | **247.0** [231.4–262.59] c=173.929 |
| polars:eager | **0.49** [0.48–0.49] c=0.576 | **4.64** [4.58–4.75] c=4.89 | — | — |
| polars:lazy | **0.48** [0.48–0.49] c=0.578 | **4.99** [4.97–5.05] c=5.334 | — | — |
| polars:streaming | **0.45** [0.45–0.46] c=0.538 | **4.33** [4.3–4.38] c=4.56 | **27.28** [26.69–27.29] c=30.483 | **55.43** [55.35–55.51] c=57.297 |

## Peak RSS — median of warm runs (GB)

| Config | SF1 | SF10 | SF50 | SF100 |
|---|---|---|---|---|
| spark:aqe | 7.12 | 21.5 | 24.57 | 29.32 |
| duckdb:direct | 0.5 | 2.44 | 10.21 | 19.78 |
| duckdb:memory | 2.25 | 17.93 | — | — |
| duckdb:spill | 2.27 | 12.93 | 32.11 | 24.9 |
| duckdb:disk | 2.25 | 5.75 | 9.23 | 13.35 |
| polars:eager | 2.83 | 21.75 | — | — |
| polars:lazy | 1.64 | 8.67 | — | — |
| polars:streaming | 0.93 | 4.12 | 19.04 | 34.19 |

## vs 2023 — for reference only, NOT a like-for-like comparison

> Different hardware (unknown AWS m4 vs a Ryzen 9 9900X), different
> storage (S3 vs MinIO-on-localhost), newer engines, and correctness
> fixes all move at once. Do not read these as pure engine speedups.

| Config | SF1 dur 2023 → now (s) | SF10 dur 2023 → now (s) |
|---|---|---|
| spark:aqe | 285 → 9.02 | 1170 → 23.96 |
| duckdb:memory | 61 → 0.51 | 560 → 3.33 |
| polars:eager | 42 → 0.49 | 370 → 4.64 |
| polars:lazy | 247 → 0.48 | 2480 → 4.99 |

## Failed / timed-out configs

- **duckdb:memory:sf50**: 1/1 runs failed — `Command terminated by signal 9
	Command being timed: "/home/rfiszel/tpch-bench/.venv/bin/python /home/rfiszel/tpch-bench`
- **polars:eager:sf50**: 1/1 runs failed — `Command terminated by signal 9
	Command being timed: "/home/rfiszel/tpch-bench/.venv/bin/python /home/rfiszel/tpch-bench`
- **duckdb:memory:sf100**: 1/1 runs failed — `Command terminated by signal 9
	Command being timed: "/home/rfiszel/tpch-bench/.venv/bin/python /home/rfiszel/tpch-bench`
- **polars:eager:sf100**: 1/1 runs failed — `Command terminated by signal 9
	Command being timed: "/home/rfiszel/tpch-bench/.venv/bin/python /home/rfiszel/tpch-bench`
- **polars:lazy:sf100**: 1/1 runs failed — `Command terminated by signal 9
	Command being timed: "/home/rfiszel/tpch-bench/.venv/bin/python /home/rfiszel/tpch-bench`
- **polars:lazy:sf50**: 1/1 runs failed — `Command terminated by signal 9
	Command being timed: "/home/rfiszel/tpch-bench/.venv/bin/python /home/rfiszel/tpch-bench`
