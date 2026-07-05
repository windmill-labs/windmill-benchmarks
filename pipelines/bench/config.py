"""Single source of truth for the TPC-H-derived benchmark.

Everything tunable lives here: scale factors, engine variants, repetition
counts, timeouts, per-engine memory, S3 connection, and paths. Every other
script imports from this module, so changing the benchmark = editing this file.

All paths and the S3 connection can be overridden with environment variables
(defaults target a local MinIO on localhost), so the same code runs unchanged
on a laptop, a CI runner, or a cloud box.

---- How to extend ----------------------------------------------------------
* Change scale factors:      edit SCALE_FACTORS (and WARM/TIMEOUT/*_MEM entries).
* Add a Polars/DuckDB variant: add it to CONFIGS and handle it in that engine's
  tpc_h.py (variant is passed via the <ENGINE>_VARIANT env var).
* Add a whole new engine:    create pipelines/<engine>/tpc_h.py that reads the
  same S3_ENV + BENCH_OUT contract (emit JSON {engine,variant,sf,timings}),
  add its command to orchestrate.build_cmd(), and list it in CONFIGS.
* Add / change a query:      edit the query in all three engine files (the SQL
  in duckdb/tpc_h.py is the canonical reference) and re-run validate.py.
"""
import os

# ---- paths (env-overridable) ------------------------------------------------
_here = os.path.dirname(os.path.abspath(__file__))
REPO_PIPELINES = os.path.dirname(_here)                       # pipelines/
# Working area for tools, generated data and spill. Keep OFF tmpfs (needs 100+ GB).
BENCH_HOME = os.environ.get("WM_BENCH_HOME", os.path.expanduser("~/tpch-bench"))
TOOLS_DIR = os.environ.get("WM_TOOLS_DIR", os.path.join(BENCH_HOME, "tools"))
DATA_DIR = os.environ.get("WM_DATA_DIR", os.path.join(BENCH_HOME, "data"))
SPILL_DIR = os.environ.get("WM_SPILL_DIR", os.path.join(BENCH_HOME, "spill"))
RESULTS_DIR = os.environ.get("WM_RESULTS_DIR", os.path.join(REPO_PIPELINES, "results"))
PY = os.environ.get("WM_PYTHON", os.path.join(BENCH_HOME, ".venv", "bin", "python"))

SPARK_HOME = os.environ.get("SPARK_HOME", os.path.join(TOOLS_DIR, "spark"))
JAVA_HOME = os.environ.get("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk")
MC_BIN = os.environ.get("WM_MC", os.path.join(TOOLS_DIR, "mc"))
MINIO_BIN = os.environ.get("WM_MINIO", os.path.join(TOOLS_DIR, "minio"))
HADOOP_AWS = os.environ.get("WM_HADOOP_AWS", "org.apache.hadoop:hadoop-aws:3.4.1")

# ---- S3 connection (env-overridable; defaults = local MinIO) ----------------
S3_ENV = {
    "S3_BUCKET": os.environ.get("S3_BUCKET", "windmill"),
    "S3_ENDPOINT": os.environ.get("S3_ENDPOINT", "127.0.0.1:9000"),
    "S3_USE_SSL": os.environ.get("S3_USE_SSL", "false"),
    "S3_URL_STYLE": os.environ.get("S3_URL_STYLE", "path"),
    "AWS_REGION": os.environ.get("AWS_REGION", "us-east-1"),
    "AWS_ACCESS_KEY": os.environ.get("AWS_ACCESS_KEY", "minioadmin"),
    "AWS_SECRET_KEY": os.environ.get("AWS_SECRET_KEY", "minioadmin"),
}

# ---- benchmark matrix -------------------------------------------------------
SCALE_FACTORS = [1, 10, 50, 100]

# (engine, variant) pairs to run. Variant is passed to the engine via env.
CONFIGS = [
    ("duckdb", "memory"), ("duckdb", "disk"), ("duckdb", "spill"), ("duckdb", "direct"),
    ("polars", "eager"), ("polars", "lazy"), ("polars", "streaming"),
    ("spark", "aqe"),
]

# warm runs per SF (1 cold run is always added). Fewer at large SF where each
# run is expensive — this cap is reported in RESULTS.md, never hidden.
WARM = {1: 5, 10: 5, 50: 3, 100: 2}
# per-run wall-clock timeout (s); a config exceeding it is recorded as a timeout.
TIMEOUT = {1: 300, 10: 1200, 50: 3000, 100: 5400}
# Spark single-node driver heap by SF. Kept well under total RAM so the JVM +
# reclaimable page cache don't trip the OOM killer; Spark spills the rest to
# spark.local.dir. (An oversized heap gets OOM-killed on a shared box.)
SPARK_MEM = {1: "16g", 10: "24g", 50: "26g", 100: "28g"}
# DuckDB "disk" variant: tight memory_limit (frugal, spills hard).
DDB_DISK_MEM = {1: "2GB", 10: "4GB", 50: "8GB", 100: "12GB"}
# DuckDB "spill" variant: generous memory_limit (realistic: use most of RAM,
# spill only when needed).
DDB_SPILL_MEM = {1: "4GB", 10: "12GB", 50: "32GB", 100: "24GB"}

# TPC-H tables + the local->spec query mapping (for docs / analysis).
TABLES = ["customer", "orders", "lineitem", "supplier",
          "part", "partsupp", "nation", "region"]
QUERY_MAP = {1: "Q1", 2: "Q3", 3: "Q5", 4: "Q6", 5: "Q10",
             6: "Q12", 7: "Q14", 8: "Q16", 9: "Q18"}
