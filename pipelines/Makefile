# TPC-H-derived benchmark — one entrypoint per stage.
# Everything tunable lives in bench/config.py (paths, scale factors, engines,
# repetitions, timeouts). Override paths/creds via env (WM_BENCH_HOME, S3_*, ...).
#
#   make setup     # venv + deps + Spark + MinIO (needs JDK 17 + JAVA_HOME)
#   make minio     # start MinIO and create the bucket
#   make data      # generate + upload TPC-H data for all scale factors
#   make run       # run the benchmark (resumable)
#   make validate  # cross-check engines return identical results (SF1)
#   make analyze   # aggregate results.json -> results/tables.md
#   make all       # data + run + analyze
#
# results/RESULTS.md is the curated write-up; `make analyze` refreshes the raw
# tables in results/tables.md (which RESULTS.md snapshots).

BENCH_HOME ?= $(HOME)/tpch-bench
PY         := $(BENCH_HOME)/.venv/bin/python
TOOLS      := $(BENCH_HOME)/tools
RUN        := cd bench && PYTHONPATH=. $(PY)

.PHONY: setup minio data run validate analyze all
setup:
	bash bench/setup.sh

minio:
	WM_TOOLS_DIR=$(TOOLS) bash bench/run_minio.sh

data:
	$(RUN) gen_data.py

run:
	$(RUN) orchestrate.py

validate:
	$(RUN) validate.py 1

analyze:
	$(RUN) analyze.py > ../results/tables.md && echo "wrote results/tables.md"

all: data run analyze
