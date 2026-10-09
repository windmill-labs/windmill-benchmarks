set -euo pipefail
cd ~/bench_src
docker build -t bench-py . 2>&1 | tail -5
mkdir -p ~/bench/out ~/bench/ctl
docker run --rm --network temporal-network -e TEMPORAL_ADDRESS=temporal:7233 bench-py python -c "import temporalio, sys; print(sys.version); print('temporalio', temporalio.__version__)"
