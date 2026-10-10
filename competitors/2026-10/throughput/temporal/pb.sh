set -euo pipefail
cd ~/bench_src && docker build -q -t bench-py . >/dev/null 2>&1
docker run --rm --network temporal-network -e TEMPORAL_ADDRESS=temporal:7233 bench-py python probe_backlog.py bench-act
