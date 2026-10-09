set -euo pipefail
cd ~/bench_src && docker build -q -t bench-py . >/dev/null 2>&1
docker run --rm --network temporal-network -e TEMPORAL_ADDRESS=temporal:7233 \
  -v ~/bench/ctl:/ctl -v ~/bench/out:/out bench-py python reader.py /out/smoke_sleep.json 1
cat ~/bench/out/smoke_sleep.json
