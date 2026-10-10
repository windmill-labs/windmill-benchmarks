set -euo pipefail
cd ~/bench_src && docker build -q -t bench-py . >/dev/null 2>&1
CTL=~/bench/ctl; rm -rf $CTL; mkdir -p $CTL
docker rm -f bench-wfw bench-actw >/dev/null 2>&1 || true
docker run -d --name bench-wfw --network temporal-network -e TEMPORAL_ADDRESS=temporal:7233 -v $CTL:/ctl bench-py python wfworker.py >/dev/null
for i in $(seq 1 60); do [ -f $CTL/wf_ready ] && break; sleep 1; done
docker run --rm --network temporal-network -e TEMPORAL_ADDRESS=temporal:7233 bench-py python probe_ingest2.py 2000 20 ing2
docker rm -f bench-wfw >/dev/null 2>&1 || true
