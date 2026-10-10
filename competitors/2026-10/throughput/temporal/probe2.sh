set -euo pipefail
cd ~/bench_src && docker build -q -t bench-py . >/dev/null
docker run --rm bench-py python probe_par.py | tee ~/bench/out/runtime_probe_par.json
