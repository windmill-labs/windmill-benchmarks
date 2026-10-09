set -euo pipefail
cd ~/bench_src && docker build -q -t bench-py . >/dev/null 2>&1
docker run --rm bench-py python sig.py
