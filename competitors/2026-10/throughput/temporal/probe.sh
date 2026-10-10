set -euo pipefail
docker run --rm bench-py python runtime_probe.py | tee ~/bench/out/runtime_probe.json
echo "--- host ---"
lsb_release -ds; uname -r
grep -m1 "model name" /proc/cpuinfo
nproc
