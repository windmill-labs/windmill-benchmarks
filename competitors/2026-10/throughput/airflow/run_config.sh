#!/bin/bash
# usage: run_config.sh <config>
set -u
cfg=$1
cd ~/af
mkdir -p out
echo "=== $cfg start $(date -Is) ==="
python3 harness.py "$cfg" sleep 5
echo "=== $cfg sleep done $(date -Is) ==="
python3 harness.py "$cfg" cpu 5
echo "=== $cfg cpu done $(date -Is) ==="
