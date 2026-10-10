#!/bin/bash
set -uo pipefail
cd ~/bench_src && docker build -q -t bench-py . >/dev/null 2>&1

echo "######## stock / cpu (main matrix, redo) ########"
bash ~/set_config.sh stock 2>&1 | grep -E "healthy|transferprocessormaxpollrps" | tail -2
SETTLE=5 PRERELEASE=0 bash ~/run_combo2.sh stock cpu 2000 20 6 5 0 temporal_throughput_stock_cpu.json

echo "######## pipeline control: stock / sleep ########"
SETTLE=5 PRERELEASE=1 bash ~/run_combo2.sh stock sleep 2000 20 4 5 0 pipeline_stock_sleep.json

echo "######## pipeline control: tuned / sleep ########"
bash ~/set_config.sh tuned 2>&1 | grep -E "healthy|transferprocessormaxpollrps" | tail -2
SETTLE=5 PRERELEASE=1 bash ~/run_combo2.sh tuned sleep 2000 20 4 5 0 pipeline_tuned_sleep.json

echo "FINAL COMPLETE"
