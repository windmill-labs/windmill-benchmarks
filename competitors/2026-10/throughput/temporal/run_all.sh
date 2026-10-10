#!/bin/bash
set -uo pipefail
cd ~/bench_src && docker build -q -t bench-py . >/dev/null 2>&1
for CFG in stock tuned; do
  for KIND in sleep cpu; do
    echo "############ $CFG / $KIND ############"
    bash ~/set_config.sh "$CFG" 2>&1 | grep -E "healthy|transferprocessormaxpollrps|ServerVersion|1\.29" | tail -3
    SETTLE=5 bash ~/run_combo.sh "$CFG" "$KIND" 2000 20 6 5 0 2>&1
    echo "############ done $CFG / $KIND ############"
  done
done
echo "ALL COMBOS COMPLETE"
