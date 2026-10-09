#!/bin/bash
# Supplementary: end-to-end pipeline with no pre-enqueueing, to see what the
# stock transferProcessorMaxPollRPS costs when submission is inside the window.
set -uo pipefail
cd ~/bench_src && docker build -q -t bench-py . >/dev/null 2>&1
for CFG in stock tuned; do
  echo "############ pipeline $CFG / sleep ############"
  bash ~/set_config.sh "$CFG" 2>&1 | grep -E "healthy" | tail -1
  PRERELEASE=1 bash ~/run_combo.sh "$CFG" sleep 2000 20 4 5 0 pipeline_${CFG}_sleep.json 2>&1 | grep -E "^rep|ready"
done
echo "PIPELINE COMPLETE"
