set -uo pipefail
cat > ~/redo_stock_cpu.sh <<'EOF'
set -uo pipefail
echo "############ REDO stock / cpu ############"
bash ~/set_config.sh stock 2>&1 | grep -E "healthy" | tail -1
SETTLE=5 bash ~/run_combo.sh stock cpu 2000 20 6 5 0 2>&1
echo "REDO COMPLETE"
EOF
cat > ~/chain2.sh <<'EOF'
until grep -q "PIPELINE COMPLETE" ~/bench/pipeline.log 2>/dev/null; do sleep 15; done
bash ~/redo_stock_cpu.sh > ~/bench/redo.log 2>&1
EOF
nohup bash ~/chain2.sh > ~/bench/chain2.log 2>&1 &
echo "redo chained pid $!"
