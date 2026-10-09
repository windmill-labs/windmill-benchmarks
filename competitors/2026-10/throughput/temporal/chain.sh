set -uo pipefail
cat > ~/chain_inner.sh <<'EOF'
until grep -q "ALL COMBOS COMPLETE" ~/bench/run_all.log 2>/dev/null; do sleep 10; done
bash ~/run_pipeline.sh > ~/bench/pipeline.log 2>&1
EOF
nohup bash ~/chain_inner.sh > ~/bench/chain.log 2>&1 &
echo "chained pid $!"
