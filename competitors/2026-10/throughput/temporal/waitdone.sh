until grep -q "ALL COMBOS COMPLETE" ~/bench/run_all.log 2>/dev/null; do sleep 10; done
echo COMPLETE
