cd ~/kbench
for pe in 20 50 100 200; do
  echo "###### per_exec=$pe"
  timeout 1200 python3 th.py --workload sleep --runner process --units 2000 --per-exec $pe \
     --reps 1 --warmup 1 --timeout 900 --tag _sw 2>&1 | tail -5
done
echo "###### SWEEP DONE"
