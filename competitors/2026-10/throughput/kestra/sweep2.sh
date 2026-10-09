cd ~/kbench
for pe in 20 50 100; do
  echo "###### per_exec=$pe"
  timeout 1800 python3 th.py --workload sleep --runner process --units 2000 --per-exec $pe \
     --reps 1 --warmup 1 --timeout 1500 --tag _sw2 2>&1 | tail -6
done
echo "###### SWEEP2 DONE"
