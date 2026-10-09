#!/bin/bash
K=kestra-bench-kestra-1
INNER='
N=20; tot=0; min=999999999; max=0
for i in $(seq 1 $N); do
  s=$(date +%s%N); eval "$CMD" >/dev/null 2>&1; e=$(date +%s%N)
  d=$(( (e - s) / 1000 ))
  tot=$((tot+d)); [ $d -lt $min ] && min=$d; [ $d -gt $max ] && max=$d
done
echo "mean=$((tot/N/1000)).$(( (tot/N)%1000 / 100 ))ms min=$((min/1000))ms max=$((max/1000))ms"
'
run_in() { # container, label, cmd
  local c=$1 label=$2 cmd=$3
  printf '%-52s ' "$label"
  sudo docker exec -e CMD="$cmd" "$c" bash -c "$INNER"
}
echo "== interpreter + script launch, measured INSIDE the runtime's own container =="
run_in $K "worker: python3.12 fib10.py (Process runner)" '/usr/bin/python3.12 /tmp/fib10.py'
run_in $K "worker: python3.12 -c pass" '/usr/bin/python3.12 -c pass'
run_in $K "worker: node fib10.js (Process runner)" '/usr/local/bin/node /tmp/fib10.js'
run_in $K "worker: node -e 0" '/usr/local/bin/node -e 0'
echo
echo "== same interpreters inside the task images (no container lifecycle) =="
sudo docker rm -f pyslim nodec >/dev/null 2>&1
sudo docker run -d --name pyslim -v /tmp/fib10.py:/tmp/fib10.py:ro python:slim sleep 600 >/dev/null
sudo docker run -d --name nodec  -v /tmp/fib10.js:/tmp/fib10.js:ro node        sleep 600 >/dev/null
run_in pyslim "python:slim: python /tmp/fib10.py" 'python /tmp/fib10.py'
run_in pyslim "python:slim: python -c pass" 'python -c pass'
run_in nodec  "node image: node /tmp/fib10.js" 'node /tmp/fib10.js'
run_in nodec  "node image: node -e 0" 'node -e 0'
sudo docker rm -f pyslim nodec >/dev/null 2>&1
echo
echo "== per-task docker volume create + remove (host) =="
N=20; tot=0
for i in $(seq 1 $N); do
  s=$(date +%s%N); v=$(sudo docker volume create); sudo docker volume rm "$v" >/dev/null; e=$(date +%s%N)
  tot=$(( tot + (e-s)/1000 ))
done
echo "volume create+rm mean=$((tot/N/1000)).$(( (tot/N)%1000/100 ))ms"
