#!/bin/bash
# Decomposition of per-task cost. All timings in ms, mean of N runs.
N=20
K=kestra-bench-kestra-1
mean() { python3 -c "
import sys
v=[float(x) for x in sys.stdin.read().split()]
v.sort()
print('mean=%.1fms median=%.1fms min=%.1f max=%.1f n=%d'%(sum(v)/len(v)*1000, v[len(v)//2]*1000, v[0]*1000, v[-1]*1000, len(v)))
"; }

cat > /tmp/fib10.py <<'P'
def fibo(n: int):
  if n <= 1:
      return n
  else:
      return fibo(n - 1) + fibo(n - 2)

print(str(fibo(10)))
P
cat > /tmp/fib10.js <<'J'
function fibo(n) {
  if (n <= 1) {
    return n
  } else {
    return fibo(n-1) + fibo(n-2)
  }
}
console.log(fibo(10))
J
sudo docker cp /tmp/fib10.py $K:/tmp/fib10.py >/dev/null
sudo docker cp /tmp/fib10.js $K:/tmp/fib10.js >/dev/null

t() { # label, command...
  local label=$1; shift
  for i in $(seq 1 $N); do
    s=$(date +%s.%N); "$@" >/dev/null 2>&1; e=$(date +%s.%N)
    echo "$e - $s" | bc
  done | mean | sed "s|^|$label  |"
}

echo "== interpreter-only, inside the Kestra worker (what the Process runner pays) =="
t "python3.12 fib10.py (worker)      " sudo docker exec $K /usr/bin/python3.12 /tmp/fib10.py
t "node fib10.js (worker)            " sudo docker exec $K /usr/local/bin/node /tmp/fib10.js
echo "   (subtract the ~N ms of 'docker exec' itself, measured next)"
t "docker exec /bin/true (worker)    " sudo docker exec $K /bin/true

echo
echo "== the uv/python probe commands the Python Process runner issues per task =="
t "uv --version (worker)             " sudo docker exec $K uv --version
t "uv python find --system (worker)  " sudo docker exec $K uv python find --system --no-managed-python
t "/usr/bin/python --version (worker)" sudo docker exec $K /usr/bin/python --version

echo
echo "== full container lifecycle, interpreter included (what the Docker runner pays) =="
t "docker run --rm python:slim fib10 " sudo docker run --rm -v /tmp/fib10.py:/fib10.py:ro python:slim python /fib10.py
t "docker run --rm node fib10        " sudo docker run --rm -v /tmp/fib10.js:/fib10.js:ro node node /fib10.js
t "docker run --rm python:slim true  " sudo docker run --rm python:slim true
t "docker run --rm node true         " sudo docker run --rm node true
echo
echo "== the per-task docker volume Kestra creates and deletes =="
for i in $(seq 1 $N); do
  s=$(date +%s.%N); v=$(sudo docker volume create -q); sudo docker volume rm "$v" >/dev/null; e=$(date +%s.%N)
  echo "$e - $s" | bc
done | mean | sed 's|^|volume create+rm                     |'
