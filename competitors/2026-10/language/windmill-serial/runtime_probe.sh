#!/bin/bash
cd /home/ubuntu
cat > /tmp/f.py <<'PYEOF'
import time, json
def fibo(n): return n if n<=1 else fibo(n-1)+fibo(n-2)
r=[]
for _ in range(3):
    t=time.perf_counter(); fibo(33); r.append(round(time.perf_counter()-t,4))
print(json.dumps(r))
PYEOF
cat > /tmp/f.ts <<'TSEOF'
function fibo(n: number): number { return n <= 1 ? n : fibo(n-1) + fibo(n-2) }
const r: number[] = []
for (let i = 0; i < 3; i++) { const t = performance.now(); fibo(33); r.push(Math.round(performance.now()-t)/1000) }
console.log(JSON.stringify(r))
TSEOF
sudo docker cp /tmp/f.py ubuntu-worker-1:/tmp/f.py
sudo docker cp /tmp/f.ts ubuntu-worker-1:/tmp/f.ts
echo -n 'python_version='; sudo docker compose exec -T worker python3 -c 'import sys;print(sys.version.split()[0])'
echo -n 'bun_version='; sudo docker compose exec -T worker bun --version
echo -n 'cpython_fibo33='; sudo docker compose exec -T worker python3 /tmp/f.py
echo -n 'bun_fibo33='; sudo docker compose exec -T worker bun run /tmp/f.ts
echo -n 'cpython_boot_s='; sudo docker compose exec -T worker python3 -c "
import subprocess,time
t=time.perf_counter()
for _ in range(10): subprocess.run(['python3','-c','pass'])
print(round((time.perf_counter()-t)/10,4))"
echo -n 'bun_boot_s='; sudo docker compose exec -T worker python3 -c "
import subprocess,time
subprocess.run(['bun','run','/tmp/empty.ts'],capture_output=True)
open('/tmp/empty.ts','w').write('')
t=time.perf_counter()
for _ in range(10): subprocess.run(['bun','run','/tmp/empty.ts'],capture_output=True)
print(round((time.perf_counter()-t)/10,4))"
