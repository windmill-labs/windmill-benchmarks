#!/bin/bash
cd ~/kbench
echo "=== serial docker container lifecycle on this box (20 runs) ==="
python3 - <<PY
import subprocess, time, statistics
for cmd, label in (
    (["docker","run","--rm","python:slim","true"], "docker run --rm python:slim true"),
    (["docker","run","--rm","python:slim","python","-c","import time;time.sleep(0.1)"], "docker run --rm python:slim python -c sleep(0.1)"),
):
    ts=[]
    for _ in range(20):
        t=time.perf_counter(); subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL); ts.append(time.perf_counter()-t)
    print(label, "mean_ms", round(statistics.mean(ts)*1000,1), "p50_ms", round(statistics.median(ts)*1000,1))
ts=[]
for _ in range(20):
    t=time.perf_counter()
    v=subprocess.run(["docker","volume","create"],capture_output=True,text=True).stdout.strip()
    subprocess.run(["docker","volume","rm",v],stdout=subprocess.DEVNULL)
    ts.append(time.perf_counter()-t)
print("docker volume create+rm", "mean_ms", round(statistics.mean(ts)*1000,1))
PY
echo "=== cpu attribution, one extra non-counted cpu rep ==="
timeout 1200 python3 cpu_check.py --workload cpu --runner process 2>&1 | tail -25
echo "=== POST DONE ==="
