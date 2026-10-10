#!/usr/bin/env python3
"""Pure-CPU ceilings for fibo(31), measured with the SAME interpreter the
Kestra Docker task runner uses (python:slim), with Kestra out of the picture.

1. serial: one process, fibo(31) in a loop -> single-threaded call time.
2. parallel-20: 20 processes inside ONE container, each looping fibo(31) for a
   fixed wall window -> calls/s a 20-slot pool could retire if task dispatch,
   container start and the orchestrator were free. m7i.8xlarge is 32 vCPU over
   16 PHYSICAL cores, so this is well below 20 / serial_time.
3. parallel-16 / parallel-32 for context on where SMT stops helping.
Also: the serial container lifecycle cost (docker run --rm python:slim).
"""
import json, subprocess, time, sys, os

IMG = "python:slim"

INNER = r'''
import os, sys, time, json

def fibo(n):
    if n <= 1:
        return n
    return fibo(n - 1) + fibo(n - 2)

def serial(reps):
    out = []
    for _ in range(reps):
        t = time.perf_counter(); fibo(31); out.append(round(time.perf_counter() - t, 5))
    return out

def parallel(k, window):
    """k forked processes, each looping fibo(31) for `window` seconds.
    Raw os.fork: python 3.14's multiprocessing defaults to forkserver, which
    does not come up inside this container."""
    pipes, pids = [], []
    t0 = time.perf_counter()
    for _ in range(k):
        rfd, wfd = os.pipe()
        pid = os.fork()
        if pid == 0:
            os.close(rfd)
            n = 0
            end = time.perf_counter() + window
            while time.perf_counter() < end:
                fibo(31); n += 1
            os.write(wfd, str(n).encode())
            os.close(wfd)
            os._exit(0)
        os.close(wfd)
        pipes.append(rfd); pids.append(pid)
    tot = 0
    for rfd in pipes:
        buf = b""
        while True:
            c = os.read(rfd, 64)
            if not c: break
            buf += c
        os.close(rfd)
        tot += int(buf)
    bad = 0
    for pid in pids:
        _, st = os.waitpid(pid, 0)
        if st != 0: bad += 1
    el = time.perf_counter() - t0
    return {"procs": k, "window_s": window, "calls": tot, "nonzero_exits": bad,
            "elapsed_s": round(el, 3), "calls_per_s": round(tot / el, 2)}

res = {"python_version": sys.version, "executable": sys.executable,
       "cpu_count": os.cpu_count(), "sched_affinity": len(os.sched_getaffinity(0))}
res["fibo31_serial_s"] = serial(7)
res["parallel"] = [parallel(k, 8.0) for k in (16, 20, 32)]
print("CEIL " + json.dumps(res))
'''

def sh(cmd, **kw):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True, **kw)

out = {}

# container lifecycle cost, serial, on an idle box
lc = []
for _ in range(5):
    t = time.perf_counter()
    r = sh(f"docker run --rm {IMG} true")
    lc.append(round(time.perf_counter() - t, 4))
    assert r.returncode == 0, r.stderr
out["serial_docker_run_true_s"] = lc

lc2 = []
for _ in range(5):
    t = time.perf_counter()
    r = sh(f"docker run --rm {IMG} python -c \"import time; time.sleep(0.1)\"")
    lc2.append(round(time.perf_counter() - t, 4))
    assert r.returncode == 0, r.stderr
out["serial_docker_run_sleep100ms_s"] = lc2

# cpu ceilings, inside one python:slim container
open("/tmp/ceil_inner.py", "w").write(INNER)
r = sh(f"docker run --rm -v /tmp/ceil_inner.py:/x.py:ro {IMG} python /x.py", timeout=600)
assert r.returncode == 0, (r.returncode, r.stdout[-2000:], r.stderr[-2000:])
hit = [l for l in r.stdout.splitlines() if l.startswith("CEIL ")]
assert hit, r.stdout[-2000:]
out["in_container"] = json.loads(hit[0][5:])

json.dump(out, open(os.path.expanduser("~/kbench/ceiling.json"), "w"), indent=1)
print(json.dumps(out, indent=1))
