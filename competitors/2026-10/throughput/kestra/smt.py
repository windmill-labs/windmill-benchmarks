#!/usr/bin/env python3
"""Topology plus the real cost of fibo(31) when 20 of them run at once."""
import os, json, subprocess, multiprocessing as mp, time, statistics, glob

def topo():
    sib = {}
    for p in sorted(glob.glob("/sys/devices/system/cpu/cpu[0-9]*/topology/thread_siblings_list")):
        sib[p.split("/")[5]] = open(p).read().strip()
    cores = len(set(sib.values()))
    return {"vcpus": len(sib), "distinct_sibling_groups": cores,
            "threads_per_core": round(len(sib)/cores, 1) if cores else None,
            "model": next((l.split(":",1)[1].strip() for l in open("/proc/cpuinfo") if l.startswith("model name")), None)}

def fibo(n):
    return n if n <= 1 else fibo(n-1) + fibo(n-2)

def worker(q, reps):
    ts = []
    for _ in range(reps):
        t = time.perf_counter(); fibo(31); ts.append(time.perf_counter()-t)
    q.put(ts)

def measure(par, reps=12):
    q = mp.Queue()
    ps = [mp.Process(target=worker, args=(q, reps)) for _ in range(par)]
    t0 = time.perf_counter()
    for p in ps: p.start()
    allts = [q.get() for _ in ps]
    for p in ps: p.join()
    wall = time.perf_counter() - t0
    flat = [x for ts in allts for x in ts]
    return {"parallelism": par, "fibo31_p50_s": round(statistics.median(flat), 4),
            "fibo31_mean_s": round(statistics.mean(flat), 4),
            "aggregate_calls_per_s": round(par*reps/wall, 2)}

out = {"topology": topo(), "python": __import__("sys").version,
       "fibo31_under_parallelism": [measure(p) for p in (1, 10, 20, 32)]}
json.dump(out, open("/home/ubuntu/kbench/smt.json", "w"), indent=1)
print(json.dumps(out, indent=1))
