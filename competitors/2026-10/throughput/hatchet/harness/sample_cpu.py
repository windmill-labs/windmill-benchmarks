#!/usr/bin/env python3
"""Host CPU utilisation + per-container CPU, sampled while a rep runs.

/proc/stat gives whole-box busy CPU in units of cores (32 = every core pinned);
docker stats gives the split between the worker fleet, the engine and Postgres.
"""
import json
import subprocess
import sys
import time

INTERVAL = 1.0


def proc_stat():
    with open("/proc/stat") as f:
        parts = [int(x) for x in f.readline().split()[1:]]
    idle = parts[3] + parts[4]
    return sum(parts), idle


def dstats():
    r = subprocess.run(["docker", "stats", "--no-stream", "--format",
                        "{{.Name}}\t{{.CPUPerc}}"], capture_output=True, text=True)
    out = {}
    for ln in r.stdout.strip().splitlines():
        if "\t" in ln:
            name, cpu = ln.split("\t")
            out[name] = float(cpu.rstrip("%"))
    return out


def main():
    path = sys.argv[1]
    secs = float(sys.argv[2])
    ncpu = int(subprocess.run(["nproc"], capture_output=True, text=True).stdout)
    samples = []
    t_end = time.time() + secs
    while time.time() < t_end:
        pt, pi = proc_stat()
        time.sleep(INTERVAL)
        ct, ci = proc_stat()
        dt, di = ct - pt, ci - pi
        busy_cores = round((1 - di / dt) * ncpu, 2) if dt else 0.0
        samples.append({"t": round(time.time(), 2), "busy_cores": busy_cores,
                        "containers": dstats()})
    with open(path, "w") as f:
        json.dump({"ncpu": ncpu, "interval_s": INTERVAL, "samples": samples}, f)


if __name__ == "__main__":
    main()
