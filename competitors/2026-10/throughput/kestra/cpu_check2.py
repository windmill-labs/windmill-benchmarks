#!/usr/bin/env python3
"""Attribute CPU during one extra (non-counted) rep.

Short-lived task processes cannot be sampled directly, so the Kestra JVM's
cutime+cstime is used instead: the kernel rolls a reaped child's CPU (and all of
its own reaped descendants') into its parent, and every Process-runner task is a
`/bin/sh -c python ...` child of the JVM. The delta over the rep is therefore the
total CPU of the task processes; the JVM's own utime+stime is the orchestration
cost, and the Postgres cgroup covers the queue/repository.
"""
import sys, os, json, time, glob, threading, argparse, subprocess
sys.path.insert(0, "/home/ubuntu/kbench")
import th
from th import run_rep, analyse, gen_flow
from kapi import upsert_flow

CLK = os.sysconf("SC_CLK_TCK")
NCPU = os.cpu_count()


def find_java_pid():
    out = subprocess.run(["pgrep", "-f", "io.kestra.cli.App"], capture_output=True, text=True).stdout.split()
    if not out:
        out = subprocess.run(["pgrep", "-x", "java"], capture_output=True, text=True).stdout.split()
    return int(out[0])


def proc_times(pid):
    with open(f"/proc/{pid}/stat") as f:
        p = f.read().rsplit(")", 1)[1].split()
    # utime, stime, cutime, cstime are fields 14,15,16,17 -> index 11..14 after the comm split
    return int(p[11]) + int(p[12]), int(p[13]) + int(p[14])


def cgroup_usec(container):
    cid = subprocess.run(["docker", "inspect", "-f", "{{.Id}}", container],
                         capture_output=True, text=True).stdout.strip()
    for base in (f"/sys/fs/cgroup/system.slice/docker-{cid}.scope/cpu.stat",):
        if os.path.exists(base):
            for line in open(base):
                if line.startswith("usage_usec"):
                    return int(line.split()[1]), base
    hits = glob.glob(f"/sys/fs/cgroup/**/docker-{cid}.scope/cpu.stat", recursive=True)
    if hits:
        for line in open(hits[0]):
            if line.startswith("usage_usec"):
                return int(line.split()[1]), hits[0]
    return None, None


def host_jiffies():
    with open("/proc/stat") as f:
        v = [int(x) for x in f.readline().split()[1:]]
    return sum(v), v[3] + v[4]


ap = argparse.ArgumentParser()
ap.add_argument("--workload", default="cpu")
ap.add_argument("--runner", default="process")
ap.add_argument("--per-exec", type=int, default=20)
ap.add_argument("--units", type=int, default=2000)
a = ap.parse_args()

jpid = find_java_pid()
print("kestra JVM host pid:", jpid, flush=True)
fid = f"tp_{a.runner}_{a.workload}_{a.per_exec}_m"
upsert_flow(fid, gen_flow(fid, a.workload, a.runner, a.per_exec))

pg0, pgpath = cgroup_usec("kbench-postgres-1")
own0, kids0 = proc_times(jpid)
h0, hi0 = host_jiffies()
t0 = time.time()
rows, meta = run_rep(fid, a.units // a.per_exec, "cpucheck2", 2400)
wall = time.time() - t0
h1, hi1 = host_jiffies()
own1, kids1 = proc_times(jpid)
pg1, _ = cgroup_usec("kbench-postgres-1")

r = analyse(rows, a.units)
dh = h1 - h0
host_cores = (1 - (hi1 - hi0) / dh) * NCPU
jvm_cores = (own1 - own0) / (wall * CLK)
task_cores = (kids1 - kids0) / (wall * CLK)
pg_cores = (pg1 - pg0) / (wall * 1e6) if pg0 is not None else None

out = {
 "combo": f"{a.runner}/{a.workload}",
 "note": "one extra rep, NOT part of the 5 measured reps; cores are averages over the whole rep wall time, so they are diluted by the brief idle head and tail",
 "wall_s": round(wall, 2),
 "rep": r,
 "cpu_attribution_cores": {
  "host_busy_total": round(host_cores, 2),
  "kestra_jvm_itself": round(jvm_cores, 2),
  "task_processes_reaped_children_of_jvm": round(task_cores, 2),
  "postgres_cgroup": round(pg_cores, 2) if pg_cores is not None else None,
  "host_ncpu": NCPU,
  "cgroup_path_used": pgpath,
 },
 "ideal_task_cpu_cores": None,
}
json.dump(out, open(f"/home/ubuntu/kbench/cpucheck2_{a.runner}_{a.workload}.json", "w"), indent=1)
print(json.dumps({k: out[k] for k in ("wall_s", "cpu_attribution_cores")}, indent=1))
print("throughput", r["throughput_per_s"], "span", r["span_s"], "exec_p50", r["exec_p50_s"], "peak", r["peak_concurrency"])
