#!/usr/bin/env python3
"""One extra, non-counted rep that attributes CPU: how many cores the Python
task processes burn vs how many the Kestra JVM and Postgres burn."""
import sys, os, json, time, glob, threading, argparse
sys.path.insert(0, "/home/ubuntu/kbench")
import th
from th import run_rep, analyse, gen_flow
from kapi import upsert_flow

CLK = os.sysconf("SC_CLK_TCK")
NCPU = os.cpu_count()


def task_proc_jiffies():
    """utime+stime over all python processes that are running a Kestra task
    script (cmdline points at the Kestra working dir), plus their count."""
    tot, n = 0, 0
    for st in glob.glob("/proc/[0-9]*/stat"):
        pid = st.split("/")[2]
        try:
            with open(f"/proc/{pid}/comm") as f:
                if not f.read().startswith("python"):
                    continue
            with open(f"/proc/{pid}/cmdline", "rb") as f:
                cl = f.read().decode(errors="replace")
            if "kestra-wd" not in cl:
                continue
            with open(st) as f:
                p = f.read().rsplit(")", 1)[1].split()
            tot += int(p[11]) + int(p[12])  # utime, stime
            n += 1
        except (OSError, IndexError, ValueError):
            continue
    return tot, n


def cg_usage(name):
    for p in glob.glob(f"/sys/fs/cgroup/system.slice/docker-*.scope/cpu.stat"):
        pass
    return None


def host_jiffies():
    with open("/proc/stat") as f:
        v = [int(x) for x in f.readline().split()[1:]]
    return sum(v), v[3] + v[4]


class S(threading.Thread):
    def __init__(self, interval=0.25):
        super().__init__(daemon=True)
        self.i = interval
        self.stop = threading.Event()
        self.host_cores, self.task_cores, self.nproc = [], [], []

    def run(self):
        ht, hi = host_jiffies()
        tj, _ = task_proc_jiffies()
        while not self.stop.wait(self.i):
            ht2, hi2 = host_jiffies()
            tj2, n = task_proc_jiffies()
            dt = ht2 - ht
            if dt > 0:
                self.host_cores.append(round((1 - (hi2 - hi) / dt) * NCPU, 2))
                self.task_cores.append(round((tj2 - tj) / (self.i * CLK), 2))
            self.nproc.append(n)
            ht, hi, tj = ht2, hi2, tj2

    def report(self):
        self.stop.set(); self.join(timeout=5)
        f = lambda xs: {"p50": sorted(xs)[len(xs)//2], "p90": sorted(xs)[int(len(xs)*0.9)],
                        "max": max(xs), "n": len(xs)} if xs else None
        # trim the ramp-up / drain samples
        hc, tc, npr = self.host_cores, self.task_cores, self.nproc
        k = max(1, len(hc)//10)
        return {"host_busy_cores": f(hc[k:-k] or hc), "task_python_cores": f(tc[k:-k] or tc),
                "live_task_processes": f(npr[k:-k] or npr), "host_ncpu": NCPU,
                "host_busy_cores_series": hc, "task_python_cores_series": tc,
                "live_task_processes_series": npr}


ap = argparse.ArgumentParser()
ap.add_argument("--workload", default="cpu")
ap.add_argument("--runner", default="process")
ap.add_argument("--per-exec", type=int, default=20)
ap.add_argument("--units", type=int, default=2000)
a = ap.parse_args()

fid = f"tp_{a.runner}_{a.workload}_{a.per_exec}_m"
upsert_flow(fid, gen_flow(fid, a.workload, a.runner, a.per_exec))
s = S(); s.start()
rows, meta = run_rep(fid, a.units // a.per_exec, "cpucheck", 2400)
r = analyse(rows, a.units)
rep = s.report()
out = {"combo": f"{a.runner}/{a.workload}", "rep": r, "meta": {k: v for k, v in meta.items()},
       "cpu_attribution": rep}
json.dump(out, open(f"/home/ubuntu/kbench/cpucheck_{a.runner}_{a.workload}.json", "w"), indent=1)
print(json.dumps({k: v for k, v in rep.items() if not k.endswith("series")}, indent=1))
print("throughput", r["throughput_per_s"], "exec_p50", r["exec_p50_s"], "peak", r["peak_concurrency"])
