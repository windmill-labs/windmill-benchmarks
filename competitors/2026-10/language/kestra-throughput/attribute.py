#!/usr/bin/env python3
"""One EXTRA, non-counted rep: while it runs, attribute host CPU between the
Docker daemon (dockerd + containerd + shims), the Kestra JVM, Postgres, and the
task containers themselves. Answers 'the 4 busy cores belong to whom'."""
import sys, os, json, time, threading, subprocess, glob, re
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from th import run_rep, analyse, _cpu_jiffies, NCPU

HZ = os.sysconf("SC_CLK_TCK")
FLOW = "tp_docker_cpu_20_m"
N_EXEC = 25

def pids_by_comm(pats):
    out = []
    for c in glob.glob("/proc/[0-9]*/comm"):
        pid = c.split("/")[2]
        try:
            nm = open(c).read().strip()
        except OSError:
            continue
        if any(nm.startswith(p) for p in pats):
            out.append((pid, nm))
    return out

def proc_cpu(pid):
    """utime+stime+cutime+cstime in seconds for one pid."""
    try:
        f = open(f"/proc/{pid}/stat").read()
    except OSError:
        return None
    rp = f.rindex(")") + 2
    v = f[rp:].split()
    # after state: ppid pgrp session tty tpgid flags minflt cminflt majflt cmajflt utime stime cutime cstime
    u, s, cu, cs = int(v[11]), int(v[12]), int(v[13]), int(v[14])
    return (u + s + cu + cs) / HZ

def cgroup_cpu(path):
    try:
        for l in open(path):
            if l.startswith("usage_usec"):
                return int(l.split()[1]) / 1e6
    except OSError:
        return None

def find_cg(container):
    cid = subprocess.run(f"docker inspect {container} --format '{{{{.Id}}}}'", shell=True,
                         capture_output=True, text=True).stdout.strip()
    for pat in (f"/sys/fs/cgroup/system.slice/docker-{cid}.scope/cpu.stat",
                f"/sys/fs/cgroup/docker/{cid}/cpu.stat"):
        if os.path.exists(pat):
            return pat
    hits = glob.glob(f"/sys/fs/cgroup/**/*{cid}*/cpu.stat", recursive=True)
    return hits[0] if hits else None

# cgroup roots: the whole docker hierarchy covers dockerd's children = task containers too,
# so take the daemon processes by pid and the two long-lived containers by their own cgroup.
CG = {"kestra_jvm_cgroup": find_cg("kbench-kestra-1"),
      "postgres_cgroup": find_cg("kbench-postgres-1")}
print("cgroups:", CG, flush=True)

DAEMON_PATS = ["dockerd", "containerd"]          # containerd + containerd-shim*
TASKCTR_PATS = ["python", "sh", "runc"]          # processes inside the task containers

class Attr(threading.Thread):
    """Accumulates CPU for pid groups that come and go, by remembering each
    pid's last-seen total and summing the increments."""
    def __init__(self, interval=0.5):
        super().__init__(daemon=True)
        self.interval = interval
        self.stop_flag = threading.Event()
        self.groups = {"docker_daemon": DAEMON_PATS, "task_containers": TASKCTR_PATS}
        self.acc = {g: 0.0 for g in self.groups}
        self.last = {}
        self.cg0 = {k: cgroup_cpu(v) for k, v in CG.items() if v}
        self.t0_host, self.i0_host = _cpu_jiffies()
        self.t_start = time.time()
        self.shims = []

    def tick(self):
        for g, pats in self.groups.items():
            for pid, nm in pids_by_comm(pats):
                c = proc_cpu(pid)
                if c is None:
                    continue
                k = (g, pid)
                prev = self.last.get(k)
                if prev is None:
                    self.last[k] = c          # first sight: start its clock here
                else:
                    if c >= prev:
                        self.acc[g] += c - prev
                    self.last[k] = c
        self.shims.append(len([1 for _, nm in pids_by_comm(["containerd-shim"])]))

    def run(self):
        self.tick()
        while not self.stop_flag.wait(self.interval):
            self.tick()

    def finish(self):
        self.stop_flag.set(); self.join(timeout=5)
        self.tick()
        el = time.time() - self.t_start
        t1, i1 = _cpu_jiffies()
        dt, di = t1 - self.t0_host, i1 - self.i0_host
        host = round((1 - di / dt) * NCPU, 2)
        out = {"elapsed_s": round(el, 2), "host_cores_busy_whole_sampler_window": host,
               "sampler_hz": round(1 / self.interval, 2),
               "shims_p50": sorted(self.shims)[len(self.shims)//2],
               "shims_max": max(self.shims)}
        for g in self.groups:
            out[g + "_cores"] = round(self.acc[g] / el, 2)
        for k, v in CG.items():
            if v:
                out[k.replace("_cgroup", "") + "_cores"] = round((cgroup_cpu(v) - self.cg0[k]) / el, 2)
        return out

a = Attr()
a.start()
rows, meta = run_rep(FLOW, N_EXEC, "attr", 900)
res = analyse(rows, 500)
att = a.finish()
att["rep_throughput_per_s"] = res["throughput_per_s"]
att["rep_span_s"] = res["span_s"]
att["rep_mean_concurrency"] = res["mean_concurrency"]
att["note"] = ("Extra rep, NOT counted in the reported throughput. Process-group CPU is utime+stime+"
               "cutime+cstime summed over short-lived pids, so a container's python is captured when its "
               "shim reaps it; dividing by the sampler window gives cores. The Kestra JVM and Postgres "
               "come from their own container cgroups (cpu.stat usage_usec).")
json.dump(att, open(os.path.expanduser("~/kbench/attribution.json"), "w"), indent=1)
print(json.dumps(att, indent=1))
