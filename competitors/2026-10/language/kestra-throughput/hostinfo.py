#!/usr/bin/env python3
"""Collect version / host facts for the runtime file. Run AFTER the measured reps."""
import json, subprocess, re, os, datetime

sh = lambda c: subprocess.run(c, shell=True, capture_output=True, text=True).stdout.strip()

logs = sh("docker logs kbench-kestra-1 2>&1 | head -400")
ansi = re.compile(r"\x1b\[[0-9;]*m")
clean = ansi.sub("", logs)

ver = re.search(r"Starting Kestra ([0-9.]+) with environments \[([^\]]*)\] \[revision (\w+) / ([0-9T:\-]+)\]", clean)
wl = [l.strip() for l in clean.splitlines()
      if "Worker started with" in l or "Connected to controller" in l or "SystemWorker subscribed" in l]
wl = [re.sub(r"^\d\d:\d\d:\d\d\.\d+\s+\w+\s+\S+\s+", "", l) for l in wl]

lscpu = sh("lscpu")
g = lambda k: (re.search(rf"^{k}:\s*(.+)$", lscpu, re.M) or [None, None])[1]
vcpu = int(g("CPU\\(s\\)"))
tpc = float(g("Thread\\(s\\) per core"))
sockets = int(g("Socket\\(s\\)")); cps = int(g("Core\\(s\\) per socket"))

wt = sh("docker inspect kbench-kestra-1 --format '{{json .Config.Cmd}}'")

out = {
 "run_date": datetime.date.today().isoformat(),
 "kestra_version": ver.group(1) if ver else None,
 "kestra_commit_id": ver.group(3) if ver else None,
 "kestra_commit_date": ver.group(4) if ver else None,
 "worker_logs": wl,
 "compose_command": wt,
 "docker_version": sh("docker version --format '{{.Server.Version}}'"),
 "image_digest": sh("docker image inspect python:slim --format '{{.Id}}'"),
 "image_created": sh("docker image inspect python:slim --format '{{.Created}}'"),
 "host_cpu": {
   "model": (g("Model name") or "").strip(),
   "vcpus": vcpu,
   "physical_cores": sockets * cps,
   "threads_per_core": tpc,
   "why_it_matters": ("32 vCPU is 16 physical cores with 2 SMT threads each, so 20 concurrent CPU-bound "
                      "tasks do not get 20 independent cores"),
 },
 "uptime_after_run": sh("uptime"),
 "nothing_else_running": sh("docker ps --format '{{.Names}} {{.Image}}'").splitlines(),
}
json.dump(out, open(os.path.expanduser("~/kbench/hostinfo.json"), "w"), indent=1)
print(json.dumps(out, indent=1))
