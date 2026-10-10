import sys, json, time; sys.path.insert(0, "/home/ubuntu/kbench")
from kapi import req, NS, upsert_flow, trigger, wait_exec

def run(fid, yml):
    upsert_flow(fid, yml)
    eid = trigger(fid)
    ex = wait_exec(eid, timeout=180, poll=0.5)
    trs = ex.get("taskRunList") or []
    print(f"== {fid}: state={ex['state']['current']} dur={ex['state'].get('duration')} taskruns={len(trs)}")
    for t in trs[:6]:
        print("   ", t["taskId"], t.get("value"), t["state"]["current"], t["state"].get("duration"))
    try:
        lg = req("GET", f"/logs/{eid}")
        print("   logs:", len(lg))
        for l in lg[:8]:
            print("     ", l.get("taskId"), l.get("level"), (l.get("message") or "")[:100])
    except Exception as e:
        print("   logerr", e)

run("dbg_loop", """id: dbg_loop
namespace: company.team
tasks:
  - id: fanout
    type: io.kestra.plugin.core.flow.Loop
    values: '{{ range(1, 3) }}'
    concurrencyLimit: 0
    tasks:
      - id: unit
        type: io.kestra.plugin.core.log.Log
        message: "hello {{ taskrun.value }}"
""")

run("dbg_par", """id: dbg_par
namespace: company.team
tasks:
  - id: fanout
    type: io.kestra.plugin.core.flow.Parallel
    tasks:
      - id: u0
        type: io.kestra.plugin.core.log.Log
        message: a
      - id: u1
        type: io.kestra.plugin.core.log.Log
        message: b
      - id: u2
        type: io.kestra.plugin.core.log.Log
        message: c
""")

run("dbg_loop_lit", """id: dbg_loop_lit
namespace: company.team
tasks:
  - id: fanout
    type: io.kestra.plugin.core.flow.Loop
    values:
      - "1"
      - "2"
      - "3"
    concurrencyLimit: 0
    tasks:
      - id: unit
        type: io.kestra.plugin.core.log.Log
        message: "hello {{ taskrun.value }}"
""")
