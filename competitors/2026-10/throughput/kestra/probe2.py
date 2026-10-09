#!/usr/bin/env python3
import sys; sys.path.insert(0, "/home/ubuntu/kbench")
from kapi import validate

def show(label, y):
    try:
        r = validate(y)
        c = r[0].get("constraints") if isinstance(r, list) else r.get("constraints")
        print(f"--- {label}: {c}")
    except Exception as e:
        print(f"--- {label}: ERR {e}")

show("Loop props", """id: p
namespace: company.team
tasks:
  - id: outer
    type: io.kestra.plugin.core.flow.Loop
    values: '{{ range(0, 2) }}'
    zzz: 1
    tasks:
      - id: inner
        type: io.kestra.plugin.core.log.Log
        message: hi
""")

show("flow-level concurrency", """id: p
namespace: company.team
concurrency:
  limit: 20
  behavior: QUEUE
  zzz: 1
tasks:
  - id: inner
    type: io.kestra.plugin.core.log.Log
    message: hi
""")

show("flow concurrency ok", """id: p
namespace: company.team
concurrency:
  limit: 20
  behavior: QUEUE
tasks:
  - id: inner
    type: io.kestra.plugin.core.log.Log
    message: hi
""")

show("Parallel concurrent", """id: p
namespace: company.team
tasks:
  - id: outer
    type: io.kestra.plugin.core.flow.Parallel
    concurrent: 20
    tasks:
      - id: inner
        type: io.kestra.plugin.core.log.Log
        message: hi
""")

show("Loop concurrencyLimit 0", """id: p
namespace: company.team
tasks:
  - id: outer
    type: io.kestra.plugin.core.flow.Loop
    values: '{{ range(0, 2) }}'
    concurrencyLimit: 0
    tasks:
      - id: inner
        type: io.kestra.plugin.core.log.Log
        message: hi
""")

show("Process runner on python script", """id: p
namespace: company.team
tasks:
  - id: inner
    type: io.kestra.plugin.scripts.python.Script
    taskRunner:
      type: io.kestra.plugin.core.runner.Process
    script: |
      print(1)
""")
