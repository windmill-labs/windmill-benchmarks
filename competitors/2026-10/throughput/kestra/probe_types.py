#!/usr/bin/env python3
import sys; sys.path.insert(0, "/home/ubuntu/kbench")
from kapi import validate

CAND = [
    "io.kestra.plugin.core.flow.ForEach",
    "io.kestra.plugin.core.flow.Parallel",
    "io.kestra.plugin.core.flow.EachParallel",
    "io.kestra.plugin.core.flow.Loop",
    "io.kestra.plugin.core.flow.Dag",
    "io.kestra.plugin.core.flow.Subflow",
    "io.kestra.plugin.core.flow.ForEachItem",
]
for t in CAND:
    y = f"""id: probe_type
namespace: company.team
tasks:
  - id: outer
    type: {t}
    values: '{{{{ range(0, 2) }}}}'
    concurrencyLimit: 2
    tasks:
      - id: inner
        type: io.kestra.plugin.core.log.Log
        message: hi
"""
    try:
        r = validate(y)
        c = r[0].get("constraints") if isinstance(r, list) else r.get("constraints")
        print(f"{t}: constraints={c}")
    except Exception as e:
        print(f"{t}: ERR {e}")
