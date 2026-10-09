#!/usr/bin/env python3
"""Quantify how much of the 2000-unit set was enqueued (CREATED) before the
run's first RUNNING, from the dumped per-task-run timestamps."""
import json, sys, os
SP = os.path.dirname(os.path.abspath(__file__))
for name in sys.argv[1:]:
    p = os.path.join(SP, name)
    if not os.path.exists(p):
        print("missing", name); continue
    reps = json.load(open(p))
    print("==", name)
    for i, rows in enumerate(reps, 1):
        cr = [r[0] for r in rows]; ru = [r[1] for r in rows]; su = [r[2] for r in rows]
        t0 = min(ru); span = max(su) - t0
        before = sum(1 for c in cr if c <= t0)
        print(f"  rep{i}: span={span:.2f}s  all CREATED by t0+{max(cr)-t0:+.2f}s  "
              f"created_before_first_start={before}/{len(rows)}  "
              f"created_within_first_10pct_of_span={sum(1 for c in cr if c-t0 <= 0.1*span)}/{len(rows)}")
