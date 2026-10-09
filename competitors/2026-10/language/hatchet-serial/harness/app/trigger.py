import sys, time, json
from bench_defs import WORKFLOWS

wl = sys.argv[1]
wf = WORKFLOWS[wl]
t = time.time()
ref = wf.run_no_wait()
run_id = ref.workflow_run_id
res = ref.result()
print(json.dumps({"workload": wl, "run_id": run_id, "wall_s": round(time.time()-t, 3), "keys": sorted(res.keys())[:5], "n": len(res)}))
