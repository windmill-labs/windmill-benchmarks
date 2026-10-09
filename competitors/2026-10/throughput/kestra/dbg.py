import sys, json; sys.path.insert(0, "/home/ubuntu/kbench")
from kapi import req, NS
r = req("GET", f"/executions/search?namespace={NS}&size=5&sort=state.startDate:desc")
for e in r["results"][:3]:
    print(e["id"], e["flowId"], e["state"]["current"], "taskruns:", len(e.get("taskRunList") or []))
e = req("GET", "/executions/" + r["results"][0]["id"])
print("full fetch taskruns:", len(e.get("taskRunList") or []))
print("keys:", list(e.keys()))
trs = e.get("taskRunList") or []
from collections import Counter
print(Counter(t["taskId"] for t in trs))
if trs:
    print(json.dumps(trs[0], indent=1)[:1200])
    print("...")
    print(json.dumps(trs[-1], indent=1)[:1200])
