import sys, json, collections; sys.path.insert(0, "/home/ubuntu/kbench")
from kapi import req
r = req("GET", "/executions/search?size=5&sort=state.startDate:desc")
x = r["results"][0]
print(x["id"], x["flowId"], x["state"]["current"], x["state"].get("startDate"))
e = req("GET", "/executions/" + x["id"])
trs = e.get("taskRunList") or []
print("taskruns:", len(trs), "json bytes:", len(json.dumps(e)))
print(collections.Counter(t["state"]["current"] for t in trs))
