import sys, json; sys.path.insert(0, "/home/ubuntu/kbench")
from kapi import req, NS
r = req("GET", f"/executions/search?namespace={NS}&size=5&sort=state.startDate:desc")
eid = r["results"][0]["id"]
e = req("GET", "/executions/" + eid)
print("outputs:", json.dumps((e.get("taskRunList") or [{}])[0].get("outputs"))[:800])
print("=== logs ===")
try:
    lg = req("GET", f"/logs/{eid}")
    print(len(lg), "log lines")
    for l in lg[:40]:
        print(l.get("taskId"), l.get("level"), (l.get("message") or "")[:160])
except Exception as ex:
    print("logerr", ex)
print("=== child executions? ===")
try:
    ch = req("GET", f"/executions/search?namespace={NS}&size=10&sort=state.startDate:desc")
    for x in ch["results"]:
        print(x["id"], x["flowId"], x["state"]["current"], x.get("trigger",{}).get("type"), x.get("labels"))
except Exception as ex:
    print(ex)
