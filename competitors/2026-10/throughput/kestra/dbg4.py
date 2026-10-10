import sys, json; sys.path.insert(0, "/home/ubuntu/kbench")
from kapi import req, NS
# all executions, newest first
r = req("GET", f"/executions/search?size=50&sort=state.startDate:desc")
print("total executions:", r["total"])
from collections import Counter
print(Counter((x["flowId"], x["state"]["current"]) for x in r["results"]))
# find a child of the sleep probe
for x in r["results"]:
    if x["flowId"].startswith("tp_loop") or "sleep" in x["flowId"]:
        print("candidate", x["id"], x["flowId"], x["state"]["current"], x.get("labels"))
# fetch a failed Log child
for x in r["results"]:
    if x["state"]["current"] == "FAILED":
        e = req("GET", "/executions/" + x["id"])
        print("FAILED child", x["id"], x["flowId"], json.dumps(e.get("labels")))
        lg = req("GET", f"/logs/{x['id']}")
        for l in lg[:10]:
            print("   ", l.get("taskId"), l.get("level"), (l.get("message") or "")[:300])
        break
