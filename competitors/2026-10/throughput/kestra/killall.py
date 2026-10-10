import sys; sys.path.insert(0, "/home/ubuntu/kbench")
from kapi import req
r = req("GET", "/executions/search?size=100&sort=state.startDate:desc")
for x in r["results"]:
    if x["state"]["current"] in ("RUNNING", "CREATED", "QUEUED", "PAUSED", "RESTARTED", "SUBMITTED"):
        try:
            req("POST", f"/executions/{x['id']}/kill", b"")
            print("killed", x["id"], x["flowId"])
        except Exception as e:
            print("killerr", x["id"], str(e)[:200])
