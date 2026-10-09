import sys, json, collections; sys.path.insert(0, "/home/ubuntu/kbench")
from kapi import req, NS
for fid in ("tp_process_sleep_50_sw", "tp_process_sleep_200_sw"):
    r = req("GET", f"/executions/search?namespace={NS}&flowId={fid}&size=3&sort=state.startDate:desc")
    print("==", fid, "total", r["total"])
    if not r["results"]: continue
    e = req("GET", "/executions/" + r["results"][0]["id"])
    trs = e.get("taskRunList") or []
    print("   taskruns:", len(trs), "state", e["state"]["current"])
    print("   ", collections.Counter(t["taskId"] for t in trs).most_common(5))
    ids = sorted(set(t["taskId"] for t in trs))
    print("   distinct ids:", len(ids), ids[:5], ids[-3:])
    f = req("GET", f"/flows/{NS}/{fid}")
    print("   flow task count:", len(f["tasks"][0].get("tasks") or []), "concurrent:", f["tasks"][0].get("concurrent"))
