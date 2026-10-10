import glob, json, os, statistics, sys

EXPECT = {"fibonacci_40_10": 40, "fibonacci_10_33": 10}
COMPUTE = {"py": 0.3936, "ts": 0.0321}   # measured fibo(33) in-container, seconds
ok = True
rows = []
for path in sorted(glob.glob(os.path.join(S := os.path.dirname(os.path.abspath(__file__)), "temporal_*_*_fibonacci_*.json"))):
    d = json.load(open(path))
    lang, cfg, wl = d["lang"], d["config"], d["workload"]
    n = EXPECT[wl]
    errs = []
    if len(d["reps"]) != 5:
        errs.append("expected 5 reps, got %d" % len(d["reps"]))
    totals = []
    for i, r in enumerate(d["reps"]):
        c, s, e = r["created_at"], r["started_at"], r["completed_at"]
        if not (len(c) == len(s) == len(e) == n):
            errs.append("rep%d: activity count %d/%d/%d != %d" % (i, len(c), len(s), len(e), n))
            continue
        if c[0] != 0.0:
            errs.append("rep%d: created_at[0]=%r != 0.0" % (i, c[0]))
        for j in range(n):
            if not (c[j] <= s[j] <= e[j]):
                errs.append("rep%d act%d: not monotonic within activity %r" % (i, j, (c[j], s[j], e[j])))
        for j in range(1, n):
            if c[j] < c[j-1] or s[j] < s[j-1] or e[j] < e[j-1]:
                errs.append("rep%d act%d: series not monotonic" % (i, j))
            if s[j] < e[j-1]:
                errs.append("rep%d: act%d started %.3f before act%d completed %.3f (OVERLAP)" % (i, j, s[j], j-1, e[j-1]))
        if r.get("workers") != 1:
            errs.append("rep%d: workers != 1" % i)
        totals.append(e[-1] - c[0])
    med = statistics.median(totals)
    per = med / n * 1000.0
    comp_ms = COMPUTE[lang] * 1000.0 if wl == "fibonacci_10_33" else 0.0
    rows.append((lang, cfg, wl, med, min(totals), max(totals), per, per - comp_ms, totals))
    if errs:
        ok = False
        print("FAIL %s:" % os.path.basename(path))
        for e_ in errs[:8]:
            print("   ", e_)
    else:
        print("ok   %-46s n=%d reps=5 monotonic, no overlap" % (os.path.basename(path), n))

print()
hdr = "%-4s %-6s %-16s %8s %8s %8s %9s %10s" % ("lang","config","workload","median","min","max","per-act","minus-cpu")
print(hdr); print("-"*len(hdr))
for lang,cfg,wl,med,mn,mx,per,net,tot in rows:
    print("%-4s %-6s %-16s %8.3f %8.3f %8.3f %7.1fms %8.1fms" % (lang,cfg,wl,med,mn,mx,per,net))
print()
for lang,cfg,wl,med,mn,mx,per,net,tot in rows:
    print("%-4s %-6s %-16s totals: %s" % (lang,cfg,wl,[round(t,3) for t in tot]))
print()
print("VALIDATION:", "ALL PASS" if ok else "FAILURES ABOVE")
