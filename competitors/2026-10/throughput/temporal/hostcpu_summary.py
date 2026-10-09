import glob, os, statistics, sys
for f in sorted(glob.glob(os.path.join(sys.argv[1], "hostcpu_*.txt"))):
    v = [float(l.split()[1]) for l in open(f) if len(l.split()) == 2]
    if not v:
        print(os.path.basename(f), "no samples"); continue
    v2 = sorted(v)
    print("%-48s n=%3d median=%5.2f p90=%5.2f max=%5.2f cores busy (of 32)" % (
        os.path.basename(f), len(v), statistics.median(v), v2[int(0.9*len(v2))-1], max(v)))
