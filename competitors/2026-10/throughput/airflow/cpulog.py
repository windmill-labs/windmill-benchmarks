#!/usr/bin/env python3
"""Logs cumulative /proc/stat jiffies once a second: ts total idle."""
import time, sys
f = open("/home/ubuntu/af/cpulog.tsv", "a", buffering=1)
while True:
    with open("/proc/stat") as s:
        for line in s:
            if line.startswith("cpu "):
                v = [int(x) for x in line.split()[1:]]
                f.write("%.3f\t%d\t%d\n" % (time.time(), sum(v), v[3] + v[4]))
                break
    time.sleep(1.0)
