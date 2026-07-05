#!/usr/bin/env python3
"""Benchmark orchestrator.

For each (engine, variant, scale factor) in config.py, runs the engine script
under GNU `time -v` to capture peak RSS, doing 1 cold run (page cache dropped)
and N warm runs. Records per-run wall time, engine-compute total (from
BENCH_OUT), and peak RSS. Resumable: skips configs already marked done in
results.json.

Usage: python orchestrate.py [<sf> ...]  [--warm N]
       (no SF args => config.SCALE_FACTORS)
"""
import json
import os
import re
import signal
import subprocess
import sys
import time

import config as C

RESULTS = os.path.join(C.RESULTS_DIR, "results.json")


def build_cmd(engine, variant, sf, bench_out):
    env = dict(os.environ)
    env.update(C.S3_ENV)
    env["SCALE_FACTOR"] = str(sf)
    env["BENCH_OUT"] = bench_out
    env["TMPDIR"] = C.SPILL_DIR
    env["POLARS_TEMP_DIR"] = C.SPILL_DIR
    if engine == "duckdb":
        env["DUCKDB_VARIANT"] = variant
        env["DUCKDB_TEMP"] = os.path.join(C.SPILL_DIR, "duckdb")
        env["DUCKDB_MEM_LIMIT"] = (C.DDB_SPILL_MEM if variant in ("spill", "direct") else C.DDB_DISK_MEM)[sf]
        cmd = [C.PY, os.path.join(C.REPO_PIPELINES, "duckdb", "tpc_h.py")]
    elif engine == "polars":
        env["POLARS_VARIANT"] = variant
        cmd = [C.PY, os.path.join(C.REPO_PIPELINES, "polars", "tpc_h.py")]
    elif engine == "spark":
        env["JAVA_HOME"] = C.JAVA_HOME
        cmd = [os.path.join(C.SPARK_HOME, "bin", "spark-submit"),
               "--packages", C.HADOOP_AWS,
               "--conf", f"spark.jars.ivy={C.TOOLS_DIR}/.ivy2",
               "--conf", f"spark.driver.memory={C.SPARK_MEM[sf]}",
               "--conf", f"spark.local.dir={C.SPILL_DIR}/spark",
               "--conf", "spark.ui.enabled=false",
               "--conf", "spark.sql.adaptive.enabled=true",
               os.path.join(C.REPO_PIPELINES, "spark", "tpc_h.py")]
    else:
        raise ValueError(engine)
    return cmd, env


def drop_caches():
    subprocess.run("sync", shell=True)
    subprocess.run("echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null", shell=True)


def one_run(engine, variant, sf, cold):
    bench_out = f"/tmp/bench_{engine}_{variant}_{sf}.json"
    if os.path.exists(bench_out):
        os.remove(bench_out)
    cmd, env = build_cmd(engine, variant, sf, bench_out)
    os.makedirs(C.SPILL_DIR, exist_ok=True)
    if cold:
        drop_caches()
    timed = ["/usr/bin/time", "-v"] + cmd
    t0 = time.time()
    timed_out = False
    # own process group so a timeout kills the whole tree (spark-submit -> java),
    # never leaving an orphan to pollute the next config's measurement.
    p = subprocess.Popen(timed, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         text=True, start_new_session=True)
    try:
        _, stderr = p.communicate(timeout=C.TIMEOUT[sf])
        returncode = p.returncode
    except subprocess.TimeoutExpired:
        timed_out = True
        try:
            os.killpg(os.getpgid(p.pid), signal.SIGKILL)
        except ProcessLookupError:
            pass
        _, stderr = p.communicate()
        returncode = -1
    wall = time.time() - t0
    stderr = stderr or ""
    rss_kb = None
    for line in stderr.splitlines():
        m = re.search(r"Maximum resident set size \(kbytes\):\s*(\d+)", line)
        if m:
            rss_kb = int(m.group(1))
    compute = None
    meta = {}
    if os.path.exists(bench_out):
        d = json.load(open(bench_out))
        compute = d["timings"]["total"]
        meta = {k: v for k, v in d.items() if k != "timings"}
        meta["per_query"] = {k: v for k, v in d["timings"].items() if k.startswith("query_")}
        meta["load"] = d["timings"].get("load")
    ok = returncode == 0 and compute is not None and not timed_out
    err = "" if ok else ("TIMEOUT" if timed_out else (stderr[-2000:] if stderr else "no BENCH_OUT"))
    return {
        "cold": cold, "ok": ok, "timed_out": timed_out, "wall_s": round(wall, 2),
        "compute_s": round(compute, 3) if compute else None,
        "peak_rss_gb": round(rss_kb / 1024 / 1024, 3) if rss_kb else None,
        "meta": meta, "error": err,
    }


def load_results():
    return json.load(open(RESULTS)) if os.path.exists(RESULTS) else {}


def save_results(r):
    os.makedirs(C.RESULTS_DIR, exist_ok=True)
    with open(RESULTS, "w") as f:
        json.dump(r, f, indent=2)


def main():
    argv = sys.argv[1:]
    warm_override = None
    if "--warm" in argv:
        i = argv.index("--warm")
        warm_override = int(argv[i + 1])
        del argv[i:i + 2]
    sfs = [int(a) for a in argv if not a.startswith("--")] or C.SCALE_FACTORS
    results = load_results()
    for sf in sfs:
        warm = warm_override if warm_override is not None else C.WARM.get(sf, 5)
        for engine, variant in C.CONFIGS:
            key = f"{engine}:{variant}:sf{sf}"
            if results.get(key, {}).get("done"):
                print(f"skip {key} (done)", flush=True)
                continue
            print(f"\n=== {key} ===", flush=True)
            runs = []
            for i in range(warm + 1):
                cold = (i == 0)
                r = one_run(engine, variant, sf, cold)
                tag = "cold" if cold else f"warm{i}"
                print(f"  {tag}: {'OK' if r['ok'] else 'FAIL'} compute={r['compute_s']}s "
                      f"wall={r['wall_s']}s rss={r['peak_rss_gb']}GB", flush=True)
                if not r["ok"]:
                    print("    ERR:", r["error"][:400], flush=True)
                runs.append(r)
                results[key] = {"engine": engine, "variant": variant, "sf": sf,
                                "runs": runs, "done": False}
                save_results(results)
                if not r["ok"] and (r.get("timed_out") or cold):
                    print(f"    stopping {key} early "
                          f"({'timeout' if r.get('timed_out') else 'cold failure'})", flush=True)
                    break
            results[key]["done"] = True
            save_results(results)
    print("\nDONE", flush=True)


if __name__ == "__main__":
    main()
