#!/usr/bin/env python3
"""Cross-check that all engines return identical results at a given SF.

DuckDB (canonical SQL) is the reference; Polars (lazy) is compared in-process.
Spark is compared if a dump exists (run spark/tpc_h.py with SPARK_VALIDATE_DIR).
Comparison is column-order-independent with 6-significant-figure float
tolerance (float vs decimal reduction-order differences are not real mismatches).

Usage: python validate.py [<sf>]   (default 1)
"""
import importlib.util
import json
import math
import os
import sys

import config as C

SF = sys.argv[1] if len(sys.argv) > 1 else "1"
os.environ.update(C.S3_ENV)
os.environ["SCALE_FACTOR"] = str(SF)
os.environ["POLARS_VARIANT"] = "lazy"


def _load(name, rel):
    spec = importlib.util.spec_from_file_location(name, os.path.join(C.REPO_PIPELINES, rel))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def sig(x, n=6):
    x = float(x)
    if x == 0:
        return 0.0
    return round(x, n - int(math.floor(math.log10(abs(x)))) - 1)


def norm(rows):
    out = []
    for r in rows:
        vals = []
        for v in r:
            try:
                vals.append(("n", sig(v)))
            except (TypeError, ValueError):
                vals.append(("s", str(v).strip()))
        out.append(tuple(sorted(vals, key=lambda e: (e[0], str(e[1])))))
    return sorted(out, key=lambda x: tuple(str(e) for e in x))


def main():
    ddb = _load("ddb", "duckdb/tpc_h.py")
    pol = _load("pol", "polars/tpc_h.py")
    con = ddb.connect()
    ddb.load_tables(con)
    t = pol.load_tables()
    spark = None
    sp = os.path.join(os.environ.get("SPARK_VALIDATE_DIR", "/tmp"), f"spark_sf{SF}.json")
    if os.path.exists(sp):
        spark = json.load(open(sp))
    ok = True
    for n in range(1, 10):
        ref = norm(con.execute(ddb.QUERIES[n]).fetchall())
        got = norm(pol.collect(pol.QUERIES[n - 1](t)).rows())
        same = ref == got
        line = f"query_{n}: polars={'PASS' if same else 'FAIL'}"
        if spark is not None:
            ssame = norm(spark[str(n)]) == ref
            same = same and ssame
            line += f" spark={'PASS' if ssame else 'FAIL'}"
        ok = ok and same
        print(line)
    print("ALL PASS" if ok else "SOME FAILED")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
