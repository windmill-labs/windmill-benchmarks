#!/usr/bin/env python3
"""Generate TPC-H-derived Parquet datasets and upload them to S3/MinIO.

Uses DuckDB's `tpch` extension (`CALL dbgen`) into an on-disk database (so large
scale factors don't exhaust RAM), writes each table to Parquet (zstd) with
UPPERCASE columns and DECIMAL->DOUBLE (so all engines read an identical float
representation), then uploads to <bucket>/tpc-h/<sf>/raw/<table>.parquet.

Usage: python gen_data.py [<sf> ...] [--keep-local]
       (no SF args => config.SCALE_FACTORS)
"""
import os
import subprocess
import sys
import time

import duckdb
import config as C


def gen(sf, mem_limit="16GB"):
    raw = os.path.join(C.DATA_DIR, f"sf{sf}", "raw")
    os.makedirs(raw, exist_ok=True)
    tmp = os.path.join(C.SPILL_DIR, "duckdb_gen")
    os.makedirs(tmp, exist_ok=True)
    # On-disk DB so dbgen spills to disk instead of getting OOM-killed at SF>=50.
    db_path = os.path.join(tmp, f"gen_sf{sf}.duckdb")
    for p in (db_path, db_path + ".wal"):
        if os.path.exists(p):
            os.remove(p)
    con = duckdb.connect(db_path)
    con.execute(f"SET memory_limit='{mem_limit}'")
    con.execute(f"SET temp_directory='{tmp}'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("INSTALL tpch; LOAD tpch")
    print(f"[sf{sf}] dbgen sf={sf} ...", flush=True)
    t0 = time.time()
    con.execute(f"CALL dbgen(sf={sf})")
    print(f"[sf{sf}] dbgen done in {time.time()-t0:.1f}s", flush=True)
    for t in C.TABLES:
        rows = con.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            f"WHERE table_name='{t}' ORDER BY ordinal_position").fetchall()
        parts = [f'{"CAST("+c+" AS DOUBLE)" if dt.upper().startswith("DECIMAL") else c} AS "{c.upper()}"'
                 for c, dt in rows]
        path = os.path.join(raw, f"{t}.parquet")
        con.execute(f"COPY (SELECT {', '.join(parts)} FROM {t}) TO '{path}' "
                    f"(FORMAT parquet, COMPRESSION zstd)")
        print(f"[sf{sf}]   {t:9s} -> {os.path.getsize(path)/1e6:8.1f} MB", flush=True)
    con.close()
    for p in (db_path, db_path + ".wal"):
        if os.path.exists(p):
            os.remove(p)


def upload(sf):
    src = os.path.join(C.DATA_DIR, f"sf{sf}", "raw") + "/"
    dst = f"local/{C.S3_ENV['S3_BUCKET']}/tpc-h/{sf}/raw/"
    subprocess.run([C.MC_BIN, "rm", "--recursive", "--force", dst],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run([C.MC_BIN, "cp", "--recursive", src, dst], check=True)
    print(f"[sf{sf}] uploaded to {dst}", flush=True)


if __name__ == "__main__":
    argv = sys.argv[1:]
    keep = "--keep-local" in argv
    sfs = [int(a) for a in argv if not a.startswith("--")] or C.SCALE_FACTORS
    for sf in sfs:
        gen(sf)
        upload(sf)
        if not keep:
            subprocess.run(["rm", "-rf", os.path.join(C.DATA_DIR, f"sf{sf}")])
    print("gen+upload done", flush=True)
