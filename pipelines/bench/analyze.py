#!/usr/bin/env python3
"""Aggregate results.json into median+spread tables and a 2023 comparison.

Emits Markdown to stdout (redirect into results/RESULTS.md) and writes
results/summary.json. Pure aggregation — safe to re-run any time.
"""
import json
import os
import statistics as st

import config as C

RESULTS = os.path.join(C.RESULTS_DIR, "results.json")

# 2023 blog numbers, keyed by SF (1/5/10/25). Duration seconds / peak GB.
BASE_2023 = {
    "spark:aqe":        {"dur": {1: 285, 10: 1170}, "mem": {1: 7.26, 10: 11.7}},
    "polars:eager":     {"dur": {1: 42, 10: 370},   "mem": {1: 1.78, 10: 24.2}},
    "polars:lazy":      {"dur": {1: 247, 10: 2480}, "mem": {1: 2.11, 10: 10.4}},
    "duckdb:memory":    {"dur": {1: 61, 10: 560},   "mem": {1: 2.94, 10: 12.3}},
}
ORDER = ["spark:aqe", "duckdb:direct", "duckdb:memory", "duckdb:spill", "duckdb:disk",
         "polars:eager", "polars:lazy", "polars:streaming"]


def summarize(cfg):
    w = [r for r in cfg["runs"] if not r["cold"] and r["ok"]]
    cold = next((r for r in cfg["runs"] if r["cold"]), None)
    if not w:
        return None
    durs = [r["compute_s"] for r in w]
    mems = [r["peak_rss_gb"] for r in w if r["peak_rss_gb"]]
    return {
        "dur_median": round(st.median(durs), 2),
        "dur_min": round(min(durs), 2), "dur_max": round(max(durs), 2),
        "mem_median": round(st.median(mems), 2) if mems else None,
        "cold_dur": cold["compute_s"] if cold and cold["ok"] else None,
        "n": len(w),
    }


def main():
    results = json.load(open(RESULTS))
    by_sf = {}
    for cfg in results.values():
        s = summarize(cfg)
        if s:
            by_sf.setdefault(cfg["sf"], {})[f"{cfg['engine']}:{cfg['variant']}"] = s
    sfs = sorted(by_sf)

    print("## Duration — median of warm runs (s), [min–max], cold=first-run\n")
    print("| Config | " + " | ".join(f"SF{s}" for s in sfs) + " |")
    print("|---|" + "---|" * len(sfs))
    for cfg in ORDER:
        cells = []
        for sf in sfs:
            s = by_sf[sf].get(cfg)
            cells.append(f"**{s['dur_median']}** [{s['dur_min']}–{s['dur_max']}] c={s['cold_dur']}"
                         if s else "—")
        print(f"| {cfg} | " + " | ".join(cells) + " |")

    print("\n## Peak RSS — median of warm runs (GB)\n")
    print("| Config | " + " | ".join(f"SF{s}" for s in sfs) + " |")
    print("|---|" + "---|" * len(sfs))
    for cfg in ORDER:
        cells = [str(by_sf[sf].get(cfg, {}).get("mem_median", "—")) for sf in sfs]
        print(f"| {cfg} | " + " | ".join(cells) + " |")

    print("\n## vs 2023 — for reference only, NOT a like-for-like comparison\n")
    print("> Different hardware (unknown AWS m4 vs a Ryzen 9 9900X), different\n"
          "> storage (S3 vs MinIO-on-localhost), newer engines, and correctness\n"
          "> fixes all move at once. Do not read these as pure engine speedups.\n")
    print("| Config | SF1 dur 2023 → now (s) | SF10 dur 2023 → now (s) |")
    print("|---|---|---|")
    for cfg in ORDER:
        if cfg not in BASE_2023:
            continue
        row = [f"{BASE_2023[cfg]['dur'].get(sf)} → {by_sf.get(sf, {}).get(cfg, {}).get('dur_median', '—')}"
               for sf in (1, 10)]
        print(f"| {cfg} | " + " | ".join(row) + " |")

    print("\n## Failed / timed-out configs\n")
    fails = []
    for key, cfg in results.items():
        bad = [r for r in cfg["runs"] if not r["ok"]]
        if bad:
            reason = "TIMEOUT" if any(r.get("timed_out") for r in bad) else bad[0]["error"][:120].strip()
            fails.append(f"- **{key}**: {len(bad)}/{len(cfg['runs'])} runs failed — `{reason}`")
    print("\n".join(fails) if fails else "None — every config completed.")

    json.dump(by_sf, open(os.path.join(C.RESULTS_DIR, "summary.json"), "w"), indent=2)


if __name__ == "__main__":
    main()
