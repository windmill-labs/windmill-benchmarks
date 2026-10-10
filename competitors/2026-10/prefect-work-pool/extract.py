#!/usr/bin/env python3
"""Read per-unit timings for one rep out of Prefect's database.

Emits {workers, created_at[], started_at[], completed_at[]} in seconds,
relative to the first unit's created, matching the schema the docs site
consumes for every other engine.
"""
import json
import subprocess
import sys

SQL = """
select fr.name,
       extract(epoch from fr.created),
       extract(epoch from (select min(s.timestamp) from flow_run_state s
                            where s.flow_run_id = fr.id and s.type = 'RUNNING')),
       extract(epoch from (select max(s.timestamp) from flow_run_state s
                            where s.flow_run_id = fr.id and s.type = 'COMPLETED'))
from flow_run fr
where fr.tags::jsonb ? '{tag}'
order by fr.created
"""


def fetch(ssh, tag):
    sql = SQL.format(tag=tag).replace("\n", " ")
    out = subprocess.run(
        ssh + [f'cd ~; sudo docker compose exec -T db psql -U prefect -d prefect -At -F"|" -c "{sql}"'],
        capture_output=True, text=True, check=True,
    ).stdout.strip()
    rows = []
    for line in out.splitlines():
        if not line.strip():
            continue
        name, created, started, completed = line.split("|")
        rows.append((name, float(created), float(started), float(completed)))
    return rows


def main():
    key, ip, tag = sys.argv[1], sys.argv[2], sys.argv[3]
    ssh = ["ssh", "-n", "-o", "StrictHostKeyChecking=no", "-i", key, f"ubuntu@{ip}"]
    rows = fetch(ssh, tag)
    if not rows:
        sys.exit(f"no runs tagged {tag}")
    base = rows[0][1]
    r = lambda v: round(v - base, 3)
    print(json.dumps({
        "tag": tag,
        "units": len(rows),
        "created_at": [r(x[1]) for x in rows],
        "started_at": [r(x[2]) for x in rows],
        "completed_at": [r(x[3]) for x in rows],
    }))


if __name__ == "__main__":
    main()
