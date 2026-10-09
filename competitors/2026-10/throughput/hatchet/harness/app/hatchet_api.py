"""Minimal REST helpers against the hatchet-lite API with the tenant API token."""
import json
import os
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

SERVER = os.environ["HATCHET_CLIENT_SERVER_URL"]
TOKEN = os.environ["HATCHET_CLIENT_TOKEN"]
TENANT = os.environ["HATCHET_TENANT_ID"]


def api(method, path, body=None):
    req = urllib.request.Request(
        SERVER + path,
        method=method,
        headers={"Authorization": "Bearer " + TOKEN, "Content-Type": "application/json"},
        data=json.dumps(body).encode() if body is not None else None,
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        raw = r.read()
    return json.loads(raw) if raw else None


def list_workers():
    rows = api("GET", f"/api/v1/tenants/{TENANT}/worker")["rows"] or []
    return [w for w in rows if w.get("status") == "ACTIVE"]


def set_paused(worker_ids, flag):
    """Flip every worker at once so the ramp is not serialised over 20 HTTP calls."""
    with ThreadPoolExecutor(max_workers=len(worker_ids) or 1) as ex:
        list(ex.map(lambda wid: api("PATCH", f"/api/v1/workers/{wid}", {"isPaused": flag}),
                    worker_ids))
