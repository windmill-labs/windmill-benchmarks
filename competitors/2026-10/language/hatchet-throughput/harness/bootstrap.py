#!/usr/bin/env python3
"""Headless hatchet-lite bootstrap: get a tenant id and a tenant API token.

Tries the seeded admin login first, falls back to registering a user and creating
a tenant. Writes ~/bench/tenant.txt and ~/bench/token.txt.
"""
import json
import os
import sys
import urllib.error
import urllib.request

BASE = "http://localhost:8888"
COOKIE = {"v": None}


def req(method, path, body=None, auth=True):
    headers = {"Content-Type": "application/json"}
    if auth and COOKIE["v"]:
        headers["Cookie"] = COOKIE["v"]
    r = urllib.request.Request(
        BASE + path, method=method, headers=headers,
        data=json.dumps(body).encode() if body is not None else None)
    try:
        with urllib.request.urlopen(r, timeout=30) as resp:
            sc = resp.headers.get_all("Set-Cookie") or []
            if sc:
                COOKIE["v"] = "; ".join(c.split(";")[0] for c in sc)
            raw = resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(errors="replace")
    return 200, json.loads(raw) if raw else None


CANDIDATES = [
    ("admin@example.com", "Admin123!!"),
    ("admin@example.com", "Admin123!!!"),
]

for email, pw in CANDIDATES:
    code, body = req("POST", "/api/v1/users/login", {"email": email, "password": pw},
                     auth=False)
    print(f"login {email}: {code}", file=sys.stderr)
    if code == 200:
        break
else:
    email, pw = "bench@example.com", "Benchpass123!!"
    code, body = req("POST", "/api/v1/users/register",
                     {"email": email, "password": pw, "name": "bench"}, auth=False)
    print(f"register: {code} {str(body)[:200]}", file=sys.stderr)
    if code != 200:
        sys.exit(f"cannot register: {body}")
    code, body = req("POST", "/api/v1/users/login", {"email": email, "password": pw},
                     auth=False)
    print(f"login after register: {code}", file=sys.stderr)
    if code != 200:
        sys.exit(f"cannot log in: {body}")

code, body = req("GET", "/api/v1/users/memberships")
if code != 200:
    sys.exit(f"memberships failed: {body}")
rows = body.get("rows") or []
if not rows:
    code, body = req("POST", "/api/v1/tenants", {"name": "bench", "slug": "bench"})
    print(f"tenant create: {code} {str(body)[:200]}", file=sys.stderr)
    if code != 200:
        sys.exit(f"cannot create tenant: {body}")
    tenant = body["metadata"]["id"]
else:
    tenant = rows[0]["tenant"]["metadata"]["id"]
print(f"tenant {tenant}", file=sys.stderr)

code, body = req("POST", f"/api/v1/tenants/{tenant}/api-tokens", {"name": "bench"})
if code != 200:
    sys.exit(f"cannot create api token: {body}")
token = body["token"]

os.makedirs(os.path.expanduser("~/bench"), exist_ok=True)
with open(os.path.expanduser("~/bench/tenant.txt"), "w") as f:
    f.write(tenant + "\n")
with open(os.path.expanduser("~/bench/token.txt"), "w") as f:
    f.write(token + "\n")
print(json.dumps({"tenant": tenant, "token_len": len(token)}))
