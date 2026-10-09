#!/usr/bin/env python3
"""Minimal Kestra 2.x REST client used by the throughput harness."""
import json, base64, urllib.request, urllib.error, uuid, time, datetime

BASE = "http://localhost:8080/api/v1/main"
AUTH = base64.b64encode(b"admin@kestra.io:Admin1234!").decode()
NS = "company.team"

def req(method, path, data=None, ctype=None, timeout=300, base=BASE):
    r = urllib.request.Request(base + path, method=method, data=data)
    r.add_header("Authorization", "Basic " + AUTH)
    if ctype:
        r.add_header("Content-Type", ctype)
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            body = resp.read()
    except urllib.error.HTTPError as e:
        body = e.read()
        raise RuntimeError(f"{method} {path} -> {e.code}: {body[:1200].decode(errors='replace')}")
    return json.loads(body) if body else None

def validate(yaml_text):
    return req("POST", "/flows/validate", yaml_text.encode(), "application/x-yaml")

def upsert_flow(flow_id, yaml_text):
    try:
        req("POST", "/flows", yaml_text.encode(), "application/x-yaml")
    except RuntimeError as e:
        s = str(e)
        if "already exist" in s or "409" in s or "422" in s:
            req("PUT", f"/flows/{NS}/{flow_id}", yaml_text.encode(), "application/x-yaml")
        else:
            raise

def trigger(flow_id, labels=None):
    boundary = "----b" + uuid.uuid4().hex
    body = (f"--{boundary}--\r\n").encode()
    q = f"/executions/{NS}/{flow_id}?wait=false"
    if labels:
        q += "".join(f"&labels={k}:{v}" for k, v in labels)
    ex = req("POST", q, body, f"multipart/form-data; boundary={boundary}")
    return ex["id"]

TERMINAL = ("SUCCESS", "FAILED", "KILLED", "WARNING", "CANCELLED")

def get_exec(eid):
    return req("GET", f"/executions/{eid}")

def wait_exec(eid, timeout=3600, poll=1.0):
    t0 = time.time()
    while True:
        ex = get_exec(eid)
        if ex["state"]["current"] in TERMINAL:
            return ex
        if time.time() - t0 > timeout:
            raise RuntimeError(f"timeout waiting for {eid} (state {ex['state']['current']})")
        time.sleep(poll)

def parse_iso(s):
    return datetime.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
