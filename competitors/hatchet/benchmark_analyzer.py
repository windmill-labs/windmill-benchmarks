#!/usr/bin/env python3
import sys
import json
import os
import requests
from datetime import datetime

def fetch_run_data(workflow_id):
    token = os.environ.get("HATCHET_CLIENT_TOKEN")
    if not token:
        raise RuntimeError("Environment variable HATCHET_CLIENT_TOKEN must be set.")

    url = f"http://localhost:8080/api/v1/stable/workflow-runs/{workflow_id}"
    headers = {
        "Authorization": f"Bearer {token}"
    }

    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()

def iso_to_timestamp(iso_str):
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    return dt.timestamp()

def clamp_non_negative(value):
    return max(0.0, round(value, 3))

def process_single_workflow(workflow_id, workers):
    data = fetch_run_data(workflow_id)

    # Use run.createdAt as the canonical time zero
    workflow_start = iso_to_timestamp(data["run"]["createdAt"])
    print(f"Using run.createdAt as reference: {datetime.fromtimestamp(workflow_start)}")

    # Map: task name -> task external ID
    task_name_to_id = {
        entry["taskName"]: entry["taskExternalId"]
        for entry in data["shape"]
    }

    # Map: (task_external_id, eventType) -> timestamp
    event_lookup = {}
    # Map: task_external_id -> workerId
    worker_map = {}

    for event in data["taskEvents"]:
        key = (event["taskId"], event["eventType"])
        event_lookup[key] = iso_to_timestamp(event["timestamp"])

        if event["eventType"] == "STARTED" and "workerId" in event:
            worker_map[event["taskId"]] = event["workerId"]

    created_list = []
    started_list = []
    completed_list = []
    worker_ids = []

    # Sort by numeric suffix of task name
    sorted_task_names = sorted(task_name_to_id.keys(), key=lambda name: int(name.split("_")[-1]))

    for task_name in sorted_task_names:
        task_id = task_name_to_id[task_name]

        queued_time = event_lookup.get((task_id, "QUEUED"))
        started_time = event_lookup.get((task_id, "STARTED"))
        finished_time = event_lookup.get((task_id, "FINISHED"))

        if queued_time is None or started_time is None or finished_time is None:
            print(f"⚠️ Missing events for task {task_name}")
            continue

        created_list.append(clamp_non_negative(queued_time - workflow_start))
        started_list.append(clamp_non_negative(started_time - workflow_start))
        completed_list.append(clamp_non_negative(finished_time - workflow_start))

        if workers > 1:
            worker_ids.append(worker_map.get(task_id, "unknown"))

    benchmark_data = {
        "workers": workers,
        "created_at": created_list,
        "started_at": started_list,
        "completed_at": completed_list
    }

    if workers > 1 and worker_ids:
        # Normalize worker IDs to integers
        unique_worker_ids = sorted(set(worker_ids))
        worker_id_map = {wid: idx for idx, wid in enumerate(unique_worker_ids)}
        normalized = [worker_id_map.get(wid, -1) for wid in worker_ids]
        benchmark_data["worker_execution"] = normalized

    return benchmark_data

def process_bulk_workflows(workflow_ids, workers):
    created_list = []
    started_list = []
    completed_list = []
    worker_ids = []

    for workflow_id in workflow_ids:
        data = fetch_run_data(workflow_id)
        workflow_start = iso_to_timestamp(data["run"]["createdAt"])

        # In bulk mode, each workflow has one task
        task = data["tasks"][0]
        task_id = task["taskExternalId"]

        created_time = iso_to_timestamp(task["taskInsertedAt"])
        started_time = iso_to_timestamp(task["startedAt"])
        finished_time = iso_to_timestamp(task["finishedAt"])

        created_list.append(clamp_non_negative(created_time - workflow_start))
        started_list.append(clamp_non_negative(started_time - workflow_start))
        completed_list.append(clamp_non_negative(finished_time - workflow_start))

        if workers > 1:
            worker_ids.append(task.get("workerId", "unknown"))

    benchmark_data = {
        "workers": workers,
        "created_at": created_list,
        "started_at": started_list,
        "completed_at": completed_list
    }

    if workers > 1 and worker_ids:
        unique_worker_ids = sorted(set(worker_ids))
        worker_id_map = {wid: idx for idx, wid in enumerate(unique_worker_ids)}
        normalized = [worker_id_map.get(wid, -1) for wid in worker_ids]
        benchmark_data["worker_execution"] = normalized

    return benchmark_data

def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <workflow_run_id> <workers> [--bulk <workflow_ids_file>]")
        sys.exit(1)

    if "--bulk" in sys.argv:
        bulk_index = sys.argv.index("--bulk")
        if bulk_index + 1 >= len(sys.argv):
            print("Error: --bulk flag requires a file path")
            sys.exit(1)

        workers = int(sys.argv[bulk_index - 1])
        workflow_ids_file = sys.argv[bulk_index + 1]

        with open(workflow_ids_file, 'r') as f:
            workflow_ids = [line.strip() for line in f if line.strip()]

        benchmark_data = process_bulk_workflows(workflow_ids, workers)
        output = {
            "python": [benchmark_data]
        }

        output_file = f"bulk_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    else:
        workflow_id = sys.argv[1]
        workers = int(sys.argv[2])

        benchmark_data = process_single_workflow(workflow_id, workers)
        output = {
            "python": [benchmark_data]
        }

        output_file = f"{workflow_id}.json"

    with open(output_file, "w") as f:
        json.dump(output, f, indent=4)

    print(f"✅ Timing data written to {output_file}")

if __name__ == "__main__":
    main()
