#!/usr/bin/env python3
import argparse
import json
import os
from datetime import datetime
import requests
import asyncio

async def get_workflow_via_api(workflow_id, run_id=None, temporal_address="localhost:8080"):
    """
    Get workflow execution history from Temporal API
    """
    base_url = f"http://{temporal_address}/api/v1/namespaces/default/workflows/{workflow_id}"
    all_events = []
    next_page_token = None

    while True:
        # Construct URL with run_id and next_page_token if they exist
        url = f"{base_url}/history"
        if run_id:
            url = f"{url}?execution.runId={run_id}"
            if next_page_token:
                url = f"{url}&nextPageToken={next_page_token}"
        elif next_page_token:
            url = f"{url}?nextPageToken={next_page_token}"

        print(f"Fetching workflow history from: {url}")

        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            events = data.get('history', {}).get('events', [])
            all_events.extend(events)
            print(f"Successfully fetched {len(events)} events")

            # Get next page token and break if none
            next_page_token = data.get('nextPageToken')
            if not next_page_token:
                break

        except Exception as e:
            print(f"Error fetching workflow history: {e}")
            break

    print(f"Total events fetched: {len(all_events)}")
    return all_events

def extract_activity_timings(events):
    """
    Extract activity timings by processing events sequentially
    """
    print("Extracting activity timings from events")

    # Find workflow start time
    workflow_start_time = None
    for event in events:
        if event.get('eventType') == 'EVENT_TYPE_WORKFLOW_EXECUTION_STARTED':
            event_time = event.get('eventTime', '')
            if event_time:
                try:
                    dt = datetime.fromisoformat(event_time.replace('Z', '+00:00'))
                    workflow_start_time = dt.timestamp()
                    print(f"Found workflow start time: {workflow_start_time}")
                    break
                except Exception as e:
                    print(f"Error parsing start time: {e}")

    if not workflow_start_time:
        print("Could not find workflow start time")
        return []

    # Process events sequentially
    activities_dict = {}
    scheduled_events = {}
    started_events = {}

    # Track completion and scheduling times for transition time analysis
    completion_times = []
    schedule_times = []

    # Track worker assignments
    worker_activities = {}

    for event in events:
        event_type = event.get('eventType')
        event_time_str = event.get('eventTime', '')
        event_id = event.get('eventId')

        if not event_time_str:
            continue

        # Convert timestamp to seconds since epoch
        try:
            dt = datetime.fromisoformat(event_time_str.replace('Z', '+00:00'))
            event_time = dt.timestamp()
        except:
            continue

        # Track activity through its lifecycle using event IDs
        if event_type == 'EVENT_TYPE_ACTIVITY_TASK_SCHEDULED':
            attr_name = 'activityTaskScheduledEventAttributes'
            attrs = event.get(attr_name, {})
            activity_id = attrs.get('activityId', '')

            # If no activity ID, try to extract from sequence
            if not activity_id:
                activity_id = f"task_{len(activities_dict):02d}"
            elif activity_id.isdigit():
                activity_id = f"task_{int(activity_id):02d}"

            activities_dict[activity_id] = {
                'task': activity_id,
                'created_at': float(event_time - workflow_start_time)
            }
            scheduled_events[event_id] = activity_id
            schedule_times.append((event_time, activity_id))

        elif event_type == 'EVENT_TYPE_ACTIVITY_TASK_STARTED':
            attr_name = 'activityTaskStartedEventAttributes'
            attrs = event.get(attr_name, {})
            scheduled_event_id = attrs.get('scheduledEventId')
            worker_id = attrs.get('identity', '')  # Get worker identity

            if scheduled_event_id in scheduled_events:
                activity_id = scheduled_events[scheduled_event_id]
                if activity_id in activities_dict:
                    activities_dict[activity_id]['started_at'] = float(event_time - workflow_start_time)
                    activities_dict[activity_id]['worker_id'] = worker_id
                    started_events[event_id] = activity_id

                    # Track which worker handled this activity
                    if worker_id not in worker_activities:
                        worker_activities[worker_id] = []
                    worker_activities[worker_id].append(activity_id)

        elif event_type == 'EVENT_TYPE_ACTIVITY_TASK_COMPLETED':
            attr_name = 'activityTaskCompletedEventAttributes'
            attrs = event.get(attr_name, {})
            started_event_id = attrs.get('startedEventId')

            if started_event_id in started_events:
                activity_id = started_events[started_event_id]
                if activity_id in activities_dict:
                    activity = activities_dict[activity_id]
                    if 'started_at' in activity:
                        activity['completed_at'] = float(event_time - workflow_start_time)
                        activity['duration'] = float(activity['completed_at'] - activity['started_at'])
                        completion_times.append((event_time, activity_id))

    # Sort activities by task ID
    sorted_activities = sorted(
        [act for act in activities_dict.values() if 'completed_at' in act],
        key=lambda x: int(x['task'].split('_')[1])
    )

    # Calculate waiting times after sorting
    for i, activity in enumerate(sorted_activities):
        if i == 0:
            # For first activity, waiting is time from creation to start
            activity['waiting_time'] = float(activity['started_at'] - activity['created_at'])
        else:
            # Time between previous activity completion and this one starting
            prev_completed = sorted_activities[i-1]['completed_at']
            activity['waiting_time'] = float(activity['started_at'] - prev_completed)

    print(f"Successfully processed {len(sorted_activities)} activities")
    return sorted_activities

def create_benchmark_json(activities, args):
    """
    Create or update a benchmark JSON file with the specified format
    """
    # Prepare the new benchmark data
    benchmark_data = {
        "workers": args.workers,
        "created_at": [round(activity['created_at'], 3) for activity in activities],
        "started_at": [round(activity['started_at'], 3) for activity in activities],
        "completed_at": [round(activity['completed_at'], 3) for activity in activities]
    }

    # Add worker execution mapping if we have multiple workers
    if args.workers > 1:
        # First collect all unique worker IDs and create a mapping to normalized IDs (0-based)
        unique_workers = set()
        for activity in activities:
            worker_id = activity.get('worker_id', '')
            clean_id = worker_id.split('@')[0] if '@' in worker_id else worker_id
            unique_workers.add(clean_id)

        # Sort worker IDs numerically and create mapping
        sorted_workers = sorted(unique_workers, key=lambda x: int(x))
        worker_id_map = {worker_id: idx for idx, worker_id in enumerate(sorted_workers)}

        # Create the normalized worker execution mapping
        worker_mapping = []
        for activity in activities:
            worker_id = activity.get('worker_id', '')
            clean_id = worker_id.split('@')[0] if '@' in worker_id else worker_id
            normalized_id = worker_id_map[clean_id]
            worker_mapping.append(normalized_id)

        benchmark_data["worker_execution"] = worker_mapping

    # Create the usecase data
    usecase_data = {
        "title": args.title,
        "description": args.description,
        args.language: benchmark_data
    }

    # Try to load existing file if it exists
    data = {"version": "2.34.0", "usecases": {}}
    if os.path.exists(args.output_file):
        try:
            with open(args.output_file, 'r') as f:
                data = json.load(f)
        except json.JSONDecodeError:
            print(f"Warning: Could not parse existing file {args.output_file}. Creating new file.")

    # Update or add the usecase
    data["usecases"][args.usecase] = usecase_data

    # Write the updated data back to the file
    with open(args.output_file, 'w') as f:
        json.dump(data, f, indent=4)

    return args.output_file

async def main_async(args):
    """Main async function"""
    print(f"Using REST API to fetch workflow data for {args.workflow_id}")
    events = await get_workflow_via_api(args.workflow_id, args.run_id, args.address)
    activities = extract_activity_timings(events)

    # Output statistics
    if not activities:
        print("No activities found")
        return

    # Generate output file
    output_file = create_benchmark_json(activities, args)
    print(f"Analysis complete. Output file: {output_file}")

def main():
    parser = argparse.ArgumentParser(description="Temporal Workflow Timing Analysis")
    parser.add_argument("--workflow-id", required=True, help="Workflow ID")
    parser.add_argument("--run-id", help="Run ID (optional)")
    parser.add_argument("--address", default="localhost:8080", help="Temporal server address")
    parser.add_argument("--output-file", required=True, help="Output JSON file path")
    parser.add_argument("--usecase", required=True, help="Name of the usecase (e.g., fibonacci_10_33)")
    parser.add_argument("--language", required=True, help="Programming language (e.g., python, js, go)")
    parser.add_argument("--workers", type=int, required=True, help="Number of workers")
    parser.add_argument("--title", required=True, help="Title of the benchmark")
    parser.add_argument("--description", required=True, help="Description of the benchmark")

    args = parser.parse_args()
    asyncio.run(main_async(args))

if __name__ == "__main__":
    main()