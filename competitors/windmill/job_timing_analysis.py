#!/usr/bin/env python3
import argparse
import psycopg2
import json
import os
from datetime import datetime
from psycopg2.extras import RealDictCursor


def query_job_data(parent_job_id, conn_string, workers=1):
    print(f"Using connection string: {conn_string}")

    with psycopg2.connect(conn_string) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get parent job data
            parent_query = """
            SELECT created_at, started_at
            FROM v2_as_completed_job
            WHERE id = %s
            """
            cur.execute(parent_query, (parent_job_id,))
            parent_job = cur.fetchone()

            if not parent_job:
                print(f"Parent job {parent_job_id} not found")
                return None, None

            # Different query logic based on worker count
            if workers > 1:
                # Single query to get all sub-parents and their child jobs
                combined_query = """
                WITH sub_parents AS (
                    SELECT
                        sp.id AS sub_parent_id,
                        sp.created_at AS sub_parent_created_at,
                        sp.started_at AS sub_parent_started_at
                    FROM v2_as_completed_job sp
                    WHERE sp.parent_job = %s
                    ORDER BY sp.created_at
                )
                SELECT
                    sp.sub_parent_id,
                    sp.sub_parent_created_at,
                    sp.sub_parent_started_at,
                    c.id,
                    c.parent_job,
                    c.created_at,
                    c.started_at,
                    c.duration_ms,
                    jc.worker
                FROM sub_parents sp
                LEFT JOIN v2_as_completed_job c ON c.parent_job = sp.sub_parent_id
                LEFT JOIN v2_job_completed jc ON c.id = jc.id
                ORDER BY sp.sub_parent_created_at
                """
                cur.execute(combined_query, (parent_job_id,))
                results = cur.fetchall()

                print(f"Found {len(results)} jobs in hierarchy")

                # Process and structure the results
                all_jobs = []
                for row in results:
                    if row['id']: # Only include rows where we found a child job
                        job_data = {
                            'id': row['id'],
                            'parent_job': row['parent_job'],
                            'created_at': row['created_at'],
                            'started_at': row['started_at'],
                            'duration_ms': row['duration_ms'],
                            'sub_parent_id': row['sub_parent_id'],
                            'sub_parent_created_at': row['sub_parent_created_at'],
                            'sub_parent_started_at': row['sub_parent_started_at'],
                            'worker': row['worker']
                        }
                        all_jobs.append(job_data)

                return all_jobs, parent_job
            else:
                # Original logic for single worker
                query = """
                SELECT parent_job, id, created_at, started_at, duration_ms
                FROM v2_as_completed_job
                WHERE parent_job = %s
                ORDER BY created_at
                """
                cur.execute(query, (parent_job_id,))
                return cur.fetchall(), parent_job


def calculate_relative_timings(jobs, parent_job, workers=1):
    if not jobs or not parent_job:
        return [], []

    # Use parent job's start time as reference
    reference_time = parent_job['started_at']
    print(f"Using parent job start time as reference: {reference_time}")

    # DEBUG: For multi-worker, print raw sub_parent_created_at times and their relative values
    if workers > 1 and jobs:
        print("\nSub-parent creation times:")
        for i, job in enumerate(jobs[:5]):  # Print first 5 for brevity
            raw_time = job['sub_parent_created_at']
            rel_time = (raw_time - reference_time).total_seconds()
            print(f"  Job {i}: Raw={raw_time}, Relative={rel_time:.6f}s")

        # Debug worker assignments
        if 'worker' in jobs[0]:
            print("\nWorker assignments (first 5 jobs):")
            for i, job in enumerate(jobs[:5]):
                print(f"  Job {i}: Worker={job.get('worker', 'None')}")

    worker_ids = []

    result = []
    for i, job in enumerate(jobs):
        # For multi-worker, use sub-parent creation time instead of task creation time
        if workers > 1 and 'sub_parent_created_at' in job:
            created_at_rel = (job['sub_parent_created_at'] - reference_time).total_seconds()
            started_at_rel = (job['started_at'] - reference_time).total_seconds()
        else:
            created_at_rel = (job['created_at'] - reference_time).total_seconds()
            started_at_rel = (job['started_at'] - reference_time).total_seconds()

        completed_at_rel = started_at_rel + (job['duration_ms'] / 1000)

        task_result = {
            'task': f'task_{i:02d}',
            'created_at': created_at_rel,
            'started_at': started_at_rel,
            'completed_at': completed_at_rel,
            'created_started_diff': (job['started_at'] - job['created_at']).total_seconds(),
        }

        if 'worker' in job:
            task_result['worker'] = job['worker']
            worker_ids.append(job['worker'])

        result.append(task_result)

    # Debug worker IDs
    if worker_ids:
        unique_workers = set(worker_ids)
        print(f"\nFound {len(unique_workers)} unique worker IDs: {sorted(unique_workers)}")

    return result, worker_ids


def create_json_file(relative_timings, parent_job_id, worker_ids=None):
    filename = f"timing-{parent_job_id}.json"

    output = {
        "python": [{
            "workers": len(set(worker_ids)) if worker_ids else 1,
            "created_at": [round(t['created_at'], 3) for t in relative_timings],
            "started_at": [round(t['started_at'], 3) for t in relative_timings],
            "completed_at": [round(t['completed_at'], 3) for t in relative_timings],
        }]
    }

    if worker_ids:
        unique_worker_ids = sorted(set(worker_ids))
        print(f"Creating worker mapping for {len(unique_worker_ids)} workers: {unique_worker_ids}")
        worker_id_map = {wid: idx for idx, wid in enumerate(unique_worker_ids)}
        normalized = [worker_id_map.get(wid, -1) for wid in worker_ids]
        output["python"][0]["worker_execution"] = normalized

    with open(filename, 'w') as f:
        json.dump(output, f, indent=4)

    return filename


def create_stats_file(relative_timings, parent_job_id):
    filename = f"stats-{parent_job_id}.md"
    total_tasks = len(relative_timings)

    if not total_tasks:
        print("No tasks to analyze")
        return filename

    first_task_created = relative_timings[0]['created_at']
    last_task_started = relative_timings[-1]['started_at']
    last_task_completed = relative_timings[-1]['completed_at']
    total_duration = last_task_completed

    execution_times = [t['completed_at'] - t['started_at'] for t in relative_timings]
    avg_execution_time = sum(execution_times) / total_tasks
    total_execution_time = sum(execution_times)
    execution_percentage = (total_execution_time / total_duration) * 100

    assignment_times = [t['created_started_diff'] for t in relative_timings]
    avg_assignment_time = sum(assignment_times) / total_tasks
    total_assignment_time = sum(assignment_times)
    assignment_percentage = (total_assignment_time / total_duration) * 100

    transition_times = []
    for i in range(total_tasks - 1):
        current_end = relative_timings[i]['completed_at']
        next_created = relative_timings[i + 1]['created_at']
        transition_times.append(next_created - current_end)

    avg_transition_time = sum(transition_times) / (total_tasks - 1) if transition_times else 0
    total_transition_time = sum(transition_times)
    transition_percentage = (total_transition_time / total_duration) * 100 if transition_times else 0

    with open(filename, 'w') as f:
        f.write("# Performance Statistics\n\n")
        f.write(f"## Total Duration: {total_duration:.3f}s\n\n")
        f.write("## Average Times\n\n")
        f.write(f"- **Execution time**: {avg_execution_time:.3f}s ({execution_percentage:.2f}%)\n")
        f.write(f"- **Assignment time**: {avg_assignment_time:.3f}s ({assignment_percentage:.2f}%)\n")
        f.write(f"- **Transition time**: {avg_transition_time:.3f}s ({transition_percentage:.2f}%)\n")
        f.write(f"\nTotal tasks analyzed: {total_tasks}\n")

    return filename


def main():
    parser = argparse.ArgumentParser(description='Analyze Windmill job timings')
    parser.add_argument('parent_job_id', help='Parent job ID')
    parser.add_argument('--conn-string', required=True, help='PostgreSQL connection string')
    parser.add_argument('--workers', type=int, default=1, help='Number of workers')

    args = parser.parse_args()

    try:
        job_data, parent_job = query_job_data(args.parent_job_id, args.conn_string, args.workers)
        if not job_data:
            print("No matching job data found.")
            return

        relative_timings, worker_ids = calculate_relative_timings(job_data, parent_job, args.workers)

        json_file = create_json_file(relative_timings, args.parent_job_id, worker_ids if args.workers > 1 else None)
        stats_file = create_stats_file(relative_timings, args.parent_job_id)

        print(f"Analysis complete.\n- JSON:  {json_file}\n- Stats: {stats_file}")
    except Exception as e:
        print(f"Error: {e}")
        raise


if __name__ == "__main__":
    main()
