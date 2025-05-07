import sys
import requests
import pandas as pd

def calculate_times(tasks):
    task_data = []
    prev_success_time = None

    for task in tasks:
        if not task.get("taskId") in ["python", "script"]:
            continue

        histories = task["state"]["histories"]
        created_time = pd.to_datetime(histories[0]["date"])
        running_time = pd.to_datetime(histories[1]["date"])
        success_time = pd.to_datetime(histories[2]["date"])

        assignment_time = (running_time - created_time).total_seconds()
        execution_time = (success_time - running_time).total_seconds()
        transition_time = (
            (created_time - prev_success_time).total_seconds() if prev_success_time else 0
        )

        task_data.append({
            "Task ID": task["id"],
            "Value": task.get("value", "N/A"),
            "Created Time": created_time,
            "Started Time": running_time,
            "Success Time": success_time,
            "Transition Time (s)": transition_time,
            "Assignment Time (s)": assignment_time,
            "Execution Time (s)": execution_time,
            "Total Time (s)": transition_time + assignment_time + execution_time,
        })

        prev_success_time = success_time

    return task_data

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python analysis.py <url>")
        sys.exit(1)

    url = "http://localhost:8080/api/v1/executions/" + sys.argv[1]

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        print(f"Error fetching data from URL: {e}")
        sys.exit(1)

    tasks = data.get("taskRunList", [])

    # Process the tasks
    task_times = calculate_times(tasks)

    # Convert to DataFrame
    df = pd.DataFrame(task_times)

    # Compute total running time
    if not df.empty:
        total_running_time = (
            df["Success Time"].max() - df["Created Time"].min()
        ).total_seconds()
    else:
        total_running_time = 0

    # Display the result
    print(df)

    # Optional: Save to CSV or display in a more sophisticated way
    df.to_csv("output.csv", index=False)

    # Calculate percentage breakdown
    avg_assignment = df["Assignment Time (s)"].mean()
    avg_execution = df["Execution Time (s)"].mean()
    avg_transition = df["Transition Time (s)"].mean()

    total_time = avg_assignment + avg_execution + avg_transition

    percentage_breakdown = {
        "Execution (%)": (avg_execution / total_time) * 100,
        "Assignment (%)": (avg_assignment / total_time) * 100,
        "Transition (%)": (avg_transition / total_time) * 100,
    }

    # Print percentages
    print("\nPercentage Breakdown:")
    for key, value in percentage_breakdown.items():
        print(f"{key}: {value:.2f}%")

    # Print total running time
    print(f"\nTotal Running Time: {total_running_time:.2f} seconds")
