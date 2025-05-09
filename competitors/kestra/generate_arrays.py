import pandas as pd
import sys
import os

def process_csv(file_path):
    try:
        df = pd.read_csv(file_path)

        df["Created Time"] = pd.to_datetime(df["Created Time"])
        df["Started Time"] = pd.to_datetime(df["Started Time"])
        df["Success Time"] = pd.to_datetime(df["Success Time"])

        waiting_times = (df["Started Time"] - df["Created Time"]).dt.total_seconds().tolist()
        execution_times = (df["Success Time"] - df["Started Time"]).dt.total_seconds().tolist()

        waiting_times = [round(t, 3) for t in waiting_times]
        execution_times = [round(t, 3) for t in execution_times]

        formatted_result = [waiting_times, execution_times]

        print("Formatted Result:")
        print("[")
        print("\t[", ", ".join([f"{t:.3f}" for t in waiting_times]), "],")
        print("\t[", ", ".join([f"{t:.3f}" for t in execution_times]), "]")
        print("]")

        create_markdown_table(df, file_path)
        create_csv_output(waiting_times, execution_times, file_path)

    except Exception as e:
        print(f"An error occurred: {e}")

def create_csv_output(waiting_times, execution_times, input_file):
    """
    Create a CSV file with the formatted arrays
    """
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    filename = f"{base_name}.output.csv"

    output_df = pd.DataFrame({
        'waiting_time': waiting_times,
        'execution_time': execution_times
    })

    output_df.to_csv(filename, float_format='%.3f', index=False)

    print(f"Created CSV output: {filename}")

def create_markdown_table(df, input_file):
    """
    Create a markdown file with a table of relative timings
    """
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    filename = f"result-{base_name}.md"

    reference_time = df["Created Time"].iloc[0]

    df["relative_created"] = (df["Created Time"] - reference_time).dt.total_seconds()
    df["relative_started"] = (df["Started Time"] - reference_time).dt.total_seconds()
    df["relative_completed"] = (df["Success Time"] - reference_time).dt.total_seconds()

    with open(filename, 'w') as f:
        f.write("|  **Task**   | **Created at** | **Started at** | **Completed at** |\n")
        f.write("| :---------: | -------------: | -------------: | ---------------: |\n")

        for i, row in df.iterrows():
            task_name = f"task_{i:02d}"
            f.write(f"| **{task_name}** | {row['relative_created']:14.3f} | {row['relative_started']:14.3f} | {row['relative_completed']:16.3f} |\n")

    print(f"Created markdown table: {filename}")

    create_stats_file(df, base_name)

def create_stats_file(df, base_name):
    """
    Create a stats.md file with performance statistics
    """
    filename = f"stats-{base_name}.md"

    total_tasks = len(df)
    first_task_created = df["relative_created"].iloc[0]
    last_task_completed = df["relative_completed"].iloc[-1]
    total_duration = last_task_completed

    execution_times = (df["Success Time"] - df["Started Time"]).dt.total_seconds()
    avg_execution_time = execution_times.mean()
    total_execution_time = execution_times.sum()
    execution_percentage = (total_execution_time / total_duration) * 100

    assignment_times = (df["Started Time"] - df["Created Time"]).dt.total_seconds()
    avg_assignment_time = assignment_times.mean()
    total_assignment_time = assignment_times.sum()
    assignment_percentage = (total_assignment_time / total_duration) * 100

    transition_times = []
    for i in range(total_tasks - 1):
        current_task_end = df["relative_completed"].iloc[i]
        next_task_created = df["relative_created"].iloc[i+1]
        transition_time = next_task_created - current_task_end
        transition_times.append(transition_time)

    avg_transition_time = sum(transition_times) / (total_tasks - 1) if transition_times else 0
    total_transition_time = sum(transition_times)
    transition_percentage = (total_transition_time / total_duration) * 100 if transition_times else 0

    with open(filename, 'w') as f:
        f.write("# Performance Statistics\n\n")

        f.write(f"## Total Duration: {total_duration:.3f}s\n\n")

        f.write("## Average Times\n\n")
        f.write(f"- **Execution time**: {avg_execution_time:.3f}s ({execution_percentage:.2f}% of total)\n")
        f.write("  - The time it takes for the orchestrator to execute the task once it has been assigned to an executor\n\n")

        f.write(f"- **Assignment time**: {avg_assignment_time:.3f}s ({assignment_percentage:.2f}% of total)\n")
        f.write("  - The time it takes for a task to be assigned to an executor once it has been created in the queue\n\n")

        f.write(f"- **Transition time**: {avg_transition_time:.3f}s ({transition_percentage:.2f}% of total)\n")
        f.write("  - The time it takes to create the following task once a task is finished\n\n")

        f.write(f"Total tasks analyzed: {total_tasks}\n")

    print(f"Created stats file: {filename}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python generate_arrays.py <path_to_csv_file>")
    else:
        csv_file_path = sys.argv[1]
        process_csv(csv_file_path)
