#!/bin/bash

# Check if number of workers argument is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <number_of_workers>"
    echo "Example: $0 5"
    exit 1
fi

# Get number of workers from first argument
num_workers=$1

# Validate that the argument is a positive integer
if ! [[ "$num_workers" =~ ^[1-9][0-9]*$ ]]; then
    echo "Error: Number of workers must be a positive integer"
    exit 1
fi

# Array to store process IDs
pids=()

# Function to clean up background processes
cleanup() {
    echo "Terminating all worker processes..."
    for pid in "${pids[@]}"; do
        kill "$pid" 2>/dev/null
    done
    exit 0
}

# Set up trap to catch SIGINT (Ctrl+C)
trap cleanup SIGINT

# Start the specified number of worker processes
for ((i=1; i<=num_workers; i++)); do
    go run cmd/worker/main.go > /dev/null &
    pids+=($!)  # Store the process ID
done

echo "Started $num_workers worker processes. Press Ctrl+C to terminate all."

# Wait for all background processes to complete
# or for user to press Ctrl+C
wait