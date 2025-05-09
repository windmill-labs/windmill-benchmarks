#!/bin/bash

# Default values
iter=10
fibo_n=33
workers=3
parallel=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -i|--iter)
            iter="$2"
            shift 2
            ;;
        -f|--fibo-n)
            fibo_n="$2"
            shift 2
            ;;
        -w|--workers)
            workers="$2"
            shift 2
            ;;
        -p|--parallel)
            parallel=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [-i|--iter <iterations>] [-f|--fibo-n <number>] [-w|--workers <count>] [-p|--parallel]"
            exit 1
            ;;
    esac
done

echo "Starting benchmark with:"
echo "  iterations: $iter"
echo "  fibo-n: $fibo_n"
echo "  workers: $workers"
echo "  parallel: $parallel"

cleanup() {
    echo "Cleaning up processes..."
    pkill -f "go-build.*exe/main -iter $iter -fibo-n $fibo_n"
    exit
}

trap cleanup SIGINT SIGTERM EXIT

# Initialize Go modules if needed
if [ ! -f "go.mod" ]; then
    go mod init temporal-bench
    go mod tidy
fi

echo "Starting workflow..."
go run cmd/starter/main.go -iter "$iter" -fibo-n "$fibo_n" -workers "$workers" -parallel="$parallel"

wait
