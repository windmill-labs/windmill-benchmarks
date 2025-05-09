Hatchet
=======

## Installation

- Follow the guide on (Hatchet's official documentation)[https://docs.hatchet.run/self-hosting/docker-compose]
- `docker compose up -d`
- create an API token on the UI (localhost:8080) and export it in your terminal along with
    ```
    export HATCHET_CLIENT_TOKEN="..."
    HATCHET_CLIENT_TLS_STRATEGY=none
    ```

## Running flows

### Python

- Run the workers: e.g

    ```
    ITERATIONS=10 python3 worker.py
    ```

- Trigger the workflow

    ```
    python3 trigger.py --n 33
    ```

### Go

We followed the sample in https://github.com/hatchet-dev/hatchet-go-quickstart

- Run the workers: e.g

    ```
    # Run workers (e.g 10)
    # ITERATIONS / PARALLEL can be passed depending on what analysis you're trying to do
    for i in {1..10}; do go run ITERATIONS=400 cmd/worker/main.go > /dev/null & done
    ```

- Trigger the workflow

    ```
    # or use --bulk --iterations=xxx to trigger bulk run mode
    go run cmd/run/main.go --n=38
    ```

## Timing analysis

```
# workflow id, number of workers
python3 benchmark_analyzer.py d264c5cd-c769-4cef-ba46-664c06e265cd 10
```

```
# bulk run
# create file e.g run_100.txt with the 100 different workflow ids you get from the cmd/run/main.go command
# e.g 10 workers, run_100.txt with the ids
python3 benchmark_analyzer.py 10 --bulk run_100.txt
```