Windmill
========

We run Windmill in Docker using the usual docker-compose.yml, which we tweak a little to be fair with the other orchestrators:
- we run only one worker by setting their number of replicas to `1` (either in normal mode or in dedicated mode, depending on the benchmark we run)
- we removed the native workers by setting their number of replicas to `0`, as they are useless

We used Windmill Enterprise Edition to faciliate the benchmarks run in dedicated mode, although this shouldn't have any impact on performance.

To be consistent with the other orchestrator, we chose to use a python script:
```python
# WIMDMILL script: `u/benchmarkuser/fibo_script`
def fibo(n: int):
    if n <= 1:
        return n
    else:
        return fibo(n - 1) + fibo(n - 2)

def main(
    n: int,
):
    return fibo(n)
```

And then 2 flows for the 2 usecases:

```json
{
    "summary": "Fibonacci benchmark flow",
    "description": "Flow running 10 times Fibonacci of 33",
    "value": {
        "modules": [
            {
                "id": "a",
                "value": {
                    "type": "forloopflow",
                    "modules": [
                        {
                            "id": "b",
                            "value": {
                                "path": "u/admin/fibo_script",
                                "type": "script",
                                "input_transforms": {
                                    "n": {
                                        "type": "static",
                                        "value": 33
                                    }
                                }
                            }
                        }
                    ],
                    "iterator": {
                        "expr": "Array(10)",
                        "type": "javascript"
                    },
                    "parallel": false,
                    "skip_failures": true
                }
            }
        ]
    },
    "schema": {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "properties": {},
        "required": [],
        "type": "object"
    }
}
```

and for the 40 lightweight tasks:

```json
{
    "summary": "Fibonacci benchmark flow",
    "description": "Flow running 40 times Fibonacci of 10",
    "value": {
        "modules": [
            {
                "id": "a",
                "value": {
                    "type": "forloopflow",
                    "modules": [
                        {
                            "id": "b",
                            "value": {
                                "path": "u/admin/fibo_script",
                                "type": "script",
                                "input_transforms": {
                                    "n": {
                                        "type": "static",
                                        "value": 10
                                    }
                                }
                            }
                        }
                    ],
                    "iterator": {
                        "expr": "Array(40)",
                        "type": "javascript"
                    },
                    "parallel": false,
                    "skip_failures": true
                }
            }
        ]
    },
    "schema": {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "properties": {},
        "required": [],
        "type": "object"
    }
}
```

To retrieve the results, we queried the database directly. We extracted the stats of the scripts executions to be consistent with what we do for the other orchestrators

```sql
SELECT id, created_at, started_at, duration_ms FROM completed_job WHERE script_path = 'u/benchmarkuser/fibo_script' ORDER BY created_at;
```

We executed the benchmarks both in normal mode and dedicated worker mode. For the latter, just add the worker in docker compose to its own worker group (we called is `python`) by updating the value of the `WORKER_GROUP` env var and then if you're using the EE, just set the Fibonacci script path in the dedicated worker config. Wince there's only one worker, as long as the flow finished, you're sure it's executed on the dedicated worker.
