Temporal
========

Similar to Airflow, Temporal has a [dedicated repo](https://github.com/temporalio/docker-compose) for how to self host with Docker. We used their `docker-compose.yml` with no modification.

Once started, you can login to temporal going to http://<HOSTNAME>:8080.

Temporal plays the role of a pure orchestrator, i.e. it doesn't run the code of the tasks themselves. I.e. the code will be executed by the Python command you run to execute the flow. So, it's really important to run the folowing command on the EC2 instance, and not on your local laptop or anything else otherwise the execution performance will not be fair compared to the other orchestrators.

So, to run a benchmark, copy one of `bench_10.py`/`bench_40.py` in the folder `flows/` to the EC2 instance, and then simply run them with:

```bash
pip3 install temporalio
python3 bench_10.py
python3 bhench_40.py
```

Each run will connect to the temporal server running in Docker, and execute the flow. Temporal server will play the role of the orchestrator but the local python will execute the tasks.

To extract the results, Temporal has a button on the flow run page to download the events history in JSON. It contains a list of all events trigerred by the flow (Temporal thinks in terms of events, not tasks). We filtered only the `ActivityTaskScheduled`, `ActivityTaskStarted` and `ActivityTaskCompleted` using jq.

```json
{
  "events": [
    ...
    {
      "eventId": "5",
      "eventTime": "2023-11-09T14:55:51.381932302Z",
      "eventType": "ActivityTaskScheduled",
      "version": "0",
      "taskId": "3145740",
      "workerMayIgnore": false,
      "activityTaskScheduledEventAttributes": {
        "activityId": "task_0",
        "activityType": {
          "name": "fibo_activity"
        },
        "taskQueue": {
          "name": "bench-10",
          "kind": "Normal",
          "normalName": ""
        },
        "header": {
          "fields": {}
        },
        "input": {
          "payloads": [
            {
              "metadata": {
                "encoding": "anNvbi9wbGFpbg=="
              },
              "data": "MzM="
            }
          ]
        },
        "scheduleToCloseTimeout": "0s",
        "scheduleToStartTimeout": "0s",
        "startToCloseTimeout": "360s",
        "heartbeatTimeout": "0s",
        "workflowTaskCompletedEventId": "4",
        "retryPolicy": {
          "initialInterval": "1s",
          "backoffCoefficient": 2,
          "maximumInterval": "100s",
          "maximumAttempts": 0,
          "nonRetryableErrorTypes": []
        },
        "useCompatibleVersion": true
      }
    },
    {
      "eventId": "6",
      "eventTime": "2023-11-09T14:55:51.389407802Z",
      "eventType": "ActivityTaskStarted",
      "version": "0",
      "taskId": "3145746",
      "workerMayIgnore": false,
      "activityTaskStartedEventAttributes": {
        "scheduledEventId": "5",
        "identity": "39827@gorilla.local",
        "requestId": "3ea4c3d7-f495-4059-aecd-50d9f0e3a8fe",
        "attempt": 1,
        "lastFailure": null
      }
    },
    {
      "eventId": "7",
      "eventTime": "2023-11-09T14:55:51.768861927Z",
      "eventType": "ActivityTaskCompleted",
      "version": "0",
      "taskId": "3145747",
      "workerMayIgnore": false,
      "activityTaskCompletedEventAttributes": {
        "result": {
          "payloads": [
            {
              "metadata": {
                "encoding": "anNvbi9wbGFpbg=="
              },
              "data": "MzUyNDU3OA=="
            }
          ]
        },
        "scheduledEventId": "5",
        "startedEventId": "6",
        "identity": "39827@gorilla.local",
        "workerVersion": null
      }
    }
    ...
  ]
}
```

The following `jq` will filter only the `schedules`/`started`/`completed` events and extract the timestamp.
```bash
cat bench-10.json | jq '.events[] | select(.eventType == "ActivityTaskScheduled"), select(.eventType == "ActivityTaskStarted"), select(.eventType == "ActivityTaskCompleted") | .eventTime' > bench-10.csv
```
And then using regexp you can format the csv appropriately. Especially replace `^([^;]+?)\n([^;]+?)\n([^;]+?)$` with `$1;$2;$3` for have one line per task. Note for next time, inspecting the postgres database might be easier to extract the results.
