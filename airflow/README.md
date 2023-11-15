Airflow
=======

Airflow has a pretty good documentation on how to self host. They provide a docker compose [here](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html). We used it with no modification. As indicated in the docs, make sure to persist the user ID to the env variable before starting the docker compose (simply run `echo -e "AIRFLOW_UID=$(id -u)" > .env`). We tried another executor but then decided to use Celery as it seems to be the recommended one (and the default from the docker compose).

Once started, you can login to airflow going to http://<HOSTNAME>:8080. Default credentials are airflow/airflow

The folder `dags/` contains the 2 DAGs for the 2 benchmarks and will be mounted in the Airflow container, such that they should appear in the landing page of Airflow when it starts (you can filter by tag `benchmark`).

You just need to click the "Run" button, and this is it. 

To extract the results, we inspected the network requests quen loading the flow results. It contains a JSON with all the tasks information. Something like:

```json
{
    "dag_runs": [
        {
            "conf": null,
            "conf_is_json": false,
            "data_interval_end": "2023-11-09T14:45:13.629798+00:00",
            "data_interval_start": "2023-11-09T14:45:13.629798+00:00",
            "end_date": "2023-11-09T14:45:33.240037+00:00",
            "execution_date": "2023-11-09T14:45:13.629798+00:00",
            "external_trigger": true,
            "last_scheduling_decision": "2023-11-09T14:45:33.235897+00:00",
            "note": null,
            "queued_at": "2023-11-09T14:45:13.649122+00:00",
            "run_id": "manual__2023-11-09T14:45:13.629798+00:00",
            "run_type": "manual",
            "start_date": "2023-11-09T14:45:14.173491+00:00",
            "state": "success"
        }
    ],
    "groups": {
        "children": [
            {
                "extra_links": [],
                "has_outlet_datasets": false,
                "id": "task_0",
                "instances": [
                    {
                        "end_date": "2023-11-09T14:45:16.210983+00:00",
                        "note": null,
                        "queued_dttm": "2023-11-09T14:45:14.204623+00:00",
                        "run_id": "manual__2023-11-09T14:45:13.629798+00:00",
                        "start_date": "2023-11-09T14:45:15.527698+00:00",
                        "state": "success",
                        "task_id": "task_0",
                        "try_number": 1
                    }
                ],
                "is_mapped": false,
                "label": "task_0",
                "operator": "@task",
                "trigger_rule": "all_success"
            },
            ...
            {
                "extra_links": [],
                "has_outlet_datasets": false,
                "id": "task_9",
                "instances": [
                    {
                        "end_date": "2023-11-09T14:45:33.071092+00:00",
                        "note": null,
                        "queued_dttm": "2023-11-09T14:45:31.864154+00:00",
                        "run_id": "manual__2023-11-09T14:45:13.629798+00:00",
                        "start_date": "2023-11-09T14:45:32.347408+00:00",
                        "state": "success",
                        "task_id": "task_9",
                        "try_number": 1
                    }
                ],
                "is_mapped": false,
                "label": "task_9",
                "operator": "@task",
                "trigger_rule": "all_success"
            }
        ],
        "id": null,
        "instances": [],
        "label": null
    },
    "ordering": [
        "execution_date"
    ]
}
```

You can extract only the timestamps with the following `jq`:
```
cat bench-10.json | jq '.groups.children.[] | .id, .instances[0].queued_dttm, .instances[0].start_date, .instances[0].end_date' > bench-10.csv
```
And then using regexp you can format the csv appropriately. Especially replace `^([^;]+?)\n([^;]+?)\n([^;]+?)\n([^;]+?)$` with `$1;$2;$3;$4` for have one line per task. Note for next time, inspecting the postgres database might be easier to extract the results.
