Prefect
=======

Prefect does not provide a docker compose to run it in Docker. Their [documentation](https://docs.prefect.io/latest/guides/host/) on how to self host is very minimal. We created our own, and chose postgres as the database to be consistent with the other (and it's the recommended option for production use cases).

The two flows are defined in the `flows/` folder that will be mounted as a volume in the Prefect container.

To execute the flows, you can kick them off from the EC2 instance but it requires the prefect CLI to be installed and connected to the Prefect server. For simplicity, we did it straight from the container.

In one session, login to the container and expose the flow:
```bash
# to go into the container:
docker exec -it <PREFECT_CONTAINER_ID> /bin/bash
# and then from inside the container:
cd /flows/
python bench_10.py
```

And in another session:
```bash
# to go into the container:
docker exec -it <PREFECT_CONTAINER_ID> /bin/bash
# and then from inside the container:
prefect deployment run bench_10/bench_10
```

To retrieve the results, the easiest I found was to connect to the DB and look into the `tasks_run` table:
```sql
SELECT id, created, start_time, end_time FROM task_run ORDER BY created ;
```
