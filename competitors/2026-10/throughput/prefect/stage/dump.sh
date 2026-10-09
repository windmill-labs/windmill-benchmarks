#!/bin/bash
# Dump per-unit state transitions for every bench tag, straight out of Prefect's DB.
cd /home/ubuntu
Q_TASK="copy (
select fr.tags->>0 as tag, tr.id::text,
       extract(epoch from tr.created) as rec_created,
       extract(epoch from tr.expected_start_time) as expected_start,
       extract(epoch from min(s.timestamp) filter (where s.type='PENDING')) as pending,
       extract(epoch from min(s.timestamp) filter (where s.type='RUNNING')) as running,
       extract(epoch from max(s.timestamp) filter (where s.type='COMPLETED')) as completed,
       tr.state_type::text
from task_run tr
join flow_run fr on fr.id = tr.flow_run_id
left join task_run_state s on s.task_run_id = tr.id
where left(fr.tags->>0,3) = 'ip_'
group by fr.tags->>0, tr.id, tr.created, tr.expected_start_time, tr.state_type
) to stdout with csv header"
Q_FLOW="copy (
select fr.tags->>0 as tag, fr.id::text,
       extract(epoch from fr.created) as rec_created,
       extract(epoch from fr.expected_start_time) as expected_start,
       extract(epoch from min(s.timestamp) filter (where s.type='PENDING')) as pending,
       extract(epoch from min(s.timestamp) filter (where s.type='RUNNING')) as running,
       extract(epoch from max(s.timestamp) filter (where s.type='COMPLETED')) as completed,
       fr.state_type::text
from flow_run fr
left join flow_run_state s on s.flow_run_id = fr.id
where left(fr.tags->>0,3) in ('wp_','wt_')
group by fr.tags->>0, fr.id, fr.created, fr.expected_start_time, fr.state_type
) to stdout with csv header"
sudo docker compose exec -T db psql -U prefect -d prefect -c "$Q_TASK" > /home/ubuntu/task_units.csv
sudo docker compose exec -T db psql -U prefect -d prefect -c "$Q_FLOW" > /home/ubuntu/flow_units.csv
wc -l /home/ubuntu/task_units.csv /home/ubuntu/flow_units.csv
