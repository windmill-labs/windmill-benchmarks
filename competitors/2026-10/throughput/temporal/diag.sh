date -u
ls -l --time-style=+%H:%M:%S ~/bench/run_all.log
docker ps --format "{{.Names}} {{.Status}}"
echo "--- actw log ---"; docker logs bench-actw 2>&1 | tail -5
echo "--- ctl ---"; ls -l --time-style=+%H:%M:%S ~/bench/ctl/
echo "--- uptime ---"; uptime
