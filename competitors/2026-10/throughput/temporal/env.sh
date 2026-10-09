set -uo pipefail
docker exec temporal-postgresql postgres --version 2>/dev/null
docker inspect temporal --format '{{.Config.Image}}' 2>/dev/null
docker inspect temporal-postgresql --format '{{.Config.Image}}' 2>/dev/null
grep -c "^processor" /proc/cpuinfo
lscpu | grep -E "^Core|^Thread|^Socket|^Model name|^CPU\(s\)" 
curl -s --max-time 3 http://169.254.169.254/latest/meta-data/instance-type 2>/dev/null; echo
docker logs temporal 2>&1 | grep -iE "numHistoryShards|history shard" | head -3
