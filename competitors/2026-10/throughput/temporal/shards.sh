set -uo pipefail
docker exec temporal env | grep -i shard || echo "(no shard env)"
docker exec temporal cat /etc/temporal/config/docker.yaml 2>/dev/null | grep -iA2 "numHistoryShards" || true
docker logs temporal 2>&1 | head -4
tail -4 ~/bench/run_all.log
