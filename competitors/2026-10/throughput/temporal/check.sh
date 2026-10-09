set -uo pipefail
for i in $(seq 1 40); do
  if docker exec temporal-admin-tools temporal operator cluster health 2>/dev/null | grep -qi serving; then echo "HEALTHY after ${i}"; break; fi
  sleep 3
done
docker exec temporal-admin-tools temporal operator cluster system 2>&1 | head -20
docker exec temporal-admin-tools temporal --version
docker logs temporal 2>&1 | grep -i "numHistoryShards\|history shards" | head -5
docker exec temporal-admin-tools temporal operator namespace describe default 2>&1 | head -20
