#!/bin/bash
# Diagnostic only: is the ~120 activities/s dispatch ceiling set by the
# compose default of 4 history shards?
set -uo pipefail
cd ~/tc
cat > override_shards.yml <<'EOF'
services:
  temporal:
    environment:
      - NUM_HISTORY_SHARDS=512
EOF
docker compose -f docker-compose-postgres.yml down -v >/dev/null 2>&1
git checkout -- dynamicconfig/development-sql.yaml
docker compose -f docker-compose-postgres.yml -f override_shards.yml up -d >/dev/null 2>&1
for i in $(seq 1 60); do
  docker exec temporal-admin-tools temporal operator cluster health 2>/dev/null | grep -qi serving && break
  sleep 3
done
sleep 10
docker exec temporal cat /etc/temporal/config/docker.yaml | grep -i numHistoryShards
SETTLE=5 PRERELEASE=0 bash ~/run_combo2.sh stock sleep 2000 20 4 5 0 diag_shards512_stock_sleep.json
echo "DIAG COMPLETE"
