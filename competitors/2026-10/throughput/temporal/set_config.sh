#!/bin/bash
# set_config.sh <stock|tuned>   -- recreates the whole stack with a clean DB
set -euo pipefail
CFG=$1
cd ~/tc
docker compose -f docker-compose-postgres.yml down -v >/dev/null 2>&1 || true
git checkout -- dynamicconfig/development-sql.yaml
if [ "$CFG" = "tuned" ]; then
cat >> dynamicconfig/development-sql.yaml <<'EOF'
history.transferProcessorMaxPollRPS:
  - value: 1000
    constraints: {}
EOF
fi
echo "--- dynamicconfig/development-sql.yaml ---"; cat dynamicconfig/development-sql.yaml
docker compose -f docker-compose-postgres.yml up -d >/dev/null 2>&1
for i in $(seq 1 60); do
  if docker exec temporal-admin-tools temporal operator cluster health 2>/dev/null | grep -qi serving; then echo "healthy after ${i}x3s"; break; fi
  sleep 3
done
sleep 10
docker logs temporal 2>&1 | grep -i "transferProcessorMaxPollRPS" | tail -5 || echo "(no transferProcessorMaxPollRPS line in log)"
docker exec temporal-admin-tools temporal operator cluster system | tail -2
