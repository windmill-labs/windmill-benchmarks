set -euo pipefail
cd ~/tc
sed -i 's/^TEMPORAL_VERSION=.*/TEMPORAL_VERSION=1.29.7/' .env
grep '^TEMPORAL_VERSION' .env
# ensure dynamicconfig is the published one (stock)
git checkout -- dynamicconfig/development-sql.yaml
docker compose -f docker-compose-postgres.yml up -d 2>&1 | tail -20
