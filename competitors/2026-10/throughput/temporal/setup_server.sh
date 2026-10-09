set -euxo pipefail
cd ~
rm -rf ~/tc
git clone --depth 1 https://github.com/temporalio/docker-compose.git ~/tc
cd ~/tc
git log -1 --format='%H %ci'
grep -n "TEMPORAL_VERSION\|POSTGRES" docker-compose-postgres.yml | head -40
echo "--- dynamicconfig ---"
ls dynamicconfig/
cat dynamicconfig/development-sql.yaml
