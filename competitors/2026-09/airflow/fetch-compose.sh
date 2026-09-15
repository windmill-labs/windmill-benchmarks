#!/usr/bin/env bash
# The Airflow run uses the official compose file, unmodified, at the pinned
# version. It is fetched rather than vendored so there is no chance of it
# drifting from what Apache publishes for 3.3.1.
set -euo pipefail
curl -fsSL -o docker-compose.yaml \
  https://airflow.apache.org/docs/apache-airflow/3.3.1/docker-compose.yaml
mkdir -p dags logs plugins config
cp -n .env.example .env
echo "fetched docker-compose.yaml for Airflow 3.3.1"
