#!/usr/bin/env bash
set -euo pipefail

if [ ! -d "./.venv" ]; then
  python3 -m venv .venv
fi
source .venv/bin/activate

pip install --upgrade pip >/dev/null
pip install acryl-datahub >/dev/null

datahub docker quickstart

docker run -d \
  --name ingestion-mysql \
  --network datahub_network \
  -e MYSQL_ROOT_PASSWORD=pass \
  -e MYSQL_DATABASE=kestra \
  mysql:8

GMS_URL="http://localhost:8080/config"
for i in {1..40}; do
  if curl -s "$GMS_URL" >/dev/null; then
    break
  fi
  sleep 3
done
