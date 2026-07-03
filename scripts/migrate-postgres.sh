#!/usr/bin/env bash
set -euo pipefail

for migration in docker/postgres/migrations/*.sql; do
  printf 'aplicando %s\n' "$migration"
  docker compose exec -T postgres psql -v ON_ERROR_STOP=1 \
    -U ingestorx -d ingestorx < "$migration"
done
