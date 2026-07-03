#!/usr/bin/env bash
set -euo pipefail

mkdir -p backups
destino="backups/ingestorx-$(date -u +%Y%m%dT%H%M%SZ).sql.gz"
docker compose exec -T postgres pg_dump -U ingestorx -d ingestorx \
  --format=plain --no-owner --no-privileges | gzip -9 > "$destino"
chmod 600 "$destino"
printf 'backup criado: %s\n' "$destino"
