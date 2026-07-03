#!/usr/bin/env bash
set -euo pipefail

quantidade="${1:-10000}"
concorrencia="${LOAD_TEST_CONCURRENCY:-8}"
entrada="${XML_WATCHER_BASE_DIR:-./dados}/entrada/load-test-$(date +%s)"

[[ "$quantidade" =~ ^[1-9][0-9]*$ ]] || { echo 'quantidade deve ser positiva' >&2; exit 2; }
[[ "$concorrencia" =~ ^[1-9][0-9]*$ ]] || { echo 'concorrência deve ser positiva' >&2; exit 2; }
mkdir -p "$entrada"

inicio=$(date +%s)
export entrada
seq 1 "$quantidade" | xargs -P "$concorrencia" -n 1 bash -c '
  i="$1"
  printf "<documento><id>%s</id><gerado_em>%s</gerado_em></documento>\n" \
    "$i" "$(date -u +%FT%TZ)" > "$entrada/$i.xml.tmp"
  mv "$entrada/$i.xml.tmp" "$entrada/$i.xml"
' _
fim=$(date +%s)
duracao=$((fim - inicio)); ((duracao == 0)) && duracao=1
printf 'gerados=%s duracao_s=%s taxa_arquivos_s=%s pasta=%s\n' \
  "$quantidade" "$duracao" "$((quantidade / duracao))" "$entrada"
