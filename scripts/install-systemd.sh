#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
workspace="$(pwd)"

if [[ "${1:-}" != "--yes" ]]; then
  printf '%s\n' \
    'Serão compilados e instalados dois serviços systemd do usuário:' \
    '  - ingestorx-watcher.service' \
    '  - ingestorx-consumer.service'
  read -r -p 'Deseja continuar? [s/N] ' resposta
  case "${resposta,,}" in
    s|sim|y|yes) ;;
    *) printf '%s\n' 'Instalação cancelada.'; exit 0 ;;
  esac
fi

command -v systemctl >/dev/null 2>&1 || {
  printf '%s\n' 'Erro: systemd não está disponível neste sistema.' >&2
  exit 1
}
test -f .env || { printf '%s\n' 'Execute `just setup` primeiro.' >&2; exit 1; }

cargo build --release --bins
unit_dir="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
mkdir -p "$unit_dir"

for nome in ingestorx-watcher ingestorx-consumer; do
  awk -v workspace="$workspace" '{gsub(/__WORKDIR__/, workspace); print}' \
    "systemd/${nome}.service.in" > "${unit_dir}/${nome}.service"
  chmod 600 "${unit_dir}/${nome}.service"
done

systemctl --user daemon-reload
systemctl --user enable --now ingestorx-watcher.service ingestorx-consumer.service

printf '%s\n' \
  'Serviços systemd instalados e iniciados.' \
  'Status: just service-status' \
  'Logs:   journalctl --user -u ingestorx-watcher -u ingestorx-consumer -f'

if command -v loginctl >/dev/null 2>&1 && \
   [[ "$(loginctl show-user "$USER" -p Linger --value 2>/dev/null || true)" != "yes" ]]; then
  printf '%s\n' \
    'Aviso: para iniciar sem login após o boot, execute uma vez:' \
    "  sudo loginctl enable-linger $USER"
fi
