#!/usr/bin/env bash
set -euo pipefail

unit_dir="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
systemctl --user disable --now ingestorx-watcher.service ingestorx-consumer.service 2>/dev/null || true
rm -f "$unit_dir/ingestorx-watcher.service" "$unit_dir/ingestorx-consumer.service"
systemctl --user daemon-reload
printf '%s\n' 'Serviços systemd removidos; dados e secrets foram preservados.'
