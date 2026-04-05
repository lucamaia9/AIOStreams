#!/usr/bin/env bash
set -euo pipefail

UNIT_DIR="$HOME/.config/systemd/user"
mkdir -p "$UNIT_DIR"

cp /home/ubuntu/aiostreams/telegram-scraper/ops/systemd/telegram-scraper.service "$UNIT_DIR/"
cp /home/ubuntu/aiostreams/telegram-scraper/ops/systemd/telegram-scraper.timer "$UNIT_DIR/"

systemctl --user daemon-reload
systemctl --user enable --now telegram-scraper.timer
systemctl --user status telegram-scraper.timer --no-pager | sed -n '1,14p'
