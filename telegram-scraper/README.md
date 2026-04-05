# Telegram IPTV Scraper

Scrapes M3U URLs and Xtream credentials from Telegram channels.

## Setup

### 1. Get Telegram API Credentials

1. Go to https://my.telegram.org/apps
2. Login with your phone number (+447926275509)
3. Create a new application
4. Copy the `api_id` and `api_hash`

### 2. Update config.env

Edit `config.env` with your credentials:

```
TELEGRAM_API_ID=your_api_id_here
TELEGRAM_API_HASH=your_api_hash_here
TELEGRAM_PHONE=+447926275509
CHANNEL_NAME=LISTA IPTV GRÁTIS
OUTPUT_DIR=/app/output
MESSAGE_LIMIT=100
```

### 3. Build and Run

```bash
cd /home/ubuntu/aiostreams/telegram-scraper

# Build the container
sudo docker build -t telegram-scraper .

# Run once
sudo docker run --rm -it \
  --network host \
  -v $(pwd)/output:/app/output \
  -v $(pwd)/session.session:/app/session.session \
  --env-file config.env \
  telegram-scraper
```

On first run, you'll receive a code via Telegram to authorize the session.

### 4. Scheduled Runs (12-hour)

To run the full IPTV refresh pipeline every 12 hours:

```bash
# Create a systemd timer
cat > ~/.config/systemd/user/telegram-scraper.service << 'EOF'
[Unit]
Description=Telegram IPTV refresh pipeline

[Service]
Type=oneshot
WorkingDirectory=/home/ubuntu/aiostreams/telegram-scraper
ExecStartPre=/usr/bin/docker build -t telegram-scraper .
ExecStart=/home/ubuntu/aiostreams/scripts/run_iptv_refresh_pipeline.sh
EOF

cat > ~/.config/systemd/user/telegram-scraper.timer << 'EOF'
[Unit]
Description=Run Telegram IPTV Scraper every 12 hours

[Timer]
OnBootSec=5min
OnUnitActiveSec=12h
Unit=telegram-scraper.service

[Install]
WantedBy=timers.target
EOF

systemctl --user daemon-reload
systemctl --user enable --now telegram-scraper.timer
```

The wrapper script now performs:

1. Telegram scrape refresh
2. `iptv-aggregator` maintenance export into `output/telegram_production/addon/`
3. personal addon health + manifest checks

## Output

- `output/sources.json` - Ready for iptv-aggregator-v2
- `output/latest_credentials.json` - Current credentials
- `output/messages_YYYYMMDD_HHMMSS.json` - Raw message data
- `output/credentials_YYYYMMDD_HHMMSS.json` - Timestamped credentials
