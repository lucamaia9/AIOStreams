#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/home/ubuntu/aiostreams"
SCRAPER_DIR="$ROOT_DIR/telegram-scraper"
AGGREGATOR_DIR="$ROOT_DIR/iptv-aggregator"
SCRAPER_OUTPUT="$SCRAPER_DIR/output/sources_clean.json"
AGGREGATOR_OUTPUT_DIR="${IPTV_AGGREGATOR_OUTPUT_DIR:-$AGGREGATOR_DIR/output/telegram_production}"
ADDON_CATALOG_PATH="$AGGREGATOR_OUTPUT_DIR/addon/addon_catalog.json"
ADDON_MANIFEST_URL="${IPTV_ADDON_MANIFEST_URL:-https://iptv.mrdouglas.uk/manifest.json}"
ADDON_HEALTH_URL="${IPTV_ADDON_HEALTH_URL:-https://iptv.mrdouglas.uk/health}"
ADDON_BASE_URL="${ADDON_MANIFEST_URL%/manifest.json}"
ADDON_COMPOSE_SERVICE="${IPTV_ADDON_COMPOSE_SERVICE:-iptv-addon}"
FALLBACK_IMAGE_PATH="${IPTV_ADDON_FALLBACK_IMAGE_PATH:-$ROOT_DIR/iptv-addon/src/assets/personal-tv-fallback.png}"
MIN_CHANNEL_LOGO_COVERAGE="${IPTV_MIN_CHANNEL_LOGO_COVERAGE:-0.20}"
MIN_LOGO_ASSET_COVERAGE="${IPTV_MIN_LOGO_ASSET_COVERAGE:-0.20}"
LOGO_SAMPLE_CHANNEL_IDS="${IPTV_LOGO_SAMPLE_CHANNEL_IDS:-amc,cartoon.network,warner.channel,discovery.channel}"

echo "[iptv-pipeline] Starting Telegram scraper refresh"
docker run --rm --network host \
  -v "$SCRAPER_DIR/output:/app/output" \
  --env-file "$SCRAPER_DIR/config.env" \
  -e OUTPUT_DIR=/app/output \
  telegram-scraper

if [[ ! -f "$SCRAPER_OUTPUT" ]]; then
  echo "[iptv-pipeline] ERROR: expected scraper output not found at $SCRAPER_OUTPUT" >&2
  exit 1
fi

echo "[iptv-pipeline] Running aggregator maintenance export"
cd "$AGGREGATOR_DIR"
npm run start -- run-maintenance-telegram \
  --input "$SCRAPER_OUTPUT" \
  --output "$AGGREGATOR_OUTPUT_DIR" \
  --parallel "${IPTV_AGGREGATOR_PARALLEL:-6}" \
  --max-proven-servers "${IPTV_AGGREGATOR_MAX_PROVEN_SERVERS:-100}"

if [[ ! -f "$ADDON_CATALOG_PATH" ]]; then
  echo "[iptv-pipeline] ERROR: expected addon catalog not found at $ADDON_CATALOG_PATH" >&2
  exit 1
fi

echo "[iptv-pipeline] Restarting addon runtime"
cd "$ROOT_DIR"
docker compose restart "$ADDON_COMPOSE_SERVICE"

echo "[iptv-pipeline] Verifying addon health"
curl -fsS "$ADDON_HEALTH_URL" >/dev/null
curl -fsS "$ADDON_MANIFEST_URL" >/dev/null

echo "[iptv-pipeline] Verifying logo coverage and sampled live artwork"
ADDON_CATALOG_PATH="$ADDON_CATALOG_PATH" \
ADDON_MANIFEST_URL="$ADDON_MANIFEST_URL" \
ADDON_BASE_URL="$ADDON_BASE_URL" \
FALLBACK_IMAGE_PATH="$FALLBACK_IMAGE_PATH" \
MIN_CHANNEL_LOGO_COVERAGE="$MIN_CHANNEL_LOGO_COVERAGE" \
MIN_LOGO_ASSET_COVERAGE="$MIN_LOGO_ASSET_COVERAGE" \
LOGO_SAMPLE_CHANNEL_IDS="$LOGO_SAMPLE_CHANNEL_IDS" \
node - <<'NODE'
const fs = require('fs');
const crypto = require('crypto');

function fail(message) {
  console.error(`[iptv-pipeline] ERROR: ${message}`);
  process.exit(1);
}

function sha256(buffer) {
  return crypto.createHash('sha256').update(buffer).digest('hex');
}

async function main() {
  const catalogPath = process.env.ADDON_CATALOG_PATH;
  const manifestUrl = process.env.ADDON_MANIFEST_URL;
  const baseUrl = process.env.ADDON_BASE_URL;
  const fallbackPath = process.env.FALLBACK_IMAGE_PATH;
  const minChannelCoverage = Number(process.env.MIN_CHANNEL_LOGO_COVERAGE || '0.20');
  const minAssetCoverage = Number(process.env.MIN_LOGO_ASSET_COVERAGE || '0.20');
  const sampleIds = String(process.env.LOGO_SAMPLE_CHANNEL_IDS || '')
    .split(',')
    .map(value => value.trim())
    .filter(Boolean);

  const catalog = JSON.parse(fs.readFileSync(catalogPath, 'utf8'));
  const channels = Array.isArray(catalog.channels) ? catalog.channels : [];
  let channelLogos = 0;
  let logoAssets = 0;

  for (const channel of channels) {
    if (String(channel.logo || '').trim()) channelLogos += 1;
    if (String(channel.logoAssetPath || '').trim()) logoAssets += 1;
  }

  const channelCoverage = channels.length ? channelLogos / channels.length : 0;
  const assetCoverage = channels.length ? logoAssets / channels.length : 0;

  console.log(
    `[iptv-pipeline] Logo coverage: channel=${channelLogos}/${channels.length} (${channelCoverage.toFixed(3)}), asset=${logoAssets}/${channels.length} (${assetCoverage.toFixed(3)})`
  );

  if (channelCoverage < minChannelCoverage) {
    fail(`channel logo coverage ${channelCoverage.toFixed(3)} is below threshold ${minChannelCoverage.toFixed(3)}`);
  }
  if (assetCoverage < minAssetCoverage) {
    fail(`logo asset coverage ${assetCoverage.toFixed(3)} is below threshold ${minAssetCoverage.toFixed(3)}`);
  }

  const manifestResponse = await fetch(manifestUrl);
  if (!manifestResponse.ok) {
    fail(`manifest fetch failed with status ${manifestResponse.status}`);
  }
  const manifest = await manifestResponse.json();
  if (!/TV Doug/i.test(String(manifest.name || '')) || !manifest.catalogs?.some(catalog => /TV Doug/i.test(String(catalog.name || '')))) {
    fail(`live manifest still exposes stale branding: name=${JSON.stringify(manifest.name)}, catalogs=${JSON.stringify(manifest.catalogs || [])}`);
  }

  const metaSampleId = sampleIds.find(canonicalId => {
    const channel = channels.find(item => item.canonicalId === canonicalId);
    return channel && channel.status === 'ready' && channel.sourceCount > 0;
  }) || channels.find(channel => channel.status === 'ready' && channel.sourceCount > 0)?.canonicalId;

  if (!metaSampleId) {
    fail('could not find a ready channel for live meta verification');
  }

  const metaResponse = await fetch(`${baseUrl}/meta/tv/${encodeURIComponent(`personal_tv_${metaSampleId}`)}.json`);
  if (!metaResponse.ok) {
    fail(`meta fetch failed for ${metaSampleId} with status ${metaResponse.status}`);
  }
  const metaPayload = await metaResponse.json();
  const metaDescription = String(metaPayload?.meta?.description || '');
  if (/Status:|Category:|Raw group:|Observed support:/i.test(metaDescription)) {
    fail(`live meta for ${metaSampleId} still exposes stale debug fields: ${JSON.stringify(metaDescription)}`);
  }
  if (!/Servers found:/i.test(metaDescription)) {
    fail(`live meta for ${metaSampleId} is missing the compact server-count contract: ${JSON.stringify(metaDescription)}`);
  }

  const fallbackHash = sha256(fs.readFileSync(fallbackPath));
  for (const canonicalId of sampleIds) {
    const channel = channels.find(item => item.canonicalId === canonicalId);
    if (!channel) {
      fail(`sample channel ${canonicalId} not found in addon catalog`);
    }
    const posterUrl = `${baseUrl}/personal/logo/${encodeURIComponent(`personal_tv_${canonicalId}`)}.png`;
    const response = await fetch(posterUrl);
    if (!response.ok) {
      fail(`poster route failed for ${canonicalId} with status ${response.status}`);
    }
    const contentType = response.headers.get('content-type') || '';
    if (!contentType.startsWith('image/')) {
      fail(`poster route for ${canonicalId} returned non-image content-type ${contentType}`);
    }
    const buffer = Buffer.from(await response.arrayBuffer());
    const hash = sha256(buffer);
    if (hash === fallbackHash) {
      fail(`poster route for ${canonicalId} still resolves to fallback artwork`);
    }
  }
}

main().catch(error => fail(error.stack || error.message || String(error)));
NODE

echo "[iptv-pipeline] Completed successfully"
