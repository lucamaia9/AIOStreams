#!/bin/bash
# BitMagnet Backfill Script
# Scheduled for 24h after fast-path deployment
# This script will reprocess orphaned torrents to benefit from fast-path optimization

cd /home/ubuntu/aiostreams

echo "Starting BitMagnet backfill at $(date)"

# Check system stability first
UPTIME=$(uptime -p)
echo "System uptime: $UPTIME"

# Check BitMagnet health
if ! sudo docker compose ps bitmagnet | grep -q "Up"; then
    echo "ERROR: BitMagnet is not running"
    exit 1
fi

# Check fast-path is working
FAST_PATH_HITS=$(sudo docker compose logs bitmagnet 2>&1 | grep "fast-path completed" | tail -10 | wc -l)
if [ "$FAST_PATH_HITS" -lt 5 ]; then
    echo "WARNING: Fast-path not showing significant activity"
fi

echo "Fast-path activity verified"

# Get orphan count
ORPHANS=$(sudo docker compose exec -T bitmagnet-postgres psql -U postgres -d bitmagnet -t -c "SELECT COUNT(*) FROM torrents t WHERE NOT EXISTS (SELECT 1 FROM torrent_contents tc WHERE tc.info_hash = t.info_hash AND tc.content_source IS NOT NULL);" 2>/dev/null | tr -d " ")
echo "Orphaned torrents to process: $ORPHANS"

# Enqueue batch processing via GraphQL
# Process in chunks of 5000, prioritizing newest first
for i in {1..240}; do
    echo "Batch $i: Enqueueing orphan processing..."
    
    curl -s -X POST http://localhost:3333/graphql \
        -H "Content-Type: application/json" \
        -d "{
            \"query\": \"mutation EnqueueBatch(\$params: EnqueueTorrentProcessingBatchInput!) { enqueueTorrentProcessingBatch(params: \$params) }\",
            \"variables\": {
                \"params\": {
                    \"orphan\": true,
                    \"batchSize\": 500,
                    \"chunkSize\": 5000,
                    \"classifyMode\": \"default\"
                }
            }
        }" > /dev/null
    
    sleep 5
done

echo "Backfill job scheduling complete at $(date)"

