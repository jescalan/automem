#!/bin/bash
# AutoMem Daily Backup

BACKUP_DIR=~/clawd/automem/backups-local
DATE=$(date +%Y-%m-%d)

mkdir -p "$BACKUP_DIR"

echo "=== AutoMem Backup $DATE ==="

# Get current counts for verification
HEALTH=$(curl -s "http://localhost:8001/health" -H "Authorization: Bearer olly-automem-2026")
FALKOR_COUNT=$(echo "$HEALTH" | jq -r '.memory_count // 0')
QDRANT_COUNT=$(echo "$HEALTH" | jq -r '.vector_count // 0')
echo "Current counts - FalkorDB: $FALKOR_COUNT, Qdrant: $QDRANT_COUNT"

# Trigger RDB save
docker exec automem-falkordb-1 redis-cli BGSAVE
sleep 5

# Export via docker to bypass permissions
docker exec automem-falkordb-1 cat /var/lib/falkordb/data/dump.rdb > "$BACKUP_DIR/falkordb-$DATE.rdb"

# Verify backup
if [ -f "$BACKUP_DIR/falkordb-$DATE.rdb" ]; then
    SIZE=$(ls -lh "$BACKUP_DIR/falkordb-$DATE.rdb" | awk '{print $5}')
    echo "✅ Backup saved: falkordb-$DATE.rdb ($SIZE)"
else
    echo "❌ Backup failed!"
    exit 1
fi

# Keep only last 7 days
find "$BACKUP_DIR" -name "falkordb-*.rdb" -mtime +7 -delete 2>/dev/null

echo "Backups in $BACKUP_DIR:"
ls -lh "$BACKUP_DIR"/*.rdb 2>/dev/null | tail -5
