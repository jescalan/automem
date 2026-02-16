#!/bin/bash
# AutoMem Sync Monitor - Alerts if FalkorDB/Qdrant drift detected

THRESHOLD=10  # Alert if difference > 10

# Get counts
HEALTH=$(curl -s "http://localhost:8001/health" -H "Authorization: Bearer olly-automem-2026")
FALKOR_COUNT=$(echo "$HEALTH" | jq -r '.memory_count // 0')
QDRANT_COUNT=$(echo "$HEALTH" | jq -r '.vector_count // 0')
SYNC_STATUS=$(echo "$HEALTH" | jq -r '.sync_status // "unknown"')

DIFF=$((QDRANT_COUNT - FALKOR_COUNT))
ABS_DIFF=${DIFF#-}  # Absolute value

echo "$(date '+%Y-%m-%d %H:%M') | FalkorDB: $FALKOR_COUNT | Qdrant: $QDRANT_COUNT | Status: $SYNC_STATUS"

if [ "$ABS_DIFF" -gt "$THRESHOLD" ]; then
    echo "⚠️ ALERT: Sync drift detected! Difference: $DIFF"
    # Could add notification here
    exit 1
fi

if [ "$SYNC_STATUS" = "orphaned_vectors" ]; then
    echo "⚠️ ALERT: Orphaned vectors detected!"
    exit 1
fi

exit 0
