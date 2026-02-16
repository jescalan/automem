#!/usr/bin/env python3
"""
Cleanup script to remove raw transcript messages from AutoMem.

The old transcript-sync.py stored raw messages like "[user] message..." and 
"[assistant] response..." which are low-value duplicates now that smart-sync
extracts meaningful memories.

This script finds and deletes these raw transcript entries.
"""

import argparse
import os
import requests
import sys
from typing import List, Tuple

import redis

ENDPOINT = "http://localhost:8001"
TOKEN = "olly-automem-2026"
HEADERS = {"Authorization": f"Bearer {TOKEN}"}

# FalkorDB connection (Redis-compatible)
FALKORDB_HOST = os.environ.get("FALKORDB_HOST", "localhost")
FALKORDB_PORT = int(os.environ.get("FALKORDB_PORT", 6379))
GRAPH_NAME = "memories"


def get_raw_transcript_ids() -> List[Tuple[str, str]]:
    """Find all memory IDs that are raw transcript dumps."""
    try:
        r = redis.Redis(host=FALKORDB_HOST, port=FALKORDB_PORT, decode_responses=False)
        
        # Query for memories starting with [user] or [assistant]
        query = '''
            MATCH (m:Memory)
            WHERE m.content STARTS WITH "[user]" OR m.content STARTS WITH "[assistant]"
            RETURN m.id, substring(m.content, 0, 80)
        '''
        
        result = r.execute_command('GRAPH.QUERY', GRAPH_NAME, query)
        
        # Result format: [[headers], [rows], [stats]]
        rows = result[1] if len(result) > 1 else []
        
        ids = []
        for row in rows:
            if row and len(row) >= 2:
                # Decode bytes
                mid = row[0].decode('utf-8') if isinstance(row[0], bytes) else str(row[0])
                preview = row[1].decode('utf-8', errors='replace') if isinstance(row[1], bytes) else str(row[1])
                ids.append((mid, preview))
        
        return ids
    except Exception as e:
        print(f"Error querying FalkorDB: {e}")
        import traceback
        traceback.print_exc()
        return []


def delete_memory(memory_id: str) -> bool:
    """Delete a single memory by ID."""
    resp = requests.delete(
        f"{ENDPOINT}/memory/{memory_id}",
        headers=HEADERS
    )
    return resp.status_code == 200


def main():
    parser = argparse.ArgumentParser(description="Remove raw transcript dumps from AutoMem")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be deleted without deleting")
    parser.add_argument("--limit", type=int, help="Limit number of deletions")
    args = parser.parse_args()
    
    print("🔍 Finding raw transcript messages...")
    results = get_raw_transcript_ids()
    
    if not results:
        print("✅ No raw transcript messages found!")
        return
    
    print(f"📊 Found {len(results)} raw transcript messages to delete")
    
    if args.dry_run:
        print("\n🔎 Dry run - samples of what would be deleted:")
        for i, (mid, preview) in enumerate(results[:20]):
            print(f"  {preview}...")
        if len(results) > 20:
            print(f"  ... and {len(results) - 20} more")
        return
    
    # Delete
    limit = args.limit or len(results)
    to_delete = results[:limit]
    
    print(f"\n🗑️  Deleting {len(to_delete)} memories...")
    
    deleted = 0
    failed = 0
    
    for i, (mid, _) in enumerate(to_delete):
        if delete_memory(mid):
            deleted += 1
        else:
            failed += 1
        
        if (i + 1) % 100 == 0:
            print(f"  Progress: {i + 1}/{len(to_delete)} (deleted: {deleted}, failed: {failed})")
    
    print(f"\n✅ Complete: deleted {deleted}, failed {failed}")
    
    remaining = len(results) - limit
    if remaining > 0:
        print(f"⚠️  {remaining} more to delete - run again to continue")


if __name__ == "__main__":
    main()
