#!/usr/bin/env python3
"""
Cleanup duplicate memories from AutoMem.

Finds memories with similar content (high vector similarity) and keeps only one.
"""

import argparse
import requests
import redis
import json
from collections import defaultdict

ENDPOINT = "http://localhost:8001"
TOKEN = "olly-automem-2026"
HEADERS = {"Authorization": f"Bearer {TOKEN}"}

FALKORDB_HOST = "localhost"
FALKORDB_PORT = 6379
GRAPH_NAME = "memories"


def get_all_memories():
    """Get all Decision/Insight memories from FalkorDB."""
    r = redis.Redis(host=FALKORDB_HOST, port=FALKORDB_PORT, decode_responses=False)
    
    query = '''
        MATCH (m:Memory)
        WHERE m.type IN ["Decision", "Insight", "Pattern", "Preference"]
        RETURN m.id, m.content, m.type, m.timestamp
        ORDER BY m.timestamp DESC
    '''
    
    result = r.execute_command('GRAPH.QUERY', GRAPH_NAME, query)
    rows = result[1] if len(result) > 1 else []
    
    memories = []
    for row in rows:
        mid = row[0].decode() if isinstance(row[0], bytes) else row[0]
        content = row[1].decode() if isinstance(row[1], bytes) else row[1]
        mtype = row[2].decode() if isinstance(row[2], bytes) else row[2]
        ts = row[3].decode() if isinstance(row[3], bytes) else row[3]
        memories.append({
            'id': mid,
            'content': content,
            'type': mtype,
            'timestamp': ts
        })
    
    return memories


def normalize_content(content):
    """Normalize content for comparison."""
    if not content:
        return ""
    # Lowercase
    content = content.lower()
    # Remove common prefixes
    for prefix in ['user decided to ', 'user decided ', 'user realized that ', 
                   'user realized ', 'user noted ', 'jeff decided to ', 'jeff ']:
        if content.startswith(prefix):
            content = content[len(prefix):]
    return content[:100].strip()


def find_duplicates(memories):
    """Find duplicate memories based on content similarity."""
    # Group by normalized content prefix
    groups = defaultdict(list)
    
    for mem in memories:
        key = normalize_content(mem['content'])
        groups[key].append(mem)
    
    # Find groups with duplicates
    duplicates = []
    for key, mems in groups.items():
        if len(mems) > 1:
            # Keep the oldest one, mark others as duplicates
            sorted_mems = sorted(mems, key=lambda x: x['timestamp'] or '')
            keep = sorted_mems[0]
            for dupe in sorted_mems[1:]:
                duplicates.append({
                    'id': dupe['id'],
                    'content': dupe['content'][:80],
                    'kept_id': keep['id']
                })
    
    return duplicates


def delete_memory(memory_id):
    """Delete a memory by ID."""
    resp = requests.delete(f"{ENDPOINT}/memory/{memory_id}", headers=HEADERS)
    return resp.status_code == 200


def main():
    parser = argparse.ArgumentParser(description="Clean up duplicate memories")
    parser.add_argument("--dry-run", action="store_true", help="Show duplicates without deleting")
    args = parser.parse_args()
    
    print("🔍 Finding all Decision/Insight/Pattern/Preference memories...")
    memories = get_all_memories()
    print(f"   Found {len(memories)} memories")
    
    print("\n🔍 Finding duplicates...")
    duplicates = find_duplicates(memories)
    print(f"   Found {len(duplicates)} duplicates to remove")
    
    if not duplicates:
        print("\n✅ No duplicates found!")
        return
    
    if args.dry_run:
        print("\n🔎 Dry run - would delete:")
        for d in duplicates[:20]:
            print(f"   • {d['content']}...")
        if len(duplicates) > 20:
            print(f"   ... and {len(duplicates) - 20} more")
        return
    
    print(f"\n🗑️  Deleting {len(duplicates)} duplicates...")
    deleted = 0
    failed = 0
    
    for i, d in enumerate(duplicates):
        if delete_memory(d['id']):
            deleted += 1
        else:
            failed += 1
        
        if (i + 1) % 50 == 0:
            print(f"   Progress: {i + 1}/{len(duplicates)} (deleted: {deleted}, failed: {failed})")
    
    print(f"\n✅ Complete: deleted {deleted}, failed {failed}")


if __name__ == "__main__":
    main()
