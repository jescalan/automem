#!/usr/bin/env python3
"""Clean up duplicate Context memories from AutoMem."""

import argparse
import requests
import redis
from collections import defaultdict

ENDPOINT = "http://localhost:8001"
TOKEN = "olly-automem-2026"
HEADERS = {"Authorization": f"Bearer {TOKEN}"}

def normalize(content):
    if not content:
        return ''
    c = content.lower()
    for prefix in ['user is ', 'user has ', 'user was ', 'user ', 'jeff ', 'the user ']:
        if c.startswith(prefix):
            c = c[len(prefix):]
    # Remove common variations
    c = c.replace("'s ", " ").replace("'", "")
    return c[:60].strip()

def get_context_memories():
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)
    
    query = '''
        MATCH (m:Memory)
        WHERE m.type = "Context"
        RETURN m.id, m.content, m.timestamp
        ORDER BY m.timestamp ASC
    '''
    
    result = r.execute_command('GRAPH.QUERY', 'memories', query)
    rows = result[1] if len(result) > 1 else []
    
    memories = []
    for row in rows:
        mid = row[0].decode() if isinstance(row[0], bytes) else row[0]
        content = row[1].decode() if isinstance(row[1], bytes) else row[1]
        ts = row[2].decode() if isinstance(row[2], bytes) else str(row[2])
        memories.append({'id': mid, 'content': content, 'timestamp': ts})
    
    return memories

def find_duplicates(memories):
    groups = defaultdict(list)
    
    for mem in memories:
        key = normalize(mem['content'])
        groups[key].append(mem)
    
    # Find groups with duplicates, keep oldest
    duplicates = []
    for key, mems in groups.items():
        if len(mems) > 1:
            # Sort by timestamp, keep oldest
            sorted_mems = sorted(mems, key=lambda x: x['timestamp'] or '')
            for dupe in sorted_mems[1:]:
                duplicates.append(dupe)
    
    return duplicates

def delete_memory(memory_id):
    resp = requests.delete(f"{ENDPOINT}/memory/{memory_id}", headers=HEADERS)
    return resp.status_code == 200

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    
    print("🔍 Finding Context memories...")
    memories = get_context_memories()
    print(f"   Found {len(memories)} Context memories")
    
    print("\n🔍 Finding duplicates...")
    duplicates = find_duplicates(memories)
    print(f"   Found {len(duplicates)} duplicates to remove")
    
    if not duplicates:
        print("\n✅ No duplicates found!")
        return
    
    if args.dry_run:
        print("\n🔎 Dry run - samples:")
        for d in duplicates[:10]:
            print(f"   • {d['content'][:60]}...")
        if len(duplicates) > 10:
            print(f"   ... and {len(duplicates) - 10} more")
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
