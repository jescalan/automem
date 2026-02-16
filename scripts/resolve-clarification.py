#!/usr/bin/env python3
"""
Resolve a pending memory clarification.

Usage:
  resolve-clarification.py list              # Show pending clarifications
  resolve-clarification.py resolve <id> <clarified_content>  # Store with clarification
  resolve-clarification.py skip <id>         # Discard the memory
  resolve-clarification.py skip-all          # Discard all pending
"""

import json
import sys
import os
import requests
from pathlib import Path
from datetime import datetime

CLARIFICATION_QUEUE_FILE = Path(os.path.expanduser("~/.clawdbot/memory-clarification-queue.json"))
AUTOMEM_ENDPOINT = os.environ.get("AUTOMEM_ENDPOINT", "http://localhost:8001")
AUTOMEM_API_TOKEN = os.environ.get("AUTOMEM_API_TOKEN", "olly-automem-2026")


def load_queue() -> list:
    if CLARIFICATION_QUEUE_FILE.exists():
        try:
            return json.loads(CLARIFICATION_QUEUE_FILE.read_text())
        except json.JSONDecodeError:
            return []
    return []


def save_queue(queue: list):
    CLARIFICATION_QUEUE_FILE.write_text(json.dumps(queue, indent=2))


def list_pending():
    """List all pending clarifications"""
    queue = load_queue()
    pending = [e for e in queue if e.get("status") == "pending"]
    
    if not pending:
        print("No pending clarifications.")
        return
    
    print(f"\n📋 {len(pending)} pending clarification(s):\n")
    for entry in pending:
        mem = entry.get("memory", {})
        print(f"ID: {entry['id']}")
        print(f"  Content: {mem.get('content', '')[:80]}...")
        print(f"  Question: {mem.get('clarification_prompt', 'N/A')}")
        print(f"  Queued: {entry.get('queued_at', 'unknown')}")
        print()


def resolve(entry_id: str, clarified_content: str):
    """Resolve a pending clarification with the provided content"""
    queue = load_queue()
    
    entry = None
    for e in queue:
        if e.get("id") == entry_id:
            entry = e
            break
    
    if not entry:
        print(f"Error: Entry '{entry_id}' not found")
        sys.exit(1)
    
    mem = entry.get("memory", {})
    
    # Store the clarified memory
    metadata = {}
    if entry.get("source_session"):
        metadata["source_session"] = entry["source_session"]
    if entry.get("segment_timestamps"):
        metadata["segment_start"] = entry["segment_timestamps"][0]
        metadata["segment_end"] = entry["segment_timestamps"][1]
    metadata["clarified_at"] = datetime.utcnow().isoformat()
    metadata["original_content"] = mem.get("content", "")
    
    try:
        resp = requests.post(
            f"{AUTOMEM_ENDPOINT}/memory",
            headers={
                "Authorization": f"Bearer {AUTOMEM_API_TOKEN}",
                "Content-Type": "application/json"
            },
            json={
                "content": clarified_content,
                "type": mem.get("type", "Context"),
                "importance": mem.get("importance", 0.7),
                "tags": mem.get("tags", []) + ["clarified", "extracted", "conversation"],
                "metadata": metadata
            },
            timeout=15
        )
        
        if resp.status_code in (200, 201):
            print(f"✅ Stored clarified memory: {clarified_content[:60]}...")
            # Mark as resolved
            entry["status"] = "resolved"
            entry["resolved_at"] = datetime.utcnow().isoformat()
            entry["resolved_content"] = clarified_content
            save_queue(queue)
        else:
            print(f"Error storing memory: {resp.status_code} - {resp.text[:100]}")
            sys.exit(1)
            
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


def skip(entry_id: str):
    """Skip/discard a pending clarification"""
    queue = load_queue()
    
    for entry in queue:
        if entry.get("id") == entry_id:
            entry["status"] = "skipped"
            entry["skipped_at"] = datetime.utcnow().isoformat()
            save_queue(queue)
            print(f"⏭️ Skipped: {entry.get('memory', {}).get('content', '')[:60]}...")
            return
    
    print(f"Error: Entry '{entry_id}' not found")
    sys.exit(1)


def skip_all():
    """Skip all pending clarifications"""
    queue = load_queue()
    count = 0
    
    for entry in queue:
        if entry.get("status") == "pending":
            entry["status"] = "skipped"
            entry["skipped_at"] = datetime.utcnow().isoformat()
            count += 1
    
    save_queue(queue)
    print(f"⏭️ Skipped {count} pending clarification(s)")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    
    cmd = sys.argv[1]
    
    if cmd == "list":
        list_pending()
    elif cmd == "resolve" and len(sys.argv) >= 4:
        resolve(sys.argv[2], " ".join(sys.argv[3:]))
    elif cmd == "skip" and len(sys.argv) >= 3:
        skip(sys.argv[2])
    elif cmd == "skip-all":
        skip_all()
    else:
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
