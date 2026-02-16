#!/usr/bin/env python3
"""
Transcript Sync — watches Clawdbot session transcripts and feeds AutoMem
Usage: transcript-sync.py [--once] [--backfill]
"""

import json
import os
import sys
import time
import argparse
import requests
from pathlib import Path
from datetime import datetime

AUTOMEM_ENDPOINT = os.environ.get("AUTOMEM_ENDPOINT", "http://localhost:8001")
AUTOMEM_API_TOKEN = os.environ.get("AUTOMEM_API_TOKEN", "olly-automem-2026")

# SAFETY: This legacy transcript sync stores near-raw turns and tends to create a lot of junk
# (and therefore prompt bloat). It is DISABLED by default. To enable explicitly:
#   AUTOMEM_TRANSCRIPT_SYNC_ENABLED=1
TRANSCRIPT_SYNC_ENABLED = os.environ.get("AUTOMEM_TRANSCRIPT_SYNC_ENABLED", "0") in ("1", "true", "yes")

SESSION_DIR = Path(os.environ.get("CLAWDBOT_SESSION_DIR", os.path.expanduser("~/.clawdbot/agents/main/sessions")))
STATE_FILE = Path(os.path.expanduser("~/.clawdbot/automem-sync-state.json"))

def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {}

def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2))

def store_memory(content: str, mem_type: str = "Context", importance: float = 0.6, tags: list = None):
    """Store a single memory in AutoMem"""
    if tags is None:
        tags = ["transcript", "conversation"]
    
    try:
        resp = requests.post(
            f"{AUTOMEM_ENDPOINT}/memory",
            headers={
                "Authorization": f"Bearer {AUTOMEM_API_TOKEN}",
                "Content-Type": "application/json"
            },
            json={
                "content": content[:4000],  # Truncate very long content
                "type": mem_type,
                "importance": importance,
                "tags": tags
            },
            timeout=10
        )
        return resp.status_code in (200, 201)
    except Exception as e:
        print(f"  Error storing memory: {e}", file=sys.stderr)
        return False

def extract_message_content(msg_data: dict) -> tuple[str, str]:
    """Extract role and text content from a message"""
    msg = msg_data.get("message", {})
    role = msg.get("role", "")
    content_parts = msg.get("content", [])
    
    text_parts = []
    for part in content_parts:
        if isinstance(part, str):
            text_parts.append(part)
        elif isinstance(part, dict):
            if part.get("type") == "text":
                text_parts.append(part.get("text", ""))
            # Skip thinking blocks, tool calls, etc.
    
    return role, " ".join(text_parts)

def calculate_importance(role: str, content: str) -> float:
    """Calculate importance score based on content"""
    importance = 0.5
    
    # User messages slightly higher
    if role == "user":
        importance = 0.6
    
    # Boost for decision/preference indicators
    decision_words = ["decide", "prefer", "want", "choose", "should", "must", "need", "remember", "don't forget"]
    if any(word in content.lower() for word in decision_words):
        importance = min(importance + 0.15, 0.85)
    
    # Boost for questions (likely important context)
    if "?" in content:
        importance = min(importance + 0.1, 0.85)
    
    # Reduce importance for very short messages
    if len(content) < 50:
        importance = max(importance - 0.1, 0.3)
    
    return importance

def process_file(jsonl_path: Path, state: dict, backfill: bool = False) -> int:
    """Process a single JSONL transcript file"""
    filename = jsonl_path.name
    last_line = state.get(filename, 0)
    
    if not backfill and last_line == 0:
        # For new files without backfill, start from current position
        # to avoid ingesting entire history on first run
        line_count = sum(1 for _ in open(jsonl_path))
        state[filename] = line_count
        print(f"  Skipping backfill for {filename} ({line_count} lines)")
        return 0
    
    processed = 0
    current_line = 0
    
    with open(jsonl_path, "r") as f:
        for line in f:
            current_line += 1
            
            if current_line <= last_line:
                continue
            
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                continue
            
            # Only process message types
            if data.get("type") != "message":
                continue
            
            role, content = extract_message_content(data)
            
            # Skip empty or very short content
            if not content or len(content) < 10:
                continue
            
            # Skip tool results and system messages (usually verbose)
            if role not in ("user", "assistant"):
                continue
            
            importance = calculate_importance(role, content)
            tags = ["transcript", "conversation", role]
            
            # Prefix with role for context
            full_content = f"[{role}] {content}"
            
            if store_memory(full_content, "Context", importance, tags):
                processed += 1
            
            # Small delay to avoid overwhelming the API
            time.sleep(0.05)
    
    state[filename] = current_line
    return processed

def sync_once(backfill: bool = False):
    """Process all transcript files once"""
    if not TRANSCRIPT_SYNC_ENABLED:
        return 0

    state = load_state()
    total_processed = 0
    
    for jsonl_path in SESSION_DIR.glob("*.jsonl"):
        if jsonl_path.suffix == ".lock":
            continue
        
        processed = process_file(jsonl_path, state, backfill)
        if processed > 0:
            print(f"[{datetime.now().isoformat()}] Processed {processed} messages from {jsonl_path.name}")
        total_processed += processed
    
    save_state(state)
    return total_processed

def main():
    parser = argparse.ArgumentParser(description="Sync Clawdbot transcripts to AutoMem")
    parser.add_argument("--once", action="store_true", help="Process once and exit")
    parser.add_argument("--backfill", action="store_true", help="Process entire history (otherwise skips existing)")
    parser.add_argument("--interval", type=int, default=30, help="Sync interval in seconds (default: 30)")
    args = parser.parse_args()
    
    if args.once:
        processed = sync_once(args.backfill)
        print(f"Total processed: {processed}")
    else:
        print(f"[{datetime.now().isoformat()}] Starting transcript sync (interval: {args.interval}s)")
        print(f"  Session dir: {SESSION_DIR}")
        print(f"  AutoMem: {AUTOMEM_ENDPOINT}")
        if not TRANSCRIPT_SYNC_ENABLED:
            print("  NOTE: AUTOMEM_TRANSCRIPT_SYNC_ENABLED is not set; legacy transcript sync is DISABLED (no-op).")
        
        while True:
            try:
                sync_once(backfill=False)  # Never backfill in continuous mode
            except Exception as e:
                print(f"[{datetime.now().isoformat()}] Error: {e}", file=sys.stderr)
            
            time.sleep(args.interval)

if __name__ == "__main__":
    main()
