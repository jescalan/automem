#!/usr/bin/env bash
set -euo pipefail

# Transcript Sync — watches Clawdbot session transcripts and feeds AutoMem
# Usage: transcript-sync.sh [--once] [--session-dir DIR]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AUTOMEM_ENDPOINT="${AUTOMEM_ENDPOINT:-http://localhost:8001}"
AUTOMEM_API_TOKEN="${AUTOMEM_API_TOKEN:-olly-automem-2026}"
SESSION_DIR="${CLAWDBOT_SESSION_DIR:-$HOME/.clawdbot/agents/main/sessions}"
STATE_FILE="$HOME/.clawdbot/automem-sync-state.json"
ONCE=false

usage() {
  cat <<EOF
Usage: transcript-sync.sh [options]

Options:
  --once           Process once and exit (for cron)
  --session-dir    Override session directory
  --help           Show this help

Environment:
  AUTOMEM_ENDPOINT    API URL (default: http://localhost:8001)
  AUTOMEM_API_TOKEN   Auth token
EOF
  exit 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --once) ONCE=true; shift ;;
    --session-dir) SESSION_DIR="$2"; shift 2 ;;
    --help) usage ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

# Initialize state file if missing
if [[ ! -f "$STATE_FILE" ]]; then
  echo '{}' > "$STATE_FILE"
fi

get_last_line() {
  local file="$1"
  python3 -c "
import json
import sys
state_file = '$STATE_FILE'
with open(state_file) as f:
    state = json.load(f)
print(state.get('$file', 0))
"
}

set_last_line() {
  local file="$1"
  local line="$2"
  python3 -c "
import json
state_file = '$STATE_FILE'
with open(state_file) as f:
    state = json.load(f)
state['$1'] = $2
with open(state_file, 'w') as f:
    json.dump(state, f)
"
}

store_memory() {
  local content="$1"
  local msg_type="${2:-Context}"
  local importance="${3:-0.6}"
  local tags="${4:-transcript,conversation}"
  
  # Build tags array
  local tags_json=$(echo "$tags" | tr ',' '\n' | sed 's/^/"/;s/$/"/' | paste -sd ',' | sed 's/^/[/;s/$/]/')
  
  curl -s -X POST "${AUTOMEM_ENDPOINT}/memory" \
    -H "Authorization: Bearer ${AUTOMEM_API_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "$(python3 -c "
import json
import sys
content = '''$content'''
print(json.dumps({
    'content': content,
    'type': '$msg_type',
    'importance': $importance,
    'tags': $tags_json
}))
")" > /dev/null
}

process_file() {
  local jsonl_file="$1"
  local filename=$(basename "$jsonl_file")
  local last_line=$(get_last_line "$filename")
  local current_line=0
  local processed=0
  
  while IFS= read -r line; do
    current_line=$((current_line + 1))
    
    # Skip already processed lines
    if [[ $current_line -le $last_line ]]; then
      continue
    fi
    
    # Parse the JSON line
    local msg_type=$(echo "$line" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('type',''))" 2>/dev/null || echo "")
    
    # Only process message types
    if [[ "$msg_type" != "message" ]]; then
      continue
    fi
    
    # Extract message content
    local role=$(echo "$line" | python3 -c "
import json,sys
d=json.load(sys.stdin)
msg=d.get('message',{})
print(msg.get('role',''))
" 2>/dev/null || echo "")
    
    local content=$(echo "$line" | python3 -c "
import json,sys
d=json.load(sys.stdin)
msg=d.get('message',{})
parts=msg.get('content',[])
text_parts=[]
for p in parts:
    if isinstance(p, str):
        text_parts.append(p)
    elif isinstance(p, dict) and p.get('type')=='text':
        text_parts.append(p.get('text',''))
print(' '.join(text_parts)[:2000])  # Truncate very long messages
" 2>/dev/null || echo "")
    
    # Skip empty content
    if [[ -z "$content" || ${#content} -lt 10 ]]; then
      continue
    fi
    
    # Determine importance based on role and content length
    local importance="0.5"
    local mem_type="Context"
    
    if [[ "$role" == "user" ]]; then
      importance="0.6"
      mem_type="Context"
      # Higher importance for questions or decisions
      if echo "$content" | grep -qiE "(decide|prefer|want|should|how do|what is|remind|remember)"; then
        importance="0.7"
      fi
    elif [[ "$role" == "assistant" ]]; then
      importance="0.5"
      mem_type="Context"
      # Higher importance for decisions or learnings
      if echo "$content" | grep -qiE "(I'll remember|stored|decision|preference|important)"; then
        importance="0.7"
      fi
    fi
    
    # Prefix with role for context
    local full_content="[$role] $content"
    
    # Store in AutoMem
    store_memory "$full_content" "$mem_type" "$importance" "transcript,conversation,$role"
    processed=$((processed + 1))
    
    # Rate limit slightly
    sleep 0.1
    
  done < "$jsonl_file"
  
  # Update state
  if [[ $current_line -gt $last_line ]]; then
    set_last_line "$filename" "$current_line"
  fi
  
  if [[ $processed -gt 0 ]]; then
    echo "[$(date -Iseconds)] Processed $processed messages from $filename"
  fi
}

sync_once() {
  for jsonl_file in "$SESSION_DIR"/*.jsonl; do
    [[ -f "$jsonl_file" ]] || continue
    [[ "$jsonl_file" == *.lock ]] && continue
    process_file "$jsonl_file"
  done
}

# Main
if [[ "$ONCE" == "true" ]]; then
  sync_once
else
  echo "[$(date -Iseconds)] Starting transcript sync (watching $SESSION_DIR)"
  while true; do
    sync_once
    sleep 30  # Check every 30 seconds
  done
fi
