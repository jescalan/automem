#!/usr/bin/env python3
"""
Smart Transcript Sync — extracts structured memories from conversation segments
instead of storing raw messages.

Usage: smart-transcript-sync.py [--once] [--backfill] [--dry-run]
"""

import json
import os
import sys
import time
import argparse
import requests
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

# Configuration
AUTOMEM_ENDPOINT = os.environ.get("AUTOMEM_ENDPOINT", "http://localhost:8001")
AUTOMEM_API_TOKEN = os.environ.get("AUTOMEM_API_TOKEN", "olly-automem-2026")
# Clawdbot gateway config (for LLM calls via OAuth routing)
CLAWDBOT_GATEWAY_URL = os.environ.get("CLAWDBOT_GATEWAY_URL", "http://localhost:18789")
CLAWDBOT_GATEWAY_TOKEN = os.environ.get("CLAWDBOT_GATEWAY_TOKEN", "")
# Auto-discover all agent session directories
def discover_session_dirs():
    """Find all session directories under ~/.clawdbot/agents/*/sessions"""
    agents_root = Path(os.path.expanduser("~/.clawdbot/agents"))
    dirs = []
    if agents_root.exists():
        for agent_dir in sorted(agents_root.iterdir()):
            sessions_dir = agent_dir / "sessions"
            if sessions_dir.is_dir():
                dirs.append(sessions_dir)
    return dirs if dirs else [Path(os.path.expanduser("~/.clawdbot/agents/main/sessions"))]

SESSION_DIRS = os.environ.get("CLAWDBOT_SESSION_DIRS", "").split(":") if os.environ.get("CLAWDBOT_SESSION_DIRS") else discover_session_dirs()
SESSION_DIRS = [Path(d) for d in SESSION_DIRS if d]
# Legacy single-dir support
SESSION_DIR = SESSION_DIRS[0] if SESSION_DIRS else Path(os.path.expanduser("~/.clawdbot/agents/main/sessions"))
STATE_FILE = Path(os.path.expanduser("~/.clawdbot/smart-sync-state.json"))
CLARIFICATION_QUEUE_FILE = Path(os.path.expanduser("~/.clawdbot/memory-clarification-queue.json"))
TELEGRAM_CHAT_ID = "8236522440"  # Jeff's Telegram

# Segment configuration
SEGMENT_MIN_MESSAGES = 4      # Minimum messages to form a segment
SEGMENT_MAX_MESSAGES = 30     # Maximum messages per segment
SEGMENT_GAP_MINUTES = 30      # Gap that triggers new segment

# Note: LLM calls now routed through Clawdbot gateway (uses configured model via OAuth)

EXTRACTION_PROMPT = """You are a memory extraction system. Capture THE SUBSTANCE of what happened - the specific details that matter.

OUTPUT: Valid JSON only: {{"memories": [{{"type": str, "content": str, "importance": float, "tags": [str], "needs_clarification": bool, "clarification_prompt": str|null}}]}}

CLARIFICATION RULES — BE STRICT:
- Set needs_clarification=true if reading this in 6 months, you wouldn't know WHAT SPECIFICALLY it refers to
- If it says "the feature", "the project", "the thing", "this" without naming it → needs_clarification=true
- Include a clarification_prompt asking the specific question needed

MUST flag needs_clarification=true:
- "Launch the feature next Tuesday" → VAGUE! Which feature? → needs_clarification=true, clarification_prompt="Which feature is being launched?"
- "The deadline is next Friday" → VAGUE! Deadline for what? → needs_clarification=true
- "User prefers the second option" → VAGUE! Option for what? → needs_clarification=true
- "Test the thing next week" → VAGUE! Test what? → needs_clarification=true
- "We agreed to move forward with the plan" → VAGUE! What plan? → needs_clarification=true

OK to store (specific enough):
- "Launch the B2B onboarding redesign feature on Tuesday, January 28, 2026" → names the feature, has date
- "Jeff prefers Claude Sonnet over Opus for coding tasks due to speed" → specific preference with reasoning
- "Deadline for Acorn to Oak website CVE fix is Friday, January 31, 2026" → names project and date

When in doubt, flag for clarification. It's better to ask than to store garbage.

RELATIONSHIP INFERENCE — CRITICAL:
DO NOT infer relationships or collaborations from temporal proximity.

If person A and topic B are mentioned in the same conversation, that does NOT mean:
- Person A is working on topic B
- Person A collaborated on topic B
- Person A is involved with topic B

ONLY extract a relationship/collaboration if EXPLICITLY stated:
✅ "Cody and I are building the subscription tracker together" → collaboration stated
✅ "I'm working with Cody on the sales engineering transition" → explicit
❌ "Added Cody to calendar. Also, I want to build a subscription tracker." → NO relationship (just co-occurrence)
❌ "Talked to Cody. Separately, the subscription skill is coming along." → NO relationship

When in doubt, extract the entities SEPARATELY without linking them.

ECHO PREVENTION — CRITICAL:
DO NOT extract information that is being REFERENCED or RECALLED, only NEW information.

Skip extraction entirely for:
- Assistant telling user about something from memory ("I remember Anthony works at...")
- User asking about something already discussed ("What did we say about Roy?")
- Summaries or recaps of previous conversations
- References like "as we discussed", "like you mentioned", "the update about X"

ONLY extract when USER provides NEW ORIGINAL information:
✅ "Anthony texted me, he says the job is going well but looking for new opportunities"
❌ "User received an update about Anthony" (this is just restating the above)
❌ "The assistant recalled information about Anthony's job" (echo of stored info)

The goal is to store SOURCES, not REFERENCES to sources.

TYPES:
- Decision: A specific choice that was made
- Preference: Something the user likes/dislikes/wants done a certain way  
- Insight: Something learned, realized, or discovered
- Context: Important facts about people, relationships, businesses, situations
- Pattern: Recurring behaviors or tendencies observed
- Work: Time spent on a project, investigation, research, debugging - even if not finished

THE KEY RULE: Be SPECIFIC. Capture what actually happened, not a vague category.

❌ BAD (too vague - could describe a thousand different sessions):
- "User is working on the morning briefing"
- "User discussed family matters"  
- "User has been debugging an issue"
- "We agreed to launch the feature next Tuesday" (WHAT feature? WHICH Tuesday?)
- "User decided to test this next week" (test WHAT? next week from WHEN?)

✅ GOOD (specific - I know what actually happened):
- "Worked on morning briefing: changed weather to conversational format, switched to 12h time, fixed newline before Notable section"
- "Mari (nanny) has been with the family for 2 years, is from Brazil, kids love her"
- "Spent time researching afterhour.com as a data source for stockbot - discussed reverse engineering their API"
- "Debugged FalkorDB/Qdrant sync issue - found 9000 orphaned vectors, cleaned them up"
- "Decision: Launch the B2B onboarding feature on Tuesday, January 28, 2026"
- "Jeff deferred stockbot paper trading testing to week of January 27, 2026"

TEMPORAL REFERENCES — CRITICAL:
Convert ALL relative times to absolute dates using the conversation timestamp.
- "next Tuesday" → "Tuesday, January 28, 2026"
- "next week" → "week of January 27, 2026"  
- "tomorrow" → "January 25, 2026"
- "end of month" → "end of January 2026"
If you can't determine the absolute date, include enough context that the date can be inferred.

Things DON'T need to be "finished" or "accomplished" to be worth remembering. Time spent working on something matters. Research and exploration matter. Conversations about people matter.

Ask: "If I read this in 6 months, would I know what ACTUALLY happened?"

IMPORTANCE: 0.3-1.0 (0.9+ critical, 0.7-0.8 significant, 0.5-0.6 useful)
TAGS: Include relevant keywords. For people: "person:<name>". For deadlines/dates: "temporal:date:YYYY-MM-DD"

SKIP if: conversation is routine/trivial, or you can't be specific enough to pass the "6 months" test: {{"memories": []}}

<conversation>
{conversation}
</conversation>"""


def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"files": {}, "last_run": None}


def save_state(state: dict):
    state["last_run"] = datetime.utcnow().isoformat()
    STATE_FILE.write_text(json.dumps(state, indent=2))


def load_clarification_queue() -> list:
    """Load pending clarification queue"""
    if CLARIFICATION_QUEUE_FILE.exists():
        try:
            return json.loads(CLARIFICATION_QUEUE_FILE.read_text())
        except json.JSONDecodeError:
            return []
    return []


def save_clarification_queue(queue: list):
    """Save pending clarification queue"""
    CLARIFICATION_QUEUE_FILE.write_text(json.dumps(queue, indent=2))


def queue_for_clarification(memory: dict, source_session: str = None, 
                            segment_timestamps: tuple = None) -> bool:
    """Queue a vague memory for clarification instead of storing it"""
    queue = load_clarification_queue()
    
    entry = {
        "id": f"pending-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}-{len(queue)}",
        "queued_at": datetime.utcnow().isoformat(),
        "memory": memory,
        "source_session": source_session,
        "segment_timestamps": segment_timestamps,
        "status": "pending"
    }
    
    queue.append(entry)
    save_clarification_queue(queue)
    
    # Send Telegram notification
    send_clarification_request(entry)
    
    return True


def send_clarification_request(entry: dict):
    """Spawn an isolated session to handle memory clarification via chat.
    
    Instead of sending a raw Telegram message, we spawn a sub-agent that can:
    - Review the memory with full context
    - Ask intelligent follow-up questions
    - Store the memory properly once clarified
    """
    memory = entry["memory"]
    content = memory.get("content", "")
    mem_type = memory.get("type", "Context")
    importance = memory.get("importance", 0.7)
    tags = memory.get("tags", [])
    prompt = memory.get("clarification_prompt", "Can you add more context?")
    entry_id = entry.get("id", "unknown")
    source_session = entry.get("source_session", "unknown")
    
    # Build the task for the sub-agent
    task = f"""MEMORY CLARIFICATION REVIEW

A memory was extracted from conversation but needs clarification before storage.

**Pending Memory:**
- ID: {entry_id}
- Type: {mem_type}
- Importance: {importance}
- Tags: {', '.join(tags) if tags else 'none'}
- Source Session: {source_session}

**Content:**
"{content}"

**Clarification Needed:**
{prompt}

**Your job:**
1. Message Jeff via Telegram (chatId 8236522440) asking for clarification
2. Format it nicely: include the content and the specific question
3. Wait for his response or let him know he can reply when ready
4. If the memory is clear enough after review, you can store it directly using:
   `~/clawd/skills/automem/scripts/automem.sh store "<content>" --type {mem_type} --importance {importance} --tags "{','.join(tags) if tags else 'extracted'}"` 

5. If you store it, also remove this entry from the clarification queue:
   `python3 -c "import json; p='~/.clawdbot/memory-clarification-queue.json'; q=json.loads(open(p.replace('~','/home/clawdbot')).read()); q=[e for e in q if e['id']!='{entry_id}']; open(p.replace('~','/home/clawdbot'),'w').write(json.dumps(q,indent=2))"`

Keep the message friendly and brief. Example:
"🧠 Quick clarification needed for a memory:

\"{content[:100]}...\"

❓ {prompt}

Reply when you have a moment, or say 'skip' to discard this one."
"""

    # Gateway config
    gateway_url = os.environ.get("CLAWDBOT_GATEWAY_URL", "http://localhost:18789")
    gateway_token = os.environ.get("CLAWDBOT_GATEWAY_TOKEN", "")
    
    headers = {'Content-Type': 'application/json'}
    if gateway_token:
        headers['Authorization'] = f'Bearer {gateway_token}'
    
    payload = {
        'tool': 'sessions_spawn',
        'args': {
            'task': task,
            'label': f'memory-clarify-{datetime.utcnow().strftime("%Y%m%d-%H%M%S")}',
            'cleanup': 'delete',
            'runTimeoutSeconds': 120
        }
    }
    
    try:
        resp = requests.post(
            f'{gateway_url}/tools/invoke',
            headers=headers,
            json=payload,
            timeout=30
        )
        
        if resp.status_code == 200:
            print(f"    [CLARIFY] Spawned review session for clarification")
        else:
            print(f"    [CLARIFY] Failed to spawn session: {resp.status_code} {resp.text[:100]}", file=sys.stderr)
            # Fallback to direct Telegram
            _send_telegram_fallback(content, prompt)
    except Exception as e:
        print(f"    [CLARIFY] Error spawning session: {e}", file=sys.stderr)
        # Fallback to direct Telegram
        _send_telegram_fallback(content, prompt)


def _send_telegram_fallback(content: str, prompt: str):
    """Fallback to direct Telegram if session spawn fails"""
    import subprocess
    message = f"🧠 **Memory needs context:**\n\n\"{content[:100]}\"\n\n❓ {prompt}"
    try:
        subprocess.run(
            ["clawdbot", "message", "send", "--channel", "telegram",
             "--to", TELEGRAM_CHAT_ID, "--message", message],
            capture_output=True, text=True, timeout=30
        )
    except Exception:
        pass


def check_similar_exists(content: str, threshold: float = 0.75) -> bool:
    """Check if a similar memory already exists in AutoMem.
    
    Uses vector similarity search to find duplicates before storing.
    Returns True if a similar memory exists (should skip storing).
    
    Key insight: If similar content exists from a BETTER source (direct input,
    granola, etc.), we should definitely skip. We're about to store an echo.
    """
    try:
        # Use recall endpoint with the content as query
        # Don't filter by tags - we want to find ALL similar memories
        resp = requests.get(
            f"{AUTOMEM_ENDPOINT}/recall",
            headers={"Authorization": f"Bearer {AUTOMEM_API_TOKEN}"},
            params={
                "q": content[:500],  # Truncate for query
                "limit": 5,
            },
            timeout=10
        )
        
        if resp.status_code != 200:
            return False  # On error, allow storage
        
        data = resp.json()
        results = data.get("results", [])
        
        for r in results:
            # Check vector similarity score
            score = r.get("original_score", 0)  # Raw vector similarity
            existing_content = r.get("memory", {}).get("content", "")[:60]
            existing_source = r.get("memory", {}).get("metadata", {}).get("source_session")
            existing_tags = r.get("memory", {}).get("tags", [])
            
            # Determine if existing memory is from a "better" source
            is_original_source = (
                existing_source is None or  # Direct input or granola
                "granola" in existing_tags or
                "direct" in existing_tags or
                "meeting" in existing_tags
            )
            
            # If similar content exists from original source, this is an echo - skip
            if score >= 0.65 and is_original_source:
                print(f"    [ECHO] Skipping echo of original (similarity {score:.2f}): {existing_content}...")
                return True
            
            # Standard dedup: very similar content from any source
            if score >= threshold:
                print(f"    [DEDUP] Skipping duplicate (similarity {score:.2f}): {existing_content}...")
                return True
        
        return False
    except Exception as e:
        # On any error, allow storage (don't block on dedup check failure)
        return False


def store_memory(content: str, mem_type: str, importance: float, tags: list, 
                 source_session: str = None, segment_timestamps: tuple = None,
                 dry_run: bool = False) -> bool:
    """Store a memory in AutoMem with deduplication check"""
    if dry_run:
        print(f"  [DRY RUN] Would store: {mem_type} ({importance:.2f}) - {content[:80]}...")
        return True
    
    # Check for similar existing memories before storing
    if check_similar_exists(content):
        return False  # Skip duplicate
    
    # Build metadata for source linking
    metadata = {}
    if source_session:
        metadata["source_session"] = source_session
        metadata["transcript_path"] = f"~/.clawdbot/agents/main/sessions/{source_session}.jsonl"
    if segment_timestamps and len(segment_timestamps) == 2:
        metadata["segment_start"] = segment_timestamps[0]
        metadata["segment_end"] = segment_timestamps[1]
    
    try:
        resp = requests.post(
            f"{AUTOMEM_ENDPOINT}/memory",
            headers={
                "Authorization": f"Bearer {AUTOMEM_API_TOKEN}",
                "Content-Type": "application/json"
            },
            json={
                "content": content,
                "type": mem_type,
                "importance": importance,
                "tags": tags + ["extracted", "conversation"],
                "metadata": metadata
            },
            timeout=15
        )
        if resp.status_code not in (200, 201):
            print(f"  Warning: AutoMem returned {resp.status_code}: {resp.text[:100]}", file=sys.stderr)
            return False
        return True
    except Exception as e:
        print(f"  Error storing memory: {e}", file=sys.stderr)
        return False


def extract_message(msg_data: dict) -> Optional[dict]:
    """Extract role, content, and timestamp from a message"""
    if msg_data.get("type") != "message":
        return None
    
    msg = msg_data.get("message", {})
    role = msg.get("role", "")
    
    if role not in ("user", "assistant"):
        return None
    
    content_parts = msg.get("content", [])
    text_parts = []
    
    for part in content_parts:
        if isinstance(part, str):
            text_parts.append(part)
        elif isinstance(part, dict):
            if part.get("type") == "text":
                text_parts.append(part.get("text", ""))
            # Skip thinking blocks, tool calls, images, etc.
    
    content = " ".join(text_parts).strip()
    if not content or len(content) < 10:
        return None
    
    # Skip heartbeat and routine messages
    content_upper = content.upper()
    if "HEARTBEAT_OK" in content_upper or "NO_REPLY" in content_upper:
        return None
    if content_upper.startswith("READ HEARTBEAT.MD"):
        return None
    
    timestamp = msg_data.get("timestamp")
    
    return {
        "role": role,
        "content": content,
        "timestamp": timestamp
    }


def segment_messages(messages: list[dict]) -> list[list[dict]]:
    """Split messages into conversation segments based on time gaps"""
    if not messages:
        return []
    
    segments = []
    current_segment = [messages[0]]
    
    for msg in messages[1:]:
        # Check time gap
        gap_detected = False
        if msg.get("timestamp") and current_segment[-1].get("timestamp"):
            try:
                curr_ts = datetime.fromisoformat(msg["timestamp"].replace("Z", "+00:00"))
                prev_ts = datetime.fromisoformat(current_segment[-1]["timestamp"].replace("Z", "+00:00"))
                gap = (curr_ts - prev_ts).total_seconds() / 60
                if gap > SEGMENT_GAP_MINUTES:
                    gap_detected = True
            except (ValueError, TypeError):
                pass
        
        # Check segment size
        if gap_detected or len(current_segment) >= SEGMENT_MAX_MESSAGES:
            if len(current_segment) >= SEGMENT_MIN_MESSAGES:
                segments.append(current_segment)
            current_segment = [msg]
        else:
            current_segment.append(msg)
    
    # Don't forget the last segment
    if len(current_segment) >= SEGMENT_MIN_MESSAGES:
        segments.append(current_segment)
    
    return segments


def format_segment_for_extraction(segment: list[dict]) -> str:
    """Format a segment for the LLM"""
    lines = []
    for msg in segment:
        role = msg["role"].upper()
        content = msg["content"]
        # Truncate very long messages
        if len(content) > 2000:
            content = content[:2000] + "..."
        lines.append(f"[{role}]: {content}")
    return "\n\n".join(lines)


def extract_memories(segment: list[dict]) -> list[dict]:
    """Extract structured memories from a segment using Clawdbot routing.
    
    Uses clawdbot agent CLI which routes through the gateway and uses OAuth.
    """
    import subprocess
    
    conversation_text = format_segment_for_extraction(segment)
    
    # Skip if conversation is too short
    if len(conversation_text) < 100:
        return []
    
    # Limit total conversation length to avoid API issues
    if len(conversation_text) > 15000:
        conversation_text = conversation_text[:15000] + "\n\n[...truncated...]"
    
    # Build the extraction prompt
    prompt = EXTRACTION_PROMPT.format(conversation=conversation_text)
    
    try:
        # Use clawdbot agent for LLM call (routes through gateway with OAuth)
        result = subprocess.run(
            [
                "clawdbot", "agent",
                "--session-id", f"memory-extraction-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
                "--message", prompt,
                "--json",
                "--timeout", "120"
            ],
            capture_output=True,
            text=True,
            timeout=150
        )
        
        if result.returncode != 0:
            print(f"  Warning: clawdbot agent failed: {result.stderr[:200]}", file=sys.stderr)
            return []
        
        # Parse the JSON response
        try:
            agent_response = json.loads(result.stdout)
        except json.JSONDecodeError:
            # Try to find JSON in the output
            if "```json" in result.stdout:
                json_part = result.stdout.split("```json")[1].split("```")[0].strip()
                agent_response = json.loads(json_part)
            else:
                print(f"  Warning: Could not parse agent response as JSON", file=sys.stderr)
                return []
        
        # Extract the actual response text from the agent response
        result_text = ""
        if isinstance(agent_response, dict):
            # The response might be nested in various ways
            if "response" in agent_response:
                result_text = agent_response["response"]
            elif "content" in agent_response:
                result_text = agent_response["content"]
            elif "result" in agent_response:
                result_text = str(agent_response["result"])
            elif "messages" in agent_response and agent_response["messages"]:
                last_msg = agent_response["messages"][-1]
                if isinstance(last_msg, dict) and "content" in last_msg:
                    result_text = last_msg["content"]
                elif isinstance(last_msg, str):
                    result_text = last_msg
            else:
                # Try to use the whole response
                result_text = json.dumps(agent_response)
        else:
            result_text = str(agent_response)
        
        if not result_text:
            return []
        
        result_text = result_text.strip()
        
        # Extract JSON from response (handle markdown code blocks)
        if "```json" in result_text:
            parts = result_text.split("```json")
            if len(parts) > 1:
                result_text = parts[1].split("```")[0].strip()
        elif "```" in result_text:
            parts = result_text.split("```")
            if len(parts) > 1:
                result_text = parts[1].strip()
        
        # Try to find JSON object/array in the text
        json_start = -1
        for i, c in enumerate(result_text):
            if c in '{[':
                json_start = i
                break
        
        if json_start >= 0:
            result_text = result_text[json_start:]
        
        parsed = json.loads(result_text)
        
        # Handle both {"memories": [...]} and direct [...] formats
        if isinstance(parsed, dict) and "memories" in parsed:
            memories = parsed["memories"]
        elif isinstance(parsed, list):
            memories = parsed
        else:
            memories = []
        
        # Validate structure
        valid_memories = []
        for mem in memories:
            if isinstance(mem, dict) and all(k in mem for k in ("type", "content", "importance", "tags")):
                # Clamp importance
                try:
                    mem["importance"] = max(0.3, min(1.0, float(mem["importance"])))
                except (ValueError, TypeError):
                    mem["importance"] = 0.5
                valid_memories.append(mem)
        
        return valid_memories
        
    except subprocess.TimeoutExpired:
        print(f"  Warning: clawdbot agent timed out", file=sys.stderr)
        return []
    except json.JSONDecodeError as e:
        print(f"  Warning: JSON parse error: {e}", file=sys.stderr)
        return []
    except Exception as e:
        print(f"  Warning: Extraction error ({type(e).__name__}): {e}", file=sys.stderr)
        return []


def process_file(jsonl_path: Path, state: dict, 
                 backfill: bool = False, dry_run: bool = False) -> tuple[int, int]:
    """Process a single JSONL transcript file
    
    Returns: (segments_processed, memories_stored)
    """
    filename = jsonl_path.name
    file_state = state["files"].get(filename, {"last_line": 0, "pending_messages": []})
    last_line = file_state.get("last_line", 0)
    pending = file_state.get("pending_messages", [])
    
    # Load new messages
    current_line = 0
    new_messages = []
    
    with open(jsonl_path, "r") as f:
        for line in f:
            current_line += 1
            
            if current_line <= last_line:
                continue
            
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                continue
            
            msg = extract_message(data)
            if msg:
                new_messages.append(msg)
    
    if not new_messages and not pending:
        return 0, 0
    
    # Combine pending + new messages
    all_messages = pending + new_messages
    
    # Segment the messages
    segments = segment_messages(all_messages)
    
    # The last segment might be incomplete - keep it pending
    if segments and len(all_messages) > 0:
        # Check if the last message is recent (within gap threshold)
        last_msg = all_messages[-1]
        if last_msg.get("timestamp"):
            try:
                last_ts = datetime.fromisoformat(last_msg["timestamp"].replace("Z", "+00:00"))
                now = datetime.utcnow().replace(tzinfo=last_ts.tzinfo)
                minutes_ago = (now - last_ts).total_seconds() / 60
                
                if minutes_ago < SEGMENT_GAP_MINUTES:
                    # Last segment is potentially incomplete - keep it pending
                    if segments:
                        pending = segments[-1]
                        segments = segments[:-1]
                    else:
                        pending = all_messages
            except (ValueError, TypeError):
                pending = []
        else:
            pending = []
    else:
        pending = []
    
    # Process complete segments
    segments_processed = 0
    memories_stored = 0
    
    # Get session ID from filename (strip .jsonl)
    session_id = filename.rsplit('.', 1)[0] if filename.endswith('.jsonl') else filename
    
    for segment in segments:
        print(f"  Processing segment with {len(segment)} messages...")
        
        # Get timestamp range for this segment
        segment_timestamps = None
        timestamps = [m.get("timestamp") for m in segment if m.get("timestamp")]
        if timestamps:
            segment_timestamps = (timestamps[0], timestamps[-1])
        
        memories = extract_memories(segment)
        
        if not memories:
            print(f"    No memories extracted")
            continue
        
        print(f"    Extracted {len(memories)} memories")
        segments_processed += 1
        
        for mem in memories:
            # Check if memory needs clarification before storing
            needs_clarification = mem.get("needs_clarification", False)
            
            if needs_clarification and not dry_run:
                # Queue for clarification instead of storing
                print(f"    [QUEUE] Needs clarification: {mem['content'][:60]}...")
                queue_for_clarification(
                    memory=mem,
                    source_session=session_id,
                    segment_timestamps=segment_timestamps
                )
                continue
            
            if store_memory(
                content=mem["content"],
                mem_type=mem["type"],
                importance=mem["importance"],
                tags=mem["tags"],
                source_session=session_id,
                segment_timestamps=segment_timestamps,
                dry_run=dry_run
            ):
                memories_stored += 1
        
        # Rate limit between segments
        if not dry_run:
            time.sleep(1)
    
    # Update state
    state["files"][filename] = {
        "last_line": current_line,
        "pending_messages": pending
    }
    
    return segments_processed, memories_stored


def sync_once(backfill: bool = False, dry_run: bool = False) -> tuple[int, int]:
    """Process all transcript files once"""
    state = load_state()
    total_segments = 0
    total_memories = 0
    
    # Process all configured session directories
    for session_dir in SESSION_DIRS:
        if not session_dir.exists():
            continue
        for jsonl_path in sorted(session_dir.glob("*.jsonl")):
            if ".lock" in jsonl_path.name:
                continue
            
            print(f"[{datetime.now().isoformat()}] Processing {jsonl_path.name}")
            segments, memories = process_file(jsonl_path, state, backfill, dry_run)
            
            if segments > 0 or memories > 0:
                print(f"  → {segments} segments, {memories} memories")
            
            total_segments += segments
            total_memories += memories
    
    if not dry_run:
        save_state(state)
    
    return total_segments, total_memories


def main():
    parser = argparse.ArgumentParser(description="Smart transcript sync with LLM extraction")
    parser.add_argument("--once", action="store_true", help="Process once and exit")
    parser.add_argument("--backfill", action="store_true", help="Process entire history")
    parser.add_argument("--dry-run", action="store_true", help="Extract but don't store")
    parser.add_argument("--interval", type=int, default=300, help="Sync interval in seconds (default: 300)")
    args = parser.parse_args()
    
    if args.once or args.dry_run:
        segments, memories = sync_once(args.backfill, args.dry_run)
        print(f"\nTotal: {segments} segments processed, {memories} memories stored")
    else:
        print(f"[{datetime.now().isoformat()}] Starting smart transcript sync")
        print(f"  Session dirs: {[str(d) for d in SESSION_DIRS]}")
        print(f"  AutoMem: {AUTOMEM_ENDPOINT}")
        print(f"  LLM: via Clawdbot gateway (OAuth routing)")
        print(f"  Interval: {args.interval}s")
        
        while True:
            try:
                sync_once(backfill=False, dry_run=False)
            except Exception as e:
                print(f"[{datetime.now().isoformat()}] Error: {e}", file=sys.stderr)
            
            time.sleep(args.interval)


if __name__ == "__main__":
    main()
