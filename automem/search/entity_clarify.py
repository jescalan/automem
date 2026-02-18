"""Entity clarification queue.

When entity extraction encounters ambiguous entities (first-name-only people,
uncertain categorization, possible duplicates), this module queues them for
human review. Questions are batched and sent periodically to avoid spam.

The queue is stored as a simple JSON file that persists across restarts.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)

# Where to persist the pending queue
QUEUE_FILE = Path(os.environ.get(
    "ENTITY_CLARIFY_QUEUE",
    os.path.join(os.environ.get("AUTOMEM_DATA_DIR", "/app"), "entity_clarify_queue.json"),
))

# Minimum time between notification batches (seconds)
BATCH_INTERVAL = int(os.environ.get("ENTITY_CLARIFY_INTERVAL", "3600"))  # 1 hour

# Webhook URL to notify (Telegram via OpenClaw, or direct)
NOTIFY_WEBHOOK = os.environ.get("ENTITY_CLARIFY_WEBHOOK", "")

_lock = threading.Lock()
_last_notify: float = 0.0


def _load_queue() -> Dict[str, Any]:
    """Load the pending queue from disk."""
    try:
        if QUEUE_FILE.exists():
            return json.loads(QUEUE_FILE.read_text())
    except Exception:
        logger.exception("Failed to load entity clarify queue")
    return {"pending": [], "resolved": {}}


def _save_queue(data: Dict[str, Any]) -> None:
    """Save the queue to disk."""
    try:
        QUEUE_FILE.parent.mkdir(parents=True, exist_ok=True)
        QUEUE_FILE.write_text(json.dumps(data, indent=2))
    except Exception:
        logger.exception("Failed to save entity clarify queue")


def queue_clarification(
    entity_name: str,
    category: str,
    context: str,
    question_type: str = "identity",
    memory_id: Optional[str] = None,
) -> bool:
    """Add an entity to the clarification queue.

    Args:
        entity_name: The ambiguous entity name
        category: The extracted category (people, tools, etc.)
        context: The memory content where this entity appeared
        question_type: "identity" (who is this?), "category" (tool vs project?),
                       "duplicate" (is X the same as Y?)
        memory_id: Optional memory ID for reference

    Returns:
        True if queued (new), False if already pending or resolved
    """
    with _lock:
        data = _load_queue()

        # Check if already resolved
        key = f"{category}:{entity_name.lower()}"
        if key in data.get("resolved", {}):
            return False

        # Check if already pending
        for item in data.get("pending", []):
            if item.get("key") == key:
                return False

        data.setdefault("pending", []).append({
            "key": key,
            "entity_name": entity_name,
            "category": category,
            "context": context[:200],
            "question_type": question_type,
            "memory_id": memory_id,
            "queued_at": time.time(),
        })

        _save_queue(data)
        logger.info("Queued entity clarification: %s (%s) - %s", entity_name, category, question_type)
        return True


def resolve_entity(
    entity_name: str,
    category: str,
    canonical_name: str,
    canonical_category: Optional[str] = None,
) -> None:
    """Mark an entity as resolved (user provided the answer)."""
    with _lock:
        data = _load_queue()
        key = f"{category}:{entity_name.lower()}"

        data.setdefault("resolved", {})[key] = {
            "canonical_name": canonical_name,
            "canonical_category": canonical_category or category,
            "resolved_at": time.time(),
        }

        # Remove from pending
        data["pending"] = [p for p in data.get("pending", []) if p.get("key") != key]
        _save_queue(data)


def get_pending() -> List[Dict[str, Any]]:
    """Get all pending clarifications."""
    with _lock:
        data = _load_queue()
        return data.get("pending", [])


def get_resolved() -> Dict[str, Any]:
    """Get all resolved entities (for lookup during extraction)."""
    with _lock:
        data = _load_queue()
        return data.get("resolved", {})


def format_questions_batch(items: Optional[List[Dict]] = None) -> Optional[str]:
    """Format pending clarifications as a Telegram-friendly message.

    Returns None if no pending items.
    """
    items = items or get_pending()
    if not items:
        return None

    lines = ["🏷️ **Entity clarifications needed:**\n"]
    for i, item in enumerate(items[:10], 1):  # Max 10 at a time
        name = item["entity_name"]
        cat = item["category"]
        ctx = item["context"]
        qtype = item["question_type"]

        if qtype == "identity":
            lines.append(f"{i}. **{name}** (detected as {cat}) — who is this? Full name?")
        elif qtype == "category":
            lines.append(f"{i}. **{name}** — is this a tool, project, or organization?")
        elif qtype == "duplicate":
            lines.append(f"{i}. **{name}** — is this the same as someone/something already known?")

        lines.append(f"   _Context: \"{ctx[:100]}...\"_\n")

    lines.append("Reply with corrections and I'll update the alias map.")
    return "\n".join(lines)
