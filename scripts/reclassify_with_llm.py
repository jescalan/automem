#!/usr/bin/env python3
"""Reclassify 'Memory' fallback types using LLM classification.

This script finds all memories with type='Memory' (the fallback) and reclassifies
them using the configured CLASSIFICATION_MODEL for more accurate type assignment.

Environment:
    CLASSIFICATION_MODEL: LLM model for classification (default: gpt-4o-mini)
"""

import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict

from dotenv import load_dotenv
from falkordb import FalkorDB
import subprocess
from qdrant_client import QdrantClient

# Load environment
load_dotenv()
load_dotenv(Path.home() / ".config" / "automem" / ".env")

FALKORDB_HOST = os.getenv("FALKORDB_HOST", "localhost")
FALKORDB_PORT = int(os.getenv("FALKORDB_PORT", "6379"))
FALKORDB_PASSWORD = os.getenv("FALKORDB_PASSWORD")
QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "memories")
# LLM calls now routed through Clawdbot gateway (OAuth)
CLASSIFICATION_MODEL = os.getenv("CLASSIFICATION_MODEL", "gpt-4o-mini")

# Valid memory types
VALID_TYPES = {"Decision", "Pattern", "Preference", "Style", "Habit", "Insight", "Context"}

SYSTEM_PROMPT = """You are a memory classification system. Classify each memory into exactly ONE of these types:

- **Decision**: Choices made, selected options, what was decided
- **Pattern**: Recurring behaviors, typical approaches, consistent tendencies
- **Preference**: Likes/dislikes, favorites, personal tastes
- **Style**: Communication approach, formatting, tone used
- **Habit**: Regular routines, repeated actions, schedules
- **Insight**: Discoveries, learnings, realizations, key findings
- **Context**: Situational background, what was happening, circumstances

Return JSON with: {"type": "<type>", "confidence": <0.0-1.0>}"""


def get_fallback_memories(client) -> list[Dict[str, Any]]:
    """Fetch all memories with type='Memory' (fallback)."""
    print("📥 Fetching memories with fallback type='Memory'...")
    g = client.select_graph("memories")

    result = g.query(
        """
        MATCH (m:Memory)
        WHERE m.type = 'Memory'
        RETURN m.id as id, m.content as content, m.confidence as confidence
    """
    )

    memories = []
    for row in result.result_set:
        memories.append(
            {
                "id": row[0],
                "content": row[1],
                "old_confidence": row[2],
            }
        )

    print(f"✅ Found {len(memories)} memories with fallback type\n")
    return memories


def classify_with_llm(content: str) -> tuple[str, float]:
    """Use Clawdbot to classify memory type."""
    try:
        prompt = f"{SYSTEM_PROMPT}\n\nClassify this memory:\n{content[:1000]}"
        
        result = subprocess.run(
            [
                "clawdbot", "agent",
                "--session-id", f"memory-classify-{os.getpid()}",
                "--message", prompt,
                "--json",
                "--timeout", "60"
            ],
            capture_output=True,
            text=True,
            timeout=90
        )
        
        if result.returncode != 0:
            print(f"   ⚠️  Classification failed: {result.stderr[:100]}")
            return "Context", 0.5
        
        # Parse the response to extract JSON
        output = result.stdout.strip()
        
        # Try to parse as JSON first
        try:
            response = json.loads(output)
            if isinstance(response, dict):
                # Navigate to find the actual content
                text = response.get("response") or response.get("content") or str(response)
            else:
                text = str(response)
        except json.JSONDecodeError:
            text = output
        
        # Extract JSON from text (might be wrapped in markdown)
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0].strip()
        elif "```" in text:
            text = text.split("```")[1].split("```")[0].strip()
        
        # Find JSON object in text
        json_start = text.find("{")
        if json_start >= 0:
            json_end = text.rfind("}") + 1
            text = text[json_start:json_end]
        
        classification = json.loads(text)
        memory_type = classification.get("type", "Context")
        confidence = float(classification.get("confidence", 0.7))

        # Validate type
        if memory_type not in VALID_TYPES:
            memory_type = "Context"
            confidence = 0.6

        return memory_type, confidence

    except Exception as e:
        print(f"   ⚠️  Classification failed: {e}")
        return "Context", 0.5


def update_memory_type(
    falkor_client, qdrant_client, memory_id: str, new_type: str, new_confidence: float
) -> bool:
    """Update memory type in both FalkorDB and Qdrant."""
    try:
        # Update FalkorDB
        g = falkor_client.select_graph("memories")
        g.query(
            """
            MATCH (m:Memory {id: $id})
            SET m.type = $type, m.confidence = $confidence
            """,
            {"id": memory_id, "type": new_type, "confidence": new_confidence},
        )

        # Update Qdrant
        if qdrant_client:
            try:
                qdrant_client.set_payload(
                    collection_name=QDRANT_COLLECTION,
                    points=[memory_id],
                    payload={"type": new_type, "confidence": new_confidence},
                )
            except Exception as e:
                print(f"   ⚠️  Qdrant update failed: {e}")

        return True
    except Exception as e:
        print(f"   ❌ Update failed: {e}")
        return False


def main():
    """Main reclassification process."""
    print("=" * 70)
    print("🤖 AutoMem LLM Reclassification Tool")
    print("=" * 70)
    print()

    # LLM calls routed through Clawdbot gateway (OAuth)

    # Connect to FalkorDB
    print(f"🔌 Connecting to FalkorDB at {FALKORDB_HOST}:{FALKORDB_PORT}")
    try:
        falkor_client = FalkorDB(
            host=FALKORDB_HOST,
            port=FALKORDB_PORT,
            password=FALKORDB_PASSWORD,
            username="default" if FALKORDB_PASSWORD else None,
        )
        print("✅ Connected to FalkorDB\n")
    except Exception as e:
        print(f"❌ Failed to connect to FalkorDB: {e}")
        sys.exit(1)

    # Connect to Qdrant (optional)
    qdrant_client = None
    if QDRANT_URL:
        print(f"🔌 Connecting to Qdrant at {QDRANT_URL}")
        try:
            qdrant_client = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)
            print("✅ Connected to Qdrant\n")
        except Exception as e:
            print(f"⚠️  Qdrant connection failed: {e}")
            print("   (Will update FalkorDB only)\n")

    # LLM calls via Clawdbot
    print("🤖 Using Clawdbot for LLM classification (OAuth routing)")
    print("✅ Ready\n")

    # Get fallback memories
    memories = get_fallback_memories(falkor_client)

    if not memories:
        print("✅ No memories need reclassification!")
        return

    # Estimate cost
    tokens_per_memory = 370  # ~350 input + 20 output
    total_tokens = len(memories) * tokens_per_memory
    estimated_cost = (total_tokens / 1_000_000) * 0.20  # Combined input/output

    print(f"💰 Estimated cost: ${estimated_cost:.4f} (~{estimated_cost * 100:.1f} cents)")
    print(f"📊 Tokens: ~{total_tokens:,}")
    print()

    # Confirm
    response = input(f"🔄 Reclassify {len(memories)} memories with LLM? [y/N]: ")
    if response.lower() != "y":
        print("❌ Reclassification cancelled")
        sys.exit(0)

    print()
    print("🔄 Starting reclassification...")
    print()

    success_count = 0
    failed_count = 0
    type_counts = {}

    for i, memory in enumerate(memories, 1):
        memory_id = memory["id"]
        content = memory["content"] or ""

        content_preview = content[:60] + "..." if len(content) > 60 else content
        print(f"[{i}/{len(memories)}] {content_preview}")

        # Classify with LLM
        new_type, new_confidence = classify_with_llm(content)
        type_counts[new_type] = type_counts.get(new_type, 0) + 1

        print(f"   → {new_type} (confidence: {new_confidence:.2f})")

        if update_memory_type(falkor_client, qdrant_client, memory_id, new_type, new_confidence):
            success_count += 1
            print(f"   ✅ Updated")
        else:
            failed_count += 1

        # Progress update every 10
        if i % 10 == 0:
            print(f"\n💤 Progress: {success_count} ✅ / {failed_count} ❌\n")
            time.sleep(0.5)  # Rate limiting

    print()
    print("=" * 70)
    print(f"✅ Reclassification complete!")
    print(f"   Success: {success_count}")
    print(f"   Failed: {failed_count}")
    print()
    print("📊 Type Distribution:")
    for mem_type, count in sorted(type_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"   {mem_type}: {count}")
    print("=" * 70)


if __name__ == "__main__":
    main()
