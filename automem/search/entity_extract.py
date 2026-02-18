"""LLM-based entity extraction for memory enrichment.

Replaces spaCy NER with a fast LLM call that properly categorizes and
normalizes entities. Uses the same API credentials as reranking/query expansion.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)

# Config — reuses rerank credentials (fast model endpoint)
ENTITY_EXTRACT_ENABLED = os.environ.get("ENTITY_EXTRACT_ENABLED", "true").lower() in (
    "true",
    "1",
    "yes",
)
ENTITY_EXTRACT_MODEL = os.environ.get(
    "ENTITY_EXTRACT_MODEL",
    os.environ.get("RERANK_MODEL", "claude-haiku-4-20250514"),
)
ENTITY_EXTRACT_TIMEOUT = float(os.environ.get("ENTITY_EXTRACT_TIMEOUT", "30.0"))

# API credentials — falls back to RERANK_*, then OPENAI_*
ENTITY_API_KEY = os.environ.get(
    "ENTITY_EXTRACT_API_KEY",
    os.environ.get("RERANK_API_KEY", os.environ.get("OPENAI_API_KEY", "")),
)
ENTITY_BASE_URL = os.environ.get(
    "ENTITY_EXTRACT_BASE_URL",
    os.environ.get("RERANK_BASE_URL", os.environ.get("OPENAI_BASE_URL", "")),
)

_client: Any = None
_client_type: str = ""  # "anthropic" or "openai"

SYSTEM_PROMPT = """Extract named entities from this memory text. Return a JSON object with these categories:

- **people**: Real human names (normalize to full name if recognizable, e.g. "Ignacio" → "Ignacio Rueda" if context suggests it)
- **tools**: Software tools, libraries, frameworks, APIs, services (e.g. Slack, ClickHouse, AutoMem, BM25)
- **organizations**: Companies, teams, departments (e.g. Clerk, Vercel)
- **projects**: Specific project names, codenames, repos (e.g. Firetiger, memory.jeffs.bot)
- **concepts**: Locations, methodologies, abstract concepts only if they're a key topic
- **uncertain**: Entities you're not confident about — first-name-only people where you don't know the full name, or things that could be either a tool or project, etc. Format as objects: {"name": "...", "category": "people|tools|...", "question": "short question about what's unclear"}

Rules:
1. Only extract entities that are actually mentioned — never invent entities
2. Normalize names: merge variants (e.g. "Rob" and "Rob Bruno" → "Rob Bruno")
3. Skip generic words that aren't real entities (e.g. "meeting", "update", "pipeline", "system")
4. Skip pronouns, roles ("the team", "the user"), and common nouns
5. Each entity should appear in exactly ONE category (most specific wins)
6. If unsure whether something is an entity, skip it
7. Return empty arrays for categories with no entities
8. If you see a first name only and aren't sure of the full name, still include it in "people" AND add to "uncertain"

Return ONLY valid JSON, no markdown fencing:
{"people": [], "tools": [], "organizations": [], "projects": [], "concepts": [], "uncertain": []}"""


def _get_client() -> Any:
    """Get or create the LLM client for entity extraction."""
    global _client, _client_type

    if _client is not None:
        return _client

    if not ENTITY_API_KEY:
        logger.warning("No API key for entity extraction (checked ENTITY_EXTRACT_API_KEY, RERANK_API_KEY, OPENAI_API_KEY)")
        return None

    base = (ENTITY_BASE_URL or "").rstrip("/")
    # If base URL ends with /v1, it's an OpenAI-compatible proxy (e.g. OpenClaw)
    is_openai_compat = base.endswith("/v1")
    is_anthropic = (
        not is_openai_compat
        and ("anthropic" in base.lower() if base else "claude" in ENTITY_EXTRACT_MODEL.lower())
    )

    try:
        if is_anthropic:
            from anthropic import Anthropic
            _client = Anthropic(api_key=ENTITY_API_KEY, base_url=base or None)
            _client_type = "anthropic"
        else:
            from openai import OpenAI
            kwargs: dict = {"api_key": ENTITY_API_KEY}
            if base:
                kwargs["base_url"] = base
            _client = OpenAI(**kwargs)
            _client_type = "openai"

        logger.info("Entity extraction client initialized (%s, model=%s)", _client_type, ENTITY_EXTRACT_MODEL)
        return _client
    except Exception:
        logger.exception("Failed to initialize entity extraction client")
        return None


def extract_entities_llm(content: str) -> Optional[Dict[str, List[str]]]:
    """Extract entities from memory content using an LLM.

    Returns dict with keys: people, tools, organizations, projects, concepts.
    Returns None on failure (caller should fall back to empty).
    """
    if not ENTITY_EXTRACT_ENABLED:
        return None

    if not content or len(content.strip()) < 20:
        return None

    client = _get_client()
    if client is None:
        return None

    # Truncate very long content
    text = content[:2000] if len(content) > 2000 else content

    start = time.perf_counter()
    try:
        if _client_type == "anthropic":
            response = client.messages.create(
                model=ENTITY_EXTRACT_MODEL,
                max_tokens=300,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": text}],
                timeout=ENTITY_EXTRACT_TIMEOUT,
            )
            raw = response.content[0].text.strip()
        else:
            response = client.chat.completions.create(
                model=ENTITY_EXTRACT_MODEL,
                max_tokens=300,
                temperature=0,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": text + "\n\nRespond with ONLY a JSON object, no other text."},
                ],
                timeout=ENTITY_EXTRACT_TIMEOUT,
            )
            raw = response.choices[0].message.content.strip()

        elapsed_ms = int((time.perf_counter() - start) * 1000)

        # Strip markdown fencing if present
        if raw.startswith("```"):
            raw = re.sub(r"^```\w*\n?", "", raw)
            raw = re.sub(r"\n?```$", "", raw)

        result = json.loads(raw)

        # Validate structure
        valid_keys = {"people", "tools", "organizations", "projects", "concepts"}
        cleaned: Dict[str, List[str]] = {}
        for key in valid_keys:
            values = result.get(key, [])
            if isinstance(values, list):
                cleaned[key] = [str(v).strip() for v in values if v and str(v).strip()]
            else:
                cleaned[key] = []

        # Process uncertain entities → queue for clarification
        uncertain = result.get("uncertain", [])
        if uncertain and isinstance(uncertain, list):
            try:
                from automem.search.entity_clarify import queue_clarification
                for item in uncertain:
                    if isinstance(item, dict):
                        queue_clarification(
                            entity_name=str(item.get("name", "")),
                            category=str(item.get("category", "people")),
                            context=text[:200],
                            question_type="identity",
                        )
                    elif isinstance(item, str):
                        queue_clarification(
                            entity_name=item,
                            category="people",
                            context=text[:200],
                            question_type="identity",
                        )
            except Exception:
                logger.debug("Entity clarification queueing failed", exc_info=True)

        total_entities = sum(len(v) for v in cleaned.values())
        logger.info(
            "LLM entity extraction: %d entities, %d uncertain in %dms (model=%s)",
            total_entities,
            len(uncertain) if uncertain else 0,
            elapsed_ms,
            ENTITY_EXTRACT_MODEL,
        )
        return cleaned

    except json.JSONDecodeError:
        logger.warning("LLM entity extraction returned invalid JSON: %s", raw[:200])
        return None
    except Exception:
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        logger.exception("LLM entity extraction failed (%dms)", elapsed_ms)
        return None
