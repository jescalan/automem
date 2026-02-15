"""LLM-based reranking for recall results.

After initial retrieval (vector + graph + BM25), an LLM scores each
result's actual relevance to the query. This catches false positives
from vector search and promotes results that are genuinely relevant.

Uses a cheap, fast model (gpt-4.1-nano by default) to keep latency low.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

RERANK_ENABLED = os.environ.get("RERANK_ENABLED", "true").lower() in ("1", "true", "yes")
RERANK_MODEL = os.environ.get("RERANK_MODEL", "gpt-4.1-nano")
RERANK_TOP_N = int(os.environ.get("RERANK_TOP_N", "20"))  # How many candidates to rerank
RERANK_TIMEOUT = float(os.environ.get("RERANK_TIMEOUT", "5.0"))  # Seconds

SYSTEM_PROMPT = """You are a memory relevance scorer. Given a search query and a list of memory snippets, score each snippet's relevance to the query on a scale of 0-10.

Scoring guide:
- 10: Directly answers the query or is exactly what was asked for
- 7-9: Highly relevant, contains key information related to the query
- 4-6: Somewhat relevant, tangentially related
- 1-3: Barely relevant, only loosely connected
- 0: Completely irrelevant

Return a JSON array of objects with "index" (0-based) and "score" (0-10) for each snippet.
Example: [{"index": 0, "score": 8}, {"index": 1, "score": 3}]

Be strict — only high scores for genuinely relevant results."""


def rerank(
    query: str,
    results: List[Dict[str, Any]],
    openai_client: Any,
    top_n: int = RERANK_TOP_N,
    model: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Rerank results using an LLM. Returns reordered list with rerank_score added.

    Args:
        query: The original search query
        results: List of recall results (must have 'memory' dict with 'content')
        openai_client: OpenAI client instance
        top_n: Max number of candidates to send to the LLM
        model: Override model name

    Returns:
        Reordered results list with 'rerank_score' added to each result
    """
    if not RERANK_ENABLED or not results or not query or openai_client is None:
        return results

    model = model or RERANK_MODEL
    candidates = results[:top_n]
    remainder = results[top_n:]

    # Build the prompt with numbered snippets
    snippets = []
    for i, r in enumerate(candidates):
        mem = r.get("memory") or r
        content = mem.get("content", "")
        # Truncate long content to keep prompt manageable
        if len(content) > 300:
            content = content[:300] + "..."
        snippets.append(f"[{i}] {content}")

    user_prompt = f"Query: {query}\n\nSnippets:\n" + "\n".join(snippets)

    try:
        t0 = time.monotonic()

        extra_params: Dict[str, Any] = {}
        if model.startswith(("o", "gpt-5")):
            extra_params["max_completion_tokens"] = 500
        else:
            extra_params["max_tokens"] = 500
            extra_params["temperature"] = 0.0

        response = openai_client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            response_format={"type": "json_object"},
            timeout=RERANK_TIMEOUT,
            **extra_params,
        )

        elapsed_ms = (time.monotonic() - t0) * 1000
        raw = response.choices[0].message.content

        # Parse scores — handle both {"results": [...]} and bare [...]
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            scores_list = parsed.get("results") or parsed.get("scores") or parsed.get("rankings") or []
            if not scores_list:
                # Try to find any list value
                for v in parsed.values():
                    if isinstance(v, list):
                        scores_list = v
                        break
        elif isinstance(parsed, list):
            scores_list = parsed
        else:
            scores_list = []

        # Build index -> score map
        score_map: Dict[int, float] = {}
        for item in scores_list:
            if isinstance(item, dict) and "index" in item and "score" in item:
                idx = int(item["index"])
                score = float(item["score"])
                if 0 <= idx < len(candidates):
                    score_map[idx] = score

        # Apply scores and sort
        for i, r in enumerate(candidates):
            rerank_score = score_map.get(i, 5.0)  # Default to middle if not scored
            r["rerank_score"] = rerank_score
            r.setdefault("score_components", {})
            r["score_components"]["rerank"] = rerank_score

        # Sort by rerank score descending
        candidates.sort(key=lambda r: -r.get("rerank_score", 0))

        # Remainder gets a default score
        for r in remainder:
            r["rerank_score"] = 0.0
            r.setdefault("score_components", {})
            r["score_components"]["rerank"] = 0.0

        logger.info(
            "LLM rerank: model=%s candidates=%d scored=%d time=%.0fms",
            model,
            len(candidates),
            len(score_map),
            elapsed_ms,
        )

        return candidates + remainder

    except Exception:
        logger.warning("LLM rerank failed, returning original order", exc_info=True)
        return results
