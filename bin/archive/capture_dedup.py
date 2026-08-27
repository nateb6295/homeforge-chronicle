#!/usr/bin/env python3
"""Capture Deduplication — Skip redundant captures before processing.

Adapted from MemPalace's check_duplicate pattern (mcp_server.py).
Uses embedding similarity against recent captures to detect duplicates.

The intern processes hundreds of captures daily. Many are near-duplicates:
same story from different feeds, follow-up tweets on same topic,
re-shared content. Processing duplicates wastes LLM calls.

Usage:
    from capture_dedup import is_duplicate, find_duplicates

    # Check before processing
    if is_duplicate(new_capture_text, threshold=0.85):
        skip()  # already processed something very similar

    # Find what it's similar to
    dupes = find_duplicates(text, threshold=0.80, limit=3)
    # → [{"id": 123, "similarity": 0.91, "preview": "..."}]

Build #78. Stolen from MemPalace, adapted for Chronicle.
"""

import os
import re
import sqlite3
import struct
import math
from typing import List, Dict, Optional

import requests

DB_PATH = os.environ.get(
    "CHRONICLE_DB",
    "/mnt/hdd/chronicle-data/processed.db"
)
OLLAMA_URL = os.environ.get("EMBED_URL", "http://localhost:11434")
EMBED_MODEL = "snowflake-arctic-embed2"

# How many recent captures to check against
LOOKBACK_WINDOW = 200


def _get_embedding(text: str, query_mode: bool = False) -> Optional[List[float]]:
    """Get embedding from nomic-embed-text on Jetson."""
    prefix = "search_query: " if query_mode else "search_document: "
    try:
        resp = requests.post(
            f"{OLLAMA_URL}/api/embeddings",
            json={"model": EMBED_MODEL, "prompt": prefix + text[:500]},
            timeout=10,
        )
        if resp.status_code == 200:
            emb = resp.json().get("embedding")
            if emb:
                return emb
    except Exception:
        pass
    return None


def _cosine_similarity(a: List[float], b: List[float]) -> float:
    """Compute cosine similarity between two vectors."""
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def _decode_embedding(blob: bytes) -> List[float]:
    """Decode a stored embedding blob (float32 array)."""
    n = len(blob) // 4
    return list(struct.unpack(f'{n}f', blob))


def find_duplicates(
    text: str,
    threshold: float = 0.85,
    limit: int = 3,
    lookback: int = LOOKBACK_WINDOW,
) -> List[Dict]:
    """
    Find captures similar to the given text.

    Returns list of matches above threshold, sorted by similarity.
    Uses capsule_embeddings table for pre-computed embeddings,
    falls back to activity_feed text similarity if embeddings unavailable.
    """
    # Get embedding for the new text
    new_emb = _get_embedding(text)
    if not new_emb:
        return _text_fallback(text, threshold, limit)

    db = sqlite3.connect(DB_PATH)

    # Check against recent capsule embeddings
    rows = db.execute("""
        SELECT ce.capsule_id, ce.embedding, substr(kc.restatement, 1, 150) as preview
        FROM capsule_embeddings ce
        JOIN knowledge_capsules kc ON ce.capsule_id = kc.id
        WHERE kc.consolidated_into IS NULL AND kc.metabolized_at IS NULL
        ORDER BY ce.capsule_id DESC
        LIMIT ?
    """, (lookback,)).fetchall()

    matches = []
    for capsule_id, emb_blob, preview in rows:
        if not emb_blob:
            continue
        try:
            stored_emb = _decode_embedding(emb_blob)
            sim = _cosine_similarity(new_emb, stored_emb)
            if sim >= threshold:
                matches.append({
                    "id": capsule_id,
                    "similarity": round(sim, 4),
                    "preview": preview,
                })
        except Exception:
            continue

    db.close()
    matches.sort(key=lambda x: -x["similarity"])
    return matches[:limit]


def _text_fallback(text: str, threshold: float, limit: int) -> List[Dict]:
    """
    Fallback: use simple text overlap when embeddings unavailable.
    Extracts key n-grams and checks against recent captures.
    """
    # Extract key phrases (3-grams of content words)
    words = [w.lower() for w in re.findall(r'[a-zA-Z]{3,}', text)]
    words = [w for w in words if len(w) > 3]
    if len(words) < 3:
        return []

    new_set = set(words)

    db = sqlite3.connect(DB_PATH)
    rows = db.execute(
        "SELECT id, substr(content, 1, 300) FROM activity_feed "
        "WHERE activity_type='brief' ORDER BY created_at DESC LIMIT ?",
        (LOOKBACK_WINDOW,)
    ).fetchall()
    db.close()

    matches = []
    for row_id, content in rows:
        other_words = set(w.lower() for w in re.findall(r'[a-zA-Z]{4,}', content))
        if not other_words:
            continue
        overlap = len(new_set & other_words) / max(len(new_set | other_words), 1)
        if overlap >= threshold * 0.7:  # text overlap threshold is lower than embedding
            matches.append({
                "id": row_id,
                "similarity": round(overlap, 4),
                "preview": content[:100],
                "method": "text_overlap",
            })

    matches.sort(key=lambda x: -x["similarity"])
    return matches[:limit]


def is_duplicate(text: str, threshold: float = 0.85) -> bool:
    """Simple check: is this text a duplicate of a recent capture?"""
    dupes = find_duplicates(text, threshold=threshold, limit=1)
    return len(dupes) > 0


def dedup_stats() -> Dict:
    """Stats on potential duplicates in recent captures."""
    db = sqlite3.connect(DB_PATH)
    rows = db.execute(
        "SELECT id, substr(content, 1, 300) FROM activity_feed "
        "WHERE activity_type='brief' AND created_at > strftime('%s','now','-24 hours') "
        "ORDER BY created_at DESC"
    ).fetchall()
    db.close()

    if not rows:
        return {"total": 0, "duplicates": 0, "unique": 0}

    # Check each brief against the ones before it
    duplicate_count = 0
    seen_texts = []
    for row_id, content in rows:
        words = set(w.lower() for w in re.findall(r'[a-zA-Z]{4,}', content))
        is_dupe = False
        for prev_words in seen_texts:
            overlap = len(words & prev_words) / max(len(words | prev_words), 1)
            if overlap > 0.5:
                is_dupe = True
                break
        if is_dupe:
            duplicate_count += 1
        seen_texts.append(words)

    return {
        "total": len(rows),
        "duplicates": duplicate_count,
        "unique": len(rows) - duplicate_count,
        "dedup_rate": round(duplicate_count / len(rows) * 100, 1) if rows else 0,
    }


# ═══════════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage:")
        print("  capture_dedup.py 'text to check'")
        print("  capture_dedup.py --stats              # dedup stats for last 24h")
        sys.exit(0)

    if sys.argv[1] == "--stats":
        stats = dedup_stats()
        print(f"\nCapture dedup stats (last 24h):")
        print(f"  Total briefs:  {stats['total']}")
        print(f"  Duplicates:    {stats['duplicates']} ({stats['dedup_rate']}%)")
        print(f"  Unique:        {stats['unique']}")
    else:
        text = " ".join(sys.argv[1:])
        dupes = find_duplicates(text, threshold=0.80)
        if dupes:
            print(f"DUPLICATE — {len(dupes)} similar captures found:")
            for d in dupes:
                print(f"  [{d['similarity']:.3f}] #{d['id']}: {d['preview'][:80]}...")
        else:
            print("UNIQUE — no similar captures in recent history.")
