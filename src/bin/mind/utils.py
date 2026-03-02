"""Chronicle Mind — utility functions.

Timestamps, logging, embedding helpers, and other shared utilities
used across the Mind codebase.
"""

import time, os, math, json, requests
from datetime import datetime
from typing import Optional, List

from mind.config import OLLAMA_URL, LOG_FILE, CONSOLIDATE_EMBED_MODEL, TOKEN_PATH, FEED_WATERMARK


def safe_truncate(s: str, max_chars: int) -> str:
    if not s or len(s) <= max_chars:
        return s
    return s[:max_chars] + "..."


def now_ts() -> int:
    return int(time.time())


def now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def make_cycle_id() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _parse_ts(raw) -> int:
    if isinstance(raw, (int, float)):
        return int(raw)
    try:
        return int(raw)
    except (ValueError, TypeError):
        pass
    try:
        clean = str(raw).split(".")[0].split("+")[0].replace("T", " ")
        dt = datetime.strptime(clean, "%Y-%m-%d %H:%M:%S")
        return int(dt.timestamp())
    except Exception:
        return now_ts()


def get_token() -> Optional[str]:
    try:
        with open(TOKEN_PATH) as f:
            return f.read().strip()
    except Exception:
        return None


def get_feed_watermark() -> int:
    """Get the last-seen public feed capsule ID."""
    try:
        with open(FEED_WATERMARK) as f:
            return int(f.read().strip())
    except Exception:
        return 0


def set_feed_watermark(capsule_id: int):
    """Update the feed watermark to the given capsule ID."""
    try:
        with open(FEED_WATERMARK, "w") as f:
            f.write(str(capsule_id))
    except Exception:
        pass


def log(msg: str):
    line = f"[{now_iso()}] {msg}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════
#  Embedding / Similarity Utilities (for sleep consolidation)
# ═══════════════════════════════════════════════════════════════════

def get_embeddings(texts: List[str], model: str = CONSOLIDATE_EMBED_MODEL) -> Optional[List[List[float]]]:
    """Batch-embed texts via Ollama /api/embed. Returns list of vectors or None on failure."""
    if not texts:
        return []
    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/embed",
            json={"model": model, "input": texts},
            timeout=30,
        )
        if r.status_code == 200:
            data = r.json()
            embeddings = data.get("embeddings")
            if embeddings and len(embeddings) == len(texts):
                return embeddings
        # Fallback: embed one at a time
        results = []
        for text in texts:
            r2 = requests.post(
                f"{OLLAMA_URL}/api/embed",
                json={"model": model, "input": [text]},
                timeout=15,
            )
            if r2.status_code == 200:
                embs = r2.json().get("embeddings", [])
                if embs:
                    results.append(embs[0])
                else:
                    return None
            else:
                return None
        return results
    except Exception:
        return None


def cosine_sim(a: List[float], b: List[float]) -> float:
    """Pure-Python cosine similarity between two vectors."""
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)
