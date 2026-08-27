"""
Shared embedding configuration. Single source of truth.
Import this instead of hardcoding model/URL in every script.

Usage:
    from embed_config import EMBED_URL, EMBED_MODEL, EMBED_DIM, embed
"""
import json
import os
import urllib.request

EMBED_URL = os.environ.get("CHRONICLE_OLLAMA_URL", "http://localhost:11434")
EMBED_MODEL = os.environ.get("CHRONICLE_EMBEDDING_MODEL", "snowflake-arctic-embed2")
EMBED_DIM = 1024


def embed(text, timeout=30, truncate=800):
    """Single text → 1024-d vector via /api/embeddings (legacy endpoint)."""
    body = json.dumps({"model": EMBED_MODEL, "prompt": text[:truncate]}).encode()
    req = urllib.request.Request(
        f"{EMBED_URL}/api/embeddings",
        data=body,
        headers={"Content-Type": "application/json"},
    )
    resp = urllib.request.urlopen(req, timeout=timeout)
    return json.loads(resp.read())["embedding"]


def embed_batch(texts, timeout=30, truncate=800):
    """Single or multiple texts → list of vectors via /api/embed (newer endpoint)."""
    if isinstance(texts, str):
        texts = [texts]
    body = json.dumps({"model": EMBED_MODEL, "input": [t[:truncate] for t in texts]}).encode()
    req = urllib.request.Request(
        f"{EMBED_URL}/api/embed",
        data=body,
        headers={"Content-Type": "application/json"},
    )
    resp = urllib.request.urlopen(req, timeout=timeout)
    return json.loads(resp.read())["embeddings"]
