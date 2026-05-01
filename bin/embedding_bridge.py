#!/usr/bin/env python3
"""
embedding_bridge.py — Cross-space embedding bridge.

Gap #6: Chronicle uses two embedding models in separate spaces:
  - nomic-embed-text (768d): capsule_embeddings table (10K+ vectors)
  - mxbai-embed-large (1024d): nav scoring, coherence checking

This tool bridges them by:
1. Routing queries to the correct model for each space
2. Providing unified search across both spaces
3. Detecting model mismatches (wrong model for wrong space)
4. Offering migration path if we ever unify

Usage:
    embedding_bridge.py search "query"     # Search capsule space (nomic)
    embedding_bridge.py nav-score "ccs"    # Score in nav space (mxbai)
    embedding_bridge.py status             # Show embedding space health
    embedding_bridge.py check              # Detect model mismatches
    embedding_bridge.py dual "query"       # Search both spaces, compare results
"""
import json
import math
import os
import sqlite3
import struct
import sys
import time
import urllib.request

DB = "/mnt/hdd/chronicle-data/processed.db"
OLLAMA = os.environ.get("OLLAMA_URL", "http://192.168.1.11:11434")

# Canonical model assignments
MODELS = {
    "capsule": {
        "model": "nomic-embed-text",
        "dims": 768,
        "bytes_per_vec": 3072,  # 768 * 4
        "table": "capsule_embeddings",
        "purpose": "Persistent memory search (capsules, knowledge graph)",
    },
    "nav": {
        "model": "mxbai-embed-large",
        "dims": 1024,
        "bytes_per_vec": 4096,  # 1024 * 4
        "purpose": "Navigation scoring (CCS quality, coherence, calibration)",
    },
}


def embed(text, model, timeout=30):
    """Get embedding from specified model."""
    body = json.dumps({"model": model, "prompt": text[:800]}).encode()
    req = urllib.request.Request(
        f"{OLLAMA}/api/embeddings", data=body,
        headers={"Content-Type": "application/json"}
    )
    resp = urllib.request.urlopen(req, timeout=timeout)
    return json.loads(resp.read())["embedding"]


def cosine(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def search_capsules(query, top_k=5):
    """Search capsule embedding space using nomic-embed-text."""
    model_cfg = MODELS["capsule"]
    q_vec = embed(query, model_cfg["model"])

    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT ce.capsule_id, ce.embedding, kc.restatement, kc.topic "
        "FROM capsule_embeddings ce "
        "JOIN knowledge_capsules kc ON ce.capsule_id = kc.id "
    ).fetchall()
    db.close()

    scored = []
    for cid, emb_blob, text, topic in rows:
        n_floats = len(emb_blob) // 4
        if n_floats != model_cfg["dims"]:
            continue  # Skip mismatched dimensions
        emb = list(struct.unpack(f'{n_floats}f', emb_blob))
        sim = cosine(q_vec, emb)
        scored.append((sim, cid, text[:200] if text else "", topic or ""))

    scored.sort(key=lambda x: -x[0])
    return scored[:top_k]


def show_status():
    """Show embedding space health and stats."""
    db = sqlite3.connect(DB)

    # Capsule space
    cap_count = db.execute("SELECT COUNT(*) FROM capsule_embeddings").fetchone()[0]
    cap_dims = db.execute(
        "SELECT LENGTH(embedding) FROM capsule_embeddings LIMIT 1"
    ).fetchone()
    cap_dim_val = cap_dims[0] // 4 if cap_dims else 0

    # Check for dimension mismatches in capsule space
    dim_groups = db.execute(
        "SELECT LENGTH(embedding) as sz, COUNT(*) FROM capsule_embeddings GROUP BY sz"
    ).fetchall()

    db.close()

    print("EMBEDDING SPACE STATUS")
    print("=" * 50)
    print()
    print(f"Capsule space (nomic-embed-text, {MODELS['capsule']['dims']}d):")
    print(f"  Vectors: {cap_count:,}")
    print(f"  Actual dims: {cap_dim_val}")
    if len(dim_groups) > 1:
        print(f"  WARNING: Mixed dimensions detected!")
        for sz, cnt in dim_groups:
            print(f"    {sz//4}d: {cnt} vectors")
    else:
        print(f"  Dimensions: consistent")

    print()
    print(f"Nav space (mxbai-embed-large, {MODELS['nav']['dims']}d):")
    print(f"  Usage: calibration_nav_score.py, coherence_watch.py")
    print(f"  Storage: ephemeral (computed per query, not persisted)")

    print()
    print("MODEL ASSIGNMENT:")
    for space, cfg in MODELS.items():
        print(f"  {space:10} → {cfg['model']} ({cfg['dims']}d) — {cfg['purpose']}")

    print()
    print("RULES:")
    print("  - Capsule queries MUST use nomic-embed-text")
    print("  - Nav scoring MUST use mxbai-embed-large")
    print("  - Never cosine-compare vectors from different models")
    print("  - New tools: check which space you're in before choosing a model")


def check_mismatches():
    """Detect potential model mismatches in codebase."""
    import subprocess
    print("Checking for potential embedding model mismatches...\n")

    issues = []

    # Check all Python files for embedding model references
    bin_dir = os.path.expanduser("~/chronicle/bin")
    for f in sorted(os.listdir(bin_dir)):
        if not f.endswith(".py"):
            continue
        path = os.path.join(bin_dir, f)
        try:
            content = open(path).read()
        except Exception:
            continue

        uses_nomic = "nomic-embed-text" in content or "nomic" in content.lower()
        uses_mxbai = "mxbai-embed-large" in content or "mxbai" in content.lower()
        uses_capsule_embed = "capsule_embeddings" in content
        uses_nav = "calibration" in content.lower() and "embed" in content.lower()

        # Flag: uses capsule_embeddings but queries with mxbai
        if uses_capsule_embed and uses_mxbai and not uses_nomic:
            issues.append(f"  MISMATCH: {f} — reads capsule_embeddings but uses mxbai model")

        # Flag: nav scoring but uses nomic
        if uses_nav and uses_nomic and not uses_mxbai:
            issues.append(f"  MISMATCH: {f} — nav scoring but uses nomic model")

        # Info: uses embeddings
        if uses_nomic or uses_mxbai:
            models = []
            if uses_nomic: models.append("nomic")
            if uses_mxbai: models.append("mxbai")
            spaces = []
            if uses_capsule_embed: spaces.append("capsule")
            if uses_nav: spaces.append("nav")
            # Only print if potentially interesting
            if len(models) > 1 or (uses_capsule_embed and uses_mxbai):
                issues.append(f"  CHECK: {f} — models: {', '.join(models)}, spaces: {', '.join(spaces) or 'unknown'}")

    if issues:
        print(f"Found {len(issues)} items to review:\n")
        for issue in issues:
            print(issue)
    else:
        print("No mismatches detected.")


def dual_search(query, top_k=5):
    """Search both embedding spaces, compare what surfaces."""
    print(f"Dual-space search: {query[:60]}...\n")

    # Capsule space
    print(f"--- Capsule space (nomic-embed-text) ---")
    try:
        results = search_capsules(query, top_k)
        for sim, cid, text, topic in results:
            print(f"  {sim:.4f}  #{cid} [{topic}] {text[:60]}...")
    except Exception as e:
        print(f"  Error: {e}")

    print()

    # Nav space — just show the embedding, can't search stored vectors
    print(f"--- Nav space (mxbai-embed-large) ---")
    try:
        vec = embed(query, MODELS["nav"]["model"])
        print(f"  Embedded OK ({len(vec)}d)")
        print(f"  (Nav space is ephemeral — no stored vectors to search)")
    except Exception as e:
        print(f"  Error: {e}")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return

    cmd = sys.argv[1]

    if cmd == "search":
        query = " ".join(sys.argv[2:]) if len(sys.argv) > 2 else ""
        if not query:
            print("Usage: embedding_bridge.py search \"query\"")
            return
        results = search_capsules(query)
        for sim, cid, text, topic in results:
            print(f"  {sim:.4f}  #{cid} [{topic}] {text[:80]}...")

    elif cmd == "status":
        show_status()

    elif cmd == "check":
        check_mismatches()

    elif cmd == "dual":
        query = " ".join(sys.argv[2:]) if len(sys.argv) > 2 else ""
        if not query:
            print("Usage: embedding_bridge.py dual \"query\"")
            return
        dual_search(query)

    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)


if __name__ == "__main__":
    main()
