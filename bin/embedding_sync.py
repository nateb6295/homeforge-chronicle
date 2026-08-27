#!/usr/bin/env python3
"""Embedding Sync — fills the on-chain embedding gap.

Finds capsules on the canister that don't have embeddings yet,
computes embeddings locally via Ollama, and stores them on-chain.

Runs on AGX as a systemd timer (every 5 min).
"""

import json
import os
import re
import subprocess
import sys
import time
sys.path.insert(0, os.path.dirname(__file__))
from embed_config import EMBED_URL, EMBED_MODEL, EMBED_DIM, embed as _embed_raw

# ── Configuration ──
CANISTER_ID = os.environ.get("CANISTER_ID", "fqqku-bqaaa-aaaai-q4wha-cai")
DFX_BIN = os.path.expanduser("~/.local/share/dfx/bin/dfx")
BATCH_SIZE = 20  # capsules per run — keep it light
DFX_ENV = {**os.environ, "DFX_WARNING": "-mainnet_plaintext_identity"}


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def dfx_call(method, args="()", query=False):
    """Run a dfx canister call.

    Pass query=True for #[query] methods so they run as non-replicated reads
    (free cycles) instead of replicated updates. dfx defaults to update mode
    unless --query is explicit; calling a #[query] method without the flag
    burns cycles on consensus for a read that doesn't need it.
    """
    cmd = [DFX_BIN, "canister", "--network", "ic", "call", CANISTER_ID, method, args]
    if query:
        cmd.append("--query")
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=60, env=DFX_ENV,
        )
        return result.stdout.strip()
    except Exception as e:
        log(f"  dfx error: {e}")
        return ""


def get_counts():
    """Get capsule and embedding counts."""
    cap_raw = dfx_call("get_capsule_count", query=True)
    emb_raw = dfx_call("get_embedding_count", query=True)
    cap_match = re.search(r'([\d_]+)', cap_raw)
    emb_match = re.search(r'([\d_]+)', emb_raw)
    caps = int(cap_match.group(1).replace('_', '')) if cap_match else 0
    embs = int(emb_match.group(1).replace('_', '')) if emb_match else 0
    return caps, embs


def get_capsules_batch(offset, limit):
    """Fetch a batch of capsules from canister."""
    raw = dfx_call("get_capsules_paginated", f"({offset} : nat64, {limit} : nat64)", query=True)
    if not raw:
        return []
    capsules = []
    # Candid outputs numeric field hashes when .did isn't found:
    #   23_515 = <id> : nat64           (hash of "id")
    #   2_157_225_500 = "<content>"     (hash of "content")
    # Also handle named fields for when .did IS available:
    #   id = <id> : nat64
    #   content = "<content>"
    id_pattern = r'(?:23_515|id)\s*=\s*([\d_]+)\s*:\s*nat64'
    content_pattern = r'(?:2_157_225_500|content)\s*=\s*"((?:[^"\\]|\\.)*)"'
    ids = re.findall(id_pattern, raw)
    contents = re.findall(content_pattern, raw)
    for i, cid in enumerate(ids):
        content = contents[i] if i < len(contents) else ""
        capsules.append({
            "id": int(cid.replace('_', '')),
            "content": content.replace('\\n', '\n')[:500],
        })
    return capsules


def has_embedding(capsule_id):
    """Check if a capsule already has an embedding on-chain."""
    raw = dfx_call("get_embedding", f"({capsule_id} : nat64)", query=True)
    # Returns "(null)" when no embedding exists, or "(opt record { ... })" when it does
    return raw.strip() != "(null)" and "opt record" in raw


def compute_embedding(text):
    """Compute embedding via shared embed_config."""
    try:
        emb = _embed_raw(text)
        if emb and len(emb) == EMBED_DIM:
            return emb
        elif emb:
            log(f"  Dimension mismatch: got {len(emb)}, expected {EMBED_DIM}")
    except Exception as e:
        log(f"  Embed error: {e}")
    return None


def store_embedding(capsule_id, embedding):
    """Store embedding on-chain."""
    vec_str = "vec { " + "; ".join(f"{v:.6f} : float32" for v in embedding) + " }"
    try:
        result = subprocess.run(
            [DFX_BIN, "canister", "--network", "ic", "call", CANISTER_ID,
             "add_embedding", f'({capsule_id} : nat64, {vec_str}, "{EMBED_MODEL}")'],
            capture_output=True, text=True, timeout=180, env=DFX_ENV,
        )
        return "(true)" in result.stdout or "true" in result.stdout.lower()
    except Exception as e:
        log(f"  Store error for {capsule_id}: {e}")
        return False


def main():
    log("=== Embedding Sync starting ===")

    caps, embs = get_counts()
    gap = caps - embs
    log(f"Capsules: {caps}, Embeddings: {embs}, Gap: {gap}")

    if gap <= 0:
        log("No gap. Done.")
        return

    # Strategy: scan newest capsules first (most likely to be missing embeddings)
    filled = 0
    checked = 0
    offset = max(0, caps - 50)

    while filled < BATCH_SIZE and offset >= 0:
        batch = get_capsules_batch(offset, min(50, caps - offset))
        if not batch:
            log(f"  No capsules at offset {offset}, moving back")
            offset -= 50
            continue

        log(f"  Scanning {len(batch)} capsules at offset {offset}...")
        for capsule in batch:
            if filled >= BATCH_SIZE:
                break
            checked += 1

            if has_embedding(capsule["id"]):
                continue

            content = capsule["content"]
            if not content or len(content) < 10:
                continue

            embedding = compute_embedding(content)
            if not embedding:
                continue

            if store_embedding(capsule["id"], embedding):
                filled += 1
                log(f"  ✓ Embedded capsule {capsule['id']} ({filled}/{BATCH_SIZE})")

        offset -= 50

    log(f"=== Done: checked {checked}, filled {filled}, remaining gap ~{gap - filled} ===")


if __name__ == "__main__":
    main()
