#!/usr/bin/env python3
"""
Embedding Sweep — backfill missing embeddings on the canister.

Runs on AGX where Ollama lives. Queries canister for capsules without
embeddings, generates them via Ollama, pushes them back in bulk.

Designed to run as a timer or cron job.
"""

import subprocess
import json
import re
import requests
import sys
import os
import time

CANISTER_ID = os.environ.get("CHRONICLE_CANISTER_ID", "fqqku-bqaaa-aaaai-q4wha-cai")
OLLAMA_URL_PRIMARY = "http://localhost:11434"
OLLAMA_URL_FALLBACK = "http://localhost:11434"
EMBED_MODEL = "snowflake-arctic-embed2"
EMBED_DIM = 1024
BATCH_SIZE = int(os.environ.get("SWEEP_BATCH_SIZE", "200"))  # capsules per sweep cycle
DFX_BIN = os.environ.get("DFX_BIN", os.path.expanduser("~/.local/share/dfx/bin/dfx"))
DFX_ENV = {"DFX_WARNING": "-mainnet_plaintext_identity", "PATH": os.environ.get("PATH", ""), "HOME": os.environ.get("HOME", "")}


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def dfx_call(method, args, timeout=30, query=False):
    """Call a canister method via dfx.

    Pass query=True for #[query] methods so they run as non-replicated reads
    (free cycles) instead of consensus-replicated updates.
    """
    cmd = [DFX_BIN, "canister", "--network", "ic", "call", CANISTER_ID, method, args]
    if query:
        cmd.append("--query")
    result = subprocess.run(cmd, capture_output=True, text=True, env={**os.environ, **DFX_ENV}, timeout=timeout)
    if result.returncode != 0:
        raise RuntimeError(f"dfx call {method} failed: {result.stderr}")
    return result.stdout


def get_missing_ids(limit=500):
    """Get capsule IDs that have no embedding."""
    raw = dfx_call("get_capsules_without_embeddings", f"({limit} : nat64)", query=True)
    ids = []
    for match in re.finditer(r'(\d[\d_]*)\s*:\s*nat64', raw):
        num_str = match.group(1).replace("_", "")
        try:
            ids.append(int(num_str))
        except ValueError:
            continue
    return ids


def get_capsule_content(capsule_id):
    """Get a single capsule's content from the canister."""
    raw = dfx_call("get_capsule", f"({capsule_id} : nat64)", query=True)
    # Handle both named fields and numeric Candid hashes:
    #   content = "..." OR 2_157_225_500 = "..."
    match = re.search(r'(?:2_157_225_500|content)\s*=\s*"((?:[^"\\]|\\.)*)"', raw)
    if match:
        return match.group(1).replace('\\n', '\n').replace('\\"', '"')
    return None


def _pick_embed_url() -> str:
    """Return reachable Ollama URL, preferring Jetson."""
    for url in [OLLAMA_URL_PRIMARY, OLLAMA_URL_FALLBACK]:
        try:
            r = requests.get(f"{url}/api/tags", timeout=3)
            if r.status_code == 200:
                return url
        except Exception:
            continue
    return OLLAMA_URL_FALLBACK


def embed_text(text):
    """Generate embedding via snowflake-arctic-embed2 with prefix and dimension check."""
    url = _pick_embed_url()
    resp = requests.post(
        f"{url}/api/embeddings",
        json={"model": EMBED_MODEL, "prompt": text},
        timeout=30,
    )
    resp.raise_for_status()
    emb = resp.json().get("embedding", [])
    if len(emb) != EMBED_DIM:
        raise ValueError(f"Dimension mismatch: got {len(emb)}, expected {EMBED_DIM}")
    return emb


def push_embeddings(embeddings):
    """Push a batch of embeddings to the canister."""
    if not embeddings:
        return 0

    records = []
    for capsule_id, embedding in embeddings:
        floats = "; ".join(f"{v} : float32" for v in embedding)
        records.append(
            f'record {{ capsule_id = {capsule_id} : nat64; '
            f'embedding = vec {{ {floats} }}; '
            f'model_name = "{EMBED_MODEL}" }}'
        )

    candid = f"(vec {{ {'; '.join(records)} }})"

    if len(candid) > 100_000:
        tmpfile = "/tmp/embed_sweep_args.txt"
        with open(tmpfile, "w") as f:
            f.write(candid)
        cmd = [
            DFX_BIN, "canister", "--network", "ic", "call", CANISTER_ID,
            "add_embeddings_bulk", "--argument-file", tmpfile,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, env={**os.environ, **DFX_ENV}, timeout=120)
        os.remove(tmpfile)
    else:
        result = subprocess.run(
            [DFX_BIN, "canister", "--network", "ic", "call", CANISTER_ID, "add_embeddings_bulk", candid],
            capture_output=True, text=True, env={**os.environ, **DFX_ENV}, timeout=120,
        )

    if result.returncode != 0:
        raise RuntimeError(f"add_embeddings_bulk failed: {result.stderr[:500]}")

    out = result.stdout.strip()
    try:
        count = int(out.strip("() \n").split(":")[0].strip().replace("_", ""))
    except (ValueError, IndexError):
        count = len(embeddings)

    return count


def main():
    log(f"Embedding sweep starting (canister={CANISTER_ID}, model={EMBED_MODEL})")

    missing = get_missing_ids(BATCH_SIZE)
    if not missing:
        log("No missing embeddings. All clear.")
        return

    log(f"Found {len(missing)} capsules without embeddings")

    embedded = []
    errors = 0

    for i, capsule_id in enumerate(missing):
        try:
            text = get_capsule_content(capsule_id)
            if not text or len(text) < 10:
                log(f"  [{i+1}/{len(missing)}] #{capsule_id} — no content or too short, skipping")
                errors += 1
                continue

            embedding = embed_text(text[:2000])  # truncate very long content
            embedded.append((capsule_id, embedding))
            if (i + 1) % 20 == 0:
                log(f"  [{i+1}/{len(missing)}] embedded so far: {len(embedded)}")

        except Exception as e:
            log(f"  [{i+1}/{len(missing)}] #{capsule_id} — ERROR: {e}")
            errors += 1
            continue

    if embedded:
        push_batch_size = 5
        total_pushed = 0
        for batch_start in range(0, len(embedded), push_batch_size):
            batch = embedded[batch_start:batch_start + push_batch_size]
            try:
                count = push_embeddings(batch)
                total_pushed += count
                log(f"  Pushed batch {batch_start//push_batch_size + 1}: {count} embeddings")
            except Exception as e:
                log(f"  Push batch failed: {e}")
                errors += len(batch)

        log(f"Sweep complete: {total_pushed} embedded, {errors} errors")
    else:
        log(f"Sweep complete: nothing to push ({errors} errors)")


if __name__ == "__main__":
    main()
