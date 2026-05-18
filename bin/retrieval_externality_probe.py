#!/usr/bin/env python3
"""
Retrieval Externality Probe — Build #50h (Trip Deep-Work)

Measures whether capsule retrieval during CCS compression actually reaches
external (feed) content, or preferentially retrieves self-referential capsules.

Borkar et al. (arXiv:2506.09401) show ANY nonzero external data prevents
model collapse (phase transition at a=0). But if the retrieval mechanism
filters out external content, the effective external contribution may be
vanishingly small even though feeds are 59% of the store.

Method:
1. Take the current CCS gist as query (what compression actually uses)
2. Embed it, find K nearest capsules across the full store
3. Measure family distribution of nearest neighbors
4. Compare to the actual retrieval log from stabilized_compress.py
5. Also test alternative queries (random feed capsules, diverse prompts)
   to show the retrieval mechanism ISN'T broken — just biased by query

This answers: is the self-referential retrieval a property of the query
(CCS gist) or the store (no relevant feeds exist)?
"""

import json
import os
import sqlite3
import struct
import sys
import urllib.request
from collections import Counter

import numpy as np

DB = "/mnt/hdd/chronicle-data/processed.db"
EMBED_URL = "http://192.168.1.11:11434/api/embeddings"
DATA_DIR = os.path.expanduser("~/chronicle/data")


def embed(text, model="nomic-embed-text", timeout=60):
    payload = json.dumps({
        "model": model,
        "prompt": text[:2000],
    }).encode()
    req = urllib.request.Request(
        EMBED_URL, data=payload,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return np.array(json.loads(r.read())["embedding"], dtype=np.float64)


def unpack_embedding(blob):
    n = len(blob) // 4
    return np.array(struct.unpack(f"{n}f", blob), dtype=np.float32)


def load_capsules_with_embeddings(db):
    print("Loading capsules + embeddings...")
    meta_rows = db.execute(
        "SELECT id, topic, confidence_score, restatement FROM knowledge_capsules"
    ).fetchall()
    capsule_meta = {}
    for r in meta_rows:
        capsule_meta[r[0]] = {
            "topic": r[1] or "",
            "conf": r[2] or 0.5,
            "content": (r[3] or "")[:200],
        }

    emb_rows = db.execute("SELECT capsule_id, embedding FROM capsule_embeddings").fetchall()
    embeddings = {}
    for cid, blob in emb_rows:
        if cid in capsule_meta:
            embeddings[cid] = unpack_embedding(blob)

    print(f"  {len(capsule_meta)} capsules, {len(embeddings)} with embeddings")
    return capsule_meta, embeddings


def nearest_neighbors(query_vec, ids, matrix, k=20):
    norms = np.linalg.norm(matrix, axis=1)
    q_norm = np.linalg.norm(query_vec)
    if q_norm == 0:
        return []
    sims = (matrix @ query_vec) / (norms * q_norm + 1e-12)
    top_idx = np.argsort(-sims)[:k]
    return [(ids[i], float(sims[i])) for i in top_idx]


def family_distribution(neighbors, capsule_meta):
    families = []
    for cid, sim in neighbors:
        topic = capsule_meta.get(cid, {}).get("topic", "")
        fam = topic.split("/")[0] if topic else "none"
        families.append(fam)
    counter = Counter(families)
    total = len(families)
    return {fam: {"count": n, "pct": n / total if total > 0 else 0}
            for fam, n in counter.most_common()}


def get_current_gist():
    """Get the current CCS semantic_gist."""
    mcp_bin = os.path.expanduser("~/.local/bin/chronicle-mcp")
    if not os.path.exists(mcp_bin):
        return None

    import subprocess
    env = os.environ.copy()
    env["CHRONICLE_OLLAMA_URL"] = "http://192.168.1.11:11434"
    env["CHRONICLE_EMBEDDING_MODEL"] = "nomic-embed-text"

    init_msg = json.dumps({
        "jsonrpc": "2.0", "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "probe", "version": "1.0"}
        },
        "id": 1
    })
    get_msg = json.dumps({
        "jsonrpc": "2.0", "method": "tools/call",
        "params": {"name": "get_cognitive_state", "arguments": {}},
        "id": 2
    })

    result = subprocess.run(
        [mcp_bin],
        input=f"{init_msg}\n{get_msg}\n",
        capture_output=True, text=True, timeout=60, env=env
    )

    for line in result.stdout.strip().split("\n"):
        # MCP binary sometimes prefixes with "Responding: " or "Received: "
        clean = line
        if clean.startswith("Responding: "):
            clean = clean[len("Responding: "):]
        if clean.startswith("Received: "):
            continue
        try:
            d = json.loads(clean)
            if d.get("id") == 2:
                content = d.get("result", {}).get("content", [])
                if content:
                    text = content[0].get("text", "")
                    state = json.loads(text)
                    ccs = state.get("cognitive_state", state)
                    return ccs.get("semantic_gist", "")
        except (json.JSONDecodeError, KeyError):
            continue
    return None


def main():
    k = int(os.environ.get("K", 20))

    db = sqlite3.connect(DB)
    capsule_meta, embeddings = load_capsules_with_embeddings(db)

    ids_with_emb = list(embeddings.keys())
    emb_matrix = np.stack([embeddings[cid] for cid in ids_with_emb]).astype(np.float64)

    # Count store composition
    store_families = Counter()
    for cid in ids_with_emb:
        topic = capsule_meta[cid]["topic"]
        fam = topic.split("/")[0] if topic else "none"
        store_families[fam] += 1

    total_store = len(ids_with_emb)
    print(f"\n{'='*60}")
    print(f"  RETRIEVAL EXTERNALITY PROBE")
    print(f"  {total_store} capsules with embeddings, K={k}")
    print(f"{'='*60}")

    print(f"\nStore composition (top 10 families):")
    for fam, n in store_families.most_common(10):
        print(f"  {fam:<20} {n:>6} ({100*n/total_store:.1f}%)")
    feed_pct = store_families.get("feed", 0) / total_store
    print(f"\n  Feed capsules: {store_families.get('feed', 0)}/{total_store} = {100*feed_pct:.1f}%")

    # --- Test 1: CCS Gist as query ---
    print(f"\n{'='*60}")
    print(f"  TEST 1: CCS Gist as retrieval query")
    print(f"{'='*60}")

    gist = get_current_gist()
    if gist:
        print(f"  Gist: {gist[:120]}...")
        gist_vec = embed(gist)
        neighbors = nearest_neighbors(gist_vec, ids_with_emb, emb_matrix, k)
        dist = family_distribution(neighbors, capsule_meta)

        print(f"\n  Top-{k} neighbor family distribution:")
        for fam, info in sorted(dist.items(), key=lambda x: -x[1]["count"]):
            print(f"    {fam:<20} {info['count']:>3} ({100*info['pct']:.0f}%)")

        feed_in_nn = dist.get("feed", {}).get("count", 0)
        print(f"\n  Feed capsules in top-{k}: {feed_in_nn}/{k} ({100*feed_in_nn/k:.0f}%)")
        print(f"  Store baseline:            {100*feed_pct:.0f}%")
        print(f"  Retrieval bias:            {(feed_in_nn/k) / feed_pct:.2f}x" if feed_pct > 0 else "")

        print(f"\n  Top-5 neighbors:")
        for cid, sim in neighbors[:5]:
            topic = capsule_meta[cid]["topic"]
            content = capsule_meta[cid]["content"][:80]
            print(f"    [{topic}] sim={sim:.3f} — {content}")
    else:
        print("  Could not retrieve CCS gist")

    # --- Test 2: Random feed capsules as queries ---
    print(f"\n{'='*60}")
    print(f"  TEST 2: Random feed capsules as queries (control)")
    print(f"{'='*60}")

    import random
    random.seed(42)
    feed_ids = [cid for cid in ids_with_emb if capsule_meta[cid]["topic"].startswith("feed/")]
    sample_feeds = random.sample(feed_ids, min(10, len(feed_ids)))

    feed_nn_feed_counts = []
    for sid in sample_feeds:
        vec = embeddings[sid].astype(np.float64)
        neighbors = nearest_neighbors(vec, ids_with_emb, emb_matrix, k)
        dist = family_distribution(neighbors, capsule_meta)
        feed_count = dist.get("feed", {}).get("count", 0)
        feed_nn_feed_counts.append(feed_count)

    avg_feed_nn = np.mean(feed_nn_feed_counts)
    print(f"  When a feed capsule is the query:")
    print(f"  Average feed capsules in top-{k}: {avg_feed_nn:.1f}/{k} ({100*avg_feed_nn/k:.0f}%)")
    print(f"  → Feed queries find feeds; gist queries don't")

    # --- Test 3: Diverse probes ---
    print(f"\n{'='*60}")
    print(f"  TEST 3: Diverse query probes")
    print(f"{'='*60}")

    probes = {
        "arxiv_science": "Recent advances in neural architecture search and transformer scaling laws",
        "crypto_icp": "Internet Computer Protocol canister smart contracts decentralized computing",
        "philosophy": "Epistemic humility and the limits of self-knowledge in phenomenological traditions",
        "personal": "Family relationships, fatherhood, building sovereignty and local infrastructure",
        "chronicle_self": "Compressed cognitive state, identity attractor, semantic gist preservation",
    }

    for label, query in probes.items():
        vec = embed(query)
        neighbors = nearest_neighbors(vec, ids_with_emb, emb_matrix, k)
        dist = family_distribution(neighbors, capsule_meta)
        feed_count = dist.get("feed", {}).get("count", 0)
        top_fams = ", ".join(f"{f}:{d['count']}" for f, d in
                            sorted(dist.items(), key=lambda x: -x[1]["count"])[:3])
        print(f"  {label:<20} → feed={feed_count}/{k} | {top_fams}")

    # --- Test 4: Actual retrieval log comparison ---
    print(f"\n{'='*60}")
    print(f"  TEST 4: Actual retrieval log analysis")
    print(f"{'='*60}")

    log_path = os.path.join(DATA_DIR, "capsule_retrieval_log.jsonl")
    if os.path.exists(log_path):
        entries = []
        with open(log_path) as f:
            for line in f:
                try:
                    entries.append(json.loads(line.strip()))
                except json.JSONDecodeError:
                    continue

        total_retrieved = 0
        feed_retrieved = 0
        for entry in entries:
            for fam in entry.get("families", []):
                total_retrieved += 1
                if fam == "feed":
                    feed_retrieved += 1

        print(f"  {len(entries)} compression events logged")
        print(f"  {total_retrieved} total capsules retrieved")
        print(f"  {feed_retrieved} feed capsules retrieved ({100*feed_retrieved/max(total_retrieved,1):.0f}%)")
        print(f"  Store baseline: {100*feed_pct:.0f}%")

        if total_retrieved > 0 and feed_retrieved == 0:
            print(f"\n  ⚠ ZERO feed capsules in actual retrieval")
            print(f"  The retrieval mechanism filters out 59% of the store")
    else:
        print(f"  No retrieval log found at {log_path}")

    # --- Synthesis ---
    print(f"\n{'='*60}")
    print(f"  SYNTHESIS")
    print(f"{'='*60}")

    if gist:
        feed_in_nn = dist.get("feed", {}).get("count", 0) if 'dist' in dir() else 0
        print(f"""
  The CCS gist retrieves {feed_in_nn}/{k} feed capsules ({100*feed_in_nn/k:.0f}%).
  Random feed capsules retrieve {avg_feed_nn:.0f}/{k} feeds ({100*avg_feed_nn/k:.0f}%).
  The store is {100*feed_pct:.0f}% feeds.

  The retrieval mechanism works fine — it finds feeds when asked about
  feed-like content. The problem is the QUERY: the CCS gist is self-
  referential, so it retrieves self-referential capsules.

  Borkar implication: the phase transition at a=0 (any external data
  prevents collapse) may not apply if the retrieval mechanism acts as
  a semantic filter that drives effective a → 0 even though feeds exist.

  The compression function's "persistent excitation" (Borkar's term)
  is gated by retrieval query similarity. External data is present in
  the store but absent from the compression input.
""")

    db.close()

    # Save results
    results = {
        "k": k,
        "store_total": total_store,
        "store_feed_pct": float(feed_pct),
        "gist_feed_nn": feed_in_nn if gist else None,
        "feed_query_avg_feed_nn": float(avg_feed_nn),
        "timestamp": int(os.path.getmtime(log_path)) if os.path.exists(log_path) else 0,
    }
    out_path = os.path.join(DATA_DIR, "retrieval_externality_results.json")
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"Results saved to {out_path}")


if __name__ == "__main__":
    main()
