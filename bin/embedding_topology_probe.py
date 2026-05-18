#!/usr/bin/env python3
"""
Build #51: Capsule Embedding Topology Probe

Maps the embedding space structure to understand why the CCS gist
retrieval creates a semantic membrane excluding feed capsules.

Questions:
1. Are feeds geometrically separated from self-referential capsules?
2. Where does the CCS gist sit in this space?
3. Are there bridge regions between clusters?
4. What's the curvature of the CCS trajectory?
"""

import json
import struct
import sqlite3
import subprocess
import sys
import numpy as np
from pathlib import Path
from collections import defaultdict

DB_PATH = "/mnt/hdd/chronicle-data/processed.db"
MCP_BIN = "/home/nate-agx/.local/bin/chronicle-mcp"
RESULTS_PATH = Path("/home/nate-agx/chronicle/data/embedding_topology_results.json")

def get_db():
    return sqlite3.connect(DB_PATH)

def decode_embedding(blob):
    n = len(blob) // 4
    return np.array(struct.unpack(f'{n}f', blob))

def classify_family(topic):
    if not topic:
        return 'other'
    t = topic.lower()
    if t.startswith('feed/'):
        return 'feed'
    if t.startswith('thread') or t.startswith('threads/'):
        return 'thread'
    if t.startswith('chronicle/'):
        return 'chronicle'
    if t.startswith('discord/'):
        return 'discord'
    if t.startswith('identity/'):
        return 'identity'
    if t.startswith('homeforge') or t == 'homeforge':
        return 'homeforge'
    if t.startswith('research/'):
        return 'research'
    if t.startswith('crossref/'):
        return 'crossref'
    return 'other'

def get_ccs_gist_embedding():
    """Get CCS gist and embed it."""
    env = {"HOME": "/home/nate-agx"}
    init_msg = json.dumps({
        "jsonrpc": "2.0", "method": "initialize",
        "params": {"protocolVersion": "2024-11-05", "capabilities": {},
                    "clientInfo": {"name": "probe", "version": "1.0"}},
        "id": 1
    })
    call_msg = json.dumps({
        "jsonrpc": "2.0", "method": "tools/call",
        "params": {"name": "get_cognitive_state", "arguments": {}},
        "id": 2
    })
    try:
        result = subprocess.run(
            [MCP_BIN], input=f"{init_msg}\n{call_msg}\n",
            capture_output=True, text=True, timeout=30, env=env
        )
        for line in result.stdout.strip().split('\n'):
            line = line.strip()
            if line.startswith("Responding: "):
                line = line[len("Responding: "):]
            if '"id":2' in line or '"id": 2' in line:
                data = json.loads(line)
                content = data.get("result", {}).get("content", [{}])
                text = content[0].get("text", "{}") if content else "{}"
                state = json.loads(text)
                state = state.get("cognitive_state", state)
                return state.get("semantic_gist", "")
    except Exception as e:
        print(f"  CCS gist retrieval failed: {e}")
    return None

def embed_text(text, model="nomic-embed-text"):
    """Embed text using Ollama."""
    result = subprocess.run(
        ["curl", "-s", "http://192.168.1.11:11434/api/embeddings",
         "-d", json.dumps({"model": model, "prompt": text[:2000]})],
        capture_output=True, text=True, timeout=60
    )
    data = json.loads(result.stdout)
    return np.array(data["embedding"])

def sample_embeddings(db, n_per_family=200):
    """Sample embeddings from each family, return arrays + labels."""
    cur = db.cursor()
    cur.execute("""
        SELECT ce.capsule_id, ce.embedding, kc.topic
        FROM capsule_embeddings ce
        JOIN knowledge_capsules kc ON kc.id = ce.capsule_id
    """)

    by_family = defaultdict(list)
    for cid, blob, topic in cur:
        fam = classify_family(topic)
        by_family[fam].append((cid, blob, topic))

    embeddings = []
    labels = []
    ids = []
    topics = []

    for fam, items in by_family.items():
        np.random.seed(42)
        sampled = [items[i] for i in np.random.choice(len(items), min(n_per_family, len(items)), replace=False)]
        for cid, blob, topic in sampled:
            emb = decode_embedding(blob)
            embeddings.append(emb)
            labels.append(fam)
            ids.append(cid)
            topics.append(topic)

    return np.array(embeddings), labels, ids, topics

def compute_family_centroids(embeddings, labels):
    """Compute mean embedding per family."""
    families = sorted(set(labels))
    centroids = {}
    for fam in families:
        mask = [i for i, l in enumerate(labels) if l == fam]
        centroids[fam] = embeddings[mask].mean(axis=0)
    return centroids

def inter_family_distances(centroids):
    """Compute cosine distances between family centroids."""
    families = sorted(centroids.keys())
    distances = {}
    for i, f1 in enumerate(families):
        for f2 in families[i+1:]:
            cos_sim = np.dot(centroids[f1], centroids[f2]) / (
                np.linalg.norm(centroids[f1]) * np.linalg.norm(centroids[f2])
            )
            distances[f"{f1}<->{f2}"] = round(float(1 - cos_sim), 4)
    return distances

def intra_family_spread(embeddings, labels):
    """Compute average pairwise cosine distance within each family (subsample)."""
    families = sorted(set(labels))
    spreads = {}
    for fam in families:
        mask = [i for i, l in enumerate(labels) if l == fam]
        if len(mask) < 2:
            spreads[fam] = 0.0
            continue
        fam_embs = embeddings[mask]
        # subsample for speed
        if len(fam_embs) > 50:
            idx = np.random.choice(len(fam_embs), 50, replace=False)
            fam_embs = fam_embs[idx]
        norms = fam_embs / np.linalg.norm(fam_embs, axis=1, keepdims=True)
        sims = norms @ norms.T
        n = len(fam_embs)
        avg_sim = (sims.sum() - n) / (n * (n - 1))
        spreads[fam] = round(float(1 - avg_sim), 4)
    return spreads

def gist_to_family_distances(gist_emb, centroids):
    """Cosine distance from CCS gist to each family centroid."""
    distances = {}
    for fam, centroid in centroids.items():
        cos_sim = np.dot(gist_emb, centroid) / (
            np.linalg.norm(gist_emb) * np.linalg.norm(centroid)
        )
        distances[fam] = round(float(1 - cos_sim), 4)
    return distances

def nearest_feeds_to_gist(gist_emb, embeddings, labels, topics, ids, k=10):
    """Find k nearest feed capsules to the CCS gist."""
    feed_mask = [i for i, l in enumerate(labels) if l == 'feed']
    if not feed_mask:
        return []
    feed_embs = embeddings[feed_mask]
    norms_f = feed_embs / np.linalg.norm(feed_embs, axis=1, keepdims=True)
    norm_g = gist_emb / np.linalg.norm(gist_emb)
    sims = norms_f @ norm_g
    top_k = np.argsort(sims)[-k:][::-1]
    results = []
    for idx in top_k:
        orig_idx = feed_mask[idx]
        results.append({
            "capsule_id": int(ids[orig_idx]),
            "topic": topics[orig_idx],
            "similarity": round(float(sims[idx]), 4)
        })
    return results

def nearest_self_to_gist(gist_emb, embeddings, labels, topics, ids, k=10):
    """Find k nearest self-referential capsules to the CCS gist."""
    self_families = {'chronicle', 'thread', 'identity', 'homeforge', 'research'}
    self_mask = [i for i, l in enumerate(labels) if l in self_families]
    if not self_mask:
        return []
    self_embs = embeddings[self_mask]
    norms_s = self_embs / np.linalg.norm(self_embs, axis=1, keepdims=True)
    norm_g = gist_emb / np.linalg.norm(gist_emb)
    sims = norms_s @ norm_g
    top_k = np.argsort(sims)[-k:][::-1]
    results = []
    for idx in top_k:
        orig_idx = self_mask[idx]
        results.append({
            "capsule_id": int(ids[orig_idx]),
            "topic": topics[orig_idx],
            "similarity": round(float(sims[idx]), 4)
        })
    return results

def membrane_thickness(gist_emb, embeddings, labels):
    """
    The 'membrane' is the gap between the nearest feed and nearest self-ref capsule.
    Measure: similarity to nearest self-ref minus similarity to nearest feed.
    Larger gap = thicker membrane.
    """
    norm_g = gist_emb / np.linalg.norm(gist_emb)
    self_families = {'chronicle', 'thread', 'identity', 'homeforge', 'research'}

    best_self = -1.0
    best_feed = -1.0
    for i, l in enumerate(labels):
        emb = embeddings[i]
        sim = float(np.dot(norm_g, emb / np.linalg.norm(emb)))
        if l in self_families and sim > best_self:
            best_self = sim
        if l == 'feed' and sim > best_feed:
            best_feed = sim

    return {
        "nearest_self_similarity": round(best_self, 4),
        "nearest_feed_similarity": round(best_feed, 4),
        "membrane_gap": round(best_self - best_feed, 4)
    }

def feed_subfamily_distances(embeddings, labels, topics, gist_emb):
    """Break feeds into subfamilies and measure distance to gist."""
    feed_subs = defaultdict(list)
    for i, (l, t) in enumerate(zip(labels, topics)):
        if l == 'feed' and t:
            sub = t.split('/')[1] if '/' in t else 'unknown'
            feed_subs[sub].append(i)

    results = {}
    norm_g = gist_emb / np.linalg.norm(gist_emb)
    for sub, indices in sorted(feed_subs.items(), key=lambda x: -len(x[1])):
        if len(indices) < 3:
            continue
        sub_embs = embeddings[indices]
        centroid = sub_embs.mean(axis=0)
        centroid_norm = centroid / np.linalg.norm(centroid)
        sim = float(np.dot(norm_g, centroid_norm))
        results[sub] = {
            "count": len(indices),
            "gist_similarity": round(sim, 4),
            "gist_distance": round(1 - sim, 4)
        }
    return results


def main():
    print("Build #51: Capsule Embedding Topology Probe")
    print("=" * 50)

    db = get_db()

    # 1. Sample embeddings
    print("\n[1/7] Sampling embeddings from each family...")
    embeddings, labels, ids, topics = sample_embeddings(db, n_per_family=300)
    print(f"  Sampled {len(embeddings)} capsules across {len(set(labels))} families")
    for fam in sorted(set(labels)):
        count = sum(1 for l in labels if l == fam)
        print(f"    {fam}: {count}")

    # 2. Compute centroids
    print("\n[2/7] Computing family centroids...")
    centroids = compute_family_centroids(embeddings, labels)

    # 3. Inter-family distances
    print("\n[3/7] Inter-family centroid distances (cosine distance)...")
    inter_dists = inter_family_distances(centroids)
    for pair, dist in sorted(inter_dists.items(), key=lambda x: x[1]):
        print(f"  {pair}: {dist}")

    # 4. Intra-family spread
    print("\n[4/7] Intra-family spread (avg pairwise cosine distance)...")
    spreads = intra_family_spread(embeddings, labels)
    for fam, spread in sorted(spreads.items(), key=lambda x: x[1]):
        print(f"  {fam}: {spread}")

    # 5. CCS gist position
    print("\n[5/7] Getting CCS gist and embedding it...")
    gist_text = get_ccs_gist_embedding()
    if gist_text:
        print(f"  Gist: {gist_text[:100]}...")
        gist_emb = embed_text(gist_text)

        print("\n  Gist distance to family centroids:")
        gist_dists = gist_to_family_distances(gist_emb, centroids)
        for fam, dist in sorted(gist_dists.items(), key=lambda x: x[1]):
            print(f"    {fam}: {dist}")

        # 6. Membrane measurement
        print("\n[6/7] Membrane thickness measurement...")
        membrane = membrane_thickness(gist_emb, embeddings, labels)
        print(f"  Nearest self-ref similarity: {membrane['nearest_self_similarity']}")
        print(f"  Nearest feed similarity: {membrane['nearest_feed_similarity']}")
        print(f"  Membrane gap: {membrane['membrane_gap']}")

        # Nearest feeds and self-ref
        print("\n  Top 5 nearest feeds to gist:")
        nearest_feeds = nearest_feeds_to_gist(gist_emb, embeddings, labels, topics, ids, k=5)
        for nf in nearest_feeds:
            print(f"    [{nf['similarity']}] {nf['topic']} (#{nf['capsule_id']})")

        print("\n  Top 5 nearest self-ref to gist:")
        nearest_self = nearest_self_to_gist(gist_emb, embeddings, labels, topics, ids, k=5)
        for ns in nearest_self:
            print(f"    [{ns['similarity']}] {ns['topic']} (#{ns['capsule_id']})")

        # 7. Feed subfamily analysis
        print("\n[7/7] Feed subfamily distances to gist...")
        feed_subs = feed_subfamily_distances(embeddings, labels, topics, gist_emb)
        for sub, info in sorted(feed_subs.items(), key=lambda x: x[1]['gist_distance']):
            print(f"  {sub}: dist={info['gist_distance']} (n={info['count']})")

        results = {
            "probe": "Build #51 — Embedding Topology",
            "timestamp": __import__('time').strftime('%Y-%m-%d %H:%M:%S'),
            "sample_size": len(embeddings),
            "families_sampled": dict(sorted(
                {fam: sum(1 for l in labels if l == fam) for fam in set(labels)}.items(),
                key=lambda x: -x[1]
            )),
            "inter_family_distances": inter_dists,
            "intra_family_spreads": spreads,
            "gist_to_centroids": gist_dists,
            "membrane": membrane,
            "nearest_feeds": nearest_feeds,
            "nearest_self": nearest_self,
            "feed_subfamilies": feed_subs
        }
    else:
        print("  Could not get CCS gist. Running without gist analysis.")
        results = {
            "probe": "Build #51 — Embedding Topology",
            "timestamp": __import__('time').strftime('%Y-%m-%d %H:%M:%S'),
            "sample_size": len(embeddings),
            "inter_family_distances": inter_dists,
            "intra_family_spreads": spreads
        }

    RESULTS_PATH.write_text(json.dumps(results, indent=2))
    print(f"\nResults saved to {RESULTS_PATH}")

if __name__ == "__main__":
    main()
