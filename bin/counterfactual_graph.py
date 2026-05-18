#!/usr/bin/env python3
"""
Counterfactual Graph Simulation (Trip Deep-Work Option 5)

Recompute capsule neighbor selection under two regimes:
  1. Current: topic_diversity * similarity_bell * recency * confidence * foundation_boost
  2. Uniform: raw cosine similarity only (no boosts, no penalties)

Compare degree distributions, clustering coefficients, and same-topic edge rates.
Uses existing embeddings from capsule_embeddings table.

Hermes predicted: higher clustering coefficient without boosts (same-topic
clusters can form when the 0.5x penalty is removed).
"""

import json
import math
import os
import random
import sqlite3
import struct
import sys
import time
from collections import Counter, defaultdict

import numpy as np

DB = "/mnt/hdd/chronicle-data/processed.db"
SAMPLE_N = 500
NEIGHBORS_K = 15  # matches MAX_CONNECTIONS_PER_CAPSULE
BELL_CENTER = 0.55
BELL_WIDTH = 0.20
QUALITY_FLOOR = 0.15


def unpack_embedding(blob):
    n = len(blob) // 4
    return np.array(struct.unpack(f"{n}f", blob), dtype=np.float32)


def cosine_sim_batch(vec, matrix):
    norms = np.linalg.norm(matrix, axis=1)
    vec_norm = np.linalg.norm(vec)
    if vec_norm == 0:
        return np.zeros(len(matrix))
    return (matrix @ vec) / (norms * vec_norm + 1e-12)


def compute_quality(topic_a, topic_b, similarity, age_days, conf_a, conf_b):
    if topic_a and topic_b:
        if topic_a == topic_b:
            topic_diversity = 0.5
        else:
            fam_a = topic_a.split("/")[0]
            fam_b = topic_b.split("/")[0]
            topic_diversity = 1.0 if fam_a == fam_b else 1.5
    else:
        topic_diversity = 1.0

    z = (similarity - BELL_CENTER) / BELL_WIDTH
    similarity_bell = math.exp(-z * z)
    recency = 0.3 + 0.7 * math.exp(-age_days / 30.0)
    confidence = 0.5 + (conf_a + conf_b) / 2.0

    is_foundation = False
    for t in (topic_a, topic_b):
        if t and (t.startswith("foundation") or t.startswith("homeforge")):
            is_foundation = True
            break
    foundation_boost = 2.0 if is_foundation else 1.0

    return topic_diversity * similarity_bell * recency * confidence * foundation_boost


def clustering_coefficient(adj):
    """Compute average local clustering coefficient from adjacency dict."""
    coeffs = []
    for node, neighbors in adj.items():
        n = len(neighbors)
        if n < 2:
            coeffs.append(0.0)
            continue
        triangles = 0
        neighbor_list = list(neighbors)
        for i in range(len(neighbor_list)):
            for j in range(i + 1, len(neighbor_list)):
                if neighbor_list[j] in adj.get(neighbor_list[i], set()):
                    triangles += 1
        max_triangles = n * (n - 1) / 2
        coeffs.append(triangles / max_triangles)
    return np.mean(coeffs) if coeffs else 0.0


def main():
    seed = int(os.environ.get("SEED", 42))
    sample_n = int(os.environ.get("SAMPLE_N", SAMPLE_N))
    random.seed(seed)
    np.random.seed(seed)

    db = sqlite3.connect(DB)

    print(f"Loading capsule metadata...")
    rows = db.execute(
        "SELECT id, topic, confidence_score, created_at FROM knowledge_capsules"
    ).fetchall()
    capsule_meta = {r[0]: {"topic": r[1] or "", "conf": r[2] or 0.5, "created_at": r[3]} for r in rows}
    all_ids = list(capsule_meta.keys())
    print(f"  {len(all_ids)} capsules total")

    print(f"Loading embeddings...")
    emb_rows = db.execute("SELECT capsule_id, embedding FROM capsule_embeddings").fetchall()
    embeddings = {}
    for cid, blob in emb_rows:
        if cid in capsule_meta:
            embeddings[cid] = unpack_embedding(blob)
    print(f"  {len(embeddings)} embeddings loaded")

    ids_with_emb = [cid for cid in all_ids if cid in embeddings]
    sample_ids = sorted(random.sample(ids_with_emb, min(sample_n, len(ids_with_emb))))
    print(f"  Sampled {len(sample_ids)} capsules")

    candidate_ids = ids_with_emb
    candidate_matrix = np.stack([embeddings[cid] for cid in candidate_ids])
    candidate_id_to_idx = {cid: i for i, cid in enumerate(candidate_ids)}
    now = time.time()

    adj_current = defaultdict(set)
    adj_uniform = defaultdict(set)
    edges_current = []
    edges_uniform = []

    print(f"Computing neighbors for {len(sample_ids)} capsules...")
    for progress, sid in enumerate(sample_ids):
        if (progress + 1) % 50 == 0 or progress == 0:
            print(f"  {progress + 1}/{len(sample_ids)}")

        vec = embeddings[sid]
        sims = cosine_sim_batch(vec, candidate_matrix)

        meta_s = capsule_meta[sid]
        topic_s = meta_s["topic"]
        conf_s = meta_s["conf"]

        scored_current = []
        scored_uniform = []

        for i, cid in enumerate(candidate_ids):
            if cid == sid:
                continue
            sim = float(sims[i])
            if sim < 0.1:
                continue

            scored_uniform.append((cid, sim))

            meta_c = capsule_meta[cid]
            age_days = max(0, (now - meta_c["created_at"]) / 86400)
            q = compute_quality(topic_s, meta_c["topic"], sim, age_days, conf_s, meta_c["conf"])
            if q >= QUALITY_FLOOR:
                scored_current.append((cid, q))

        scored_current.sort(key=lambda x: -x[1])
        scored_uniform.sort(key=lambda x: -x[1])

        for cid, _ in scored_current[:NEIGHBORS_K]:
            adj_current[sid].add(cid)
            adj_current[cid].add(sid)
            edges_current.append((sid, cid))

        for cid, _ in scored_uniform[:NEIGHBORS_K]:
            adj_uniform[sid].add(cid)
            adj_uniform[cid].add(sid)
            edges_uniform.append((sid, cid))

    db.close()

    # --- Analysis ---
    print(f"\n{'='*60}")
    print(f"  COUNTERFACTUAL GRAPH COMPARISON")
    print(f"  Sample: {len(sample_ids)} capsules, K={NEIGHBORS_K}")
    print(f"{'='*60}\n")

    # Degree distributions
    def degree_stats(adj, label):
        degrees = [len(adj.get(sid, set())) for sid in sample_ids]
        print(f"{label}:")
        print(f"  Edges (directed from sample): {sum(degrees)}")
        print(f"  Mean degree: {np.mean(degrees):.2f}")
        print(f"  Median degree: {np.median(degrees):.1f}")
        print(f"  Std degree: {np.std(degrees):.2f}")
        print(f"  Max degree: {max(degrees)}")
        return degrees

    deg_cur = degree_stats(adj_current, "CURRENT (with boosts)")
    print()
    deg_uni = degree_stats(adj_uniform, "UNIFORM (raw similarity)")

    # Same-topic edge rate
    def same_topic_rate(edges, label):
        same = 0
        same_family = 0
        total = len(edges)
        for a, b in edges:
            ta = capsule_meta[a]["topic"]
            tb = capsule_meta[b]["topic"]
            if ta and tb:
                if ta == tb:
                    same += 1
                if ta.split("/")[0] == tb.split("/")[0]:
                    same_family += 1
        print(f"\n{label}:")
        print(f"  Same-topic edges: {same}/{total} ({100*same/max(total,1):.1f}%)")
        print(f"  Same-family edges: {same_family}/{total} ({100*same_family/max(total,1):.1f}%)")
        return same / max(total, 1), same_family / max(total, 1)

    st_cur, sf_cur = same_topic_rate(edges_current, "CURRENT same-topic rate")
    st_uni, sf_uni = same_topic_rate(edges_uniform, "UNIFORM same-topic rate")

    # Clustering coefficient (on sample subgraph)
    print(f"\nComputing clustering coefficients (sample subgraph)...")
    sub_cur = {sid: adj_current.get(sid, set()) & set(sample_ids) for sid in sample_ids}
    sub_uni = {sid: adj_uniform.get(sid, set()) & set(sample_ids) for sid in sample_ids}
    cc_cur = clustering_coefficient(sub_cur)
    cc_uni = clustering_coefficient(sub_uni)
    print(f"  CURRENT clustering coeff: {cc_cur:.4f}")
    print(f"  UNIFORM clustering coeff: {cc_uni:.4f}")
    print(f"  Ratio (uniform/current): {cc_uni/max(cc_cur,1e-6):.2f}x")

    # Per-family degree comparison
    print(f"\nPer-family mean degree:")
    family_degrees_cur = defaultdict(list)
    family_degrees_uni = defaultdict(list)
    for i, sid in enumerate(sample_ids):
        fam = capsule_meta[sid]["topic"].split("/")[0] if capsule_meta[sid]["topic"] else "none"
        family_degrees_cur[fam].append(deg_cur[i])
        family_degrees_uni[fam].append(deg_uni[i])

    print(f"  {'Family':<20} {'N':>5} {'Current':>10} {'Uniform':>10} {'Δ':>8}")
    print(f"  {'-'*53}")
    for fam in sorted(family_degrees_cur.keys(), key=lambda f: -len(family_degrees_cur[f])):
        n = len(family_degrees_cur[fam])
        if n < 3:
            continue
        mc = np.mean(family_degrees_cur[fam])
        mu = np.mean(family_degrees_uni[fam])
        print(f"  {fam:<20} {n:>5} {mc:>10.1f} {mu:>10.1f} {mu-mc:>+8.1f}")

    # Hermes prediction check
    print(f"\n{'='*60}")
    print(f"  HERMES PREDICTION CHECK")
    print(f"{'='*60}")
    print(f"  Prediction: higher clustering coeff without boosts")
    if cc_uni > cc_cur:
        print(f"  Result: CONFIRMED — {cc_uni:.4f} > {cc_cur:.4f} ({cc_uni/max(cc_cur,1e-6):.2f}x)")
    else:
        print(f"  Result: REJECTED — {cc_uni:.4f} <= {cc_cur:.4f}")
    print(f"  Prediction: same-topic clusters form when penalty removed")
    if st_uni > st_cur:
        print(f"  Result: CONFIRMED — same-topic edges {100*st_uni:.1f}% > {100*st_cur:.1f}%")
    else:
        print(f"  Result: REJECTED — same-topic edges {100*st_uni:.1f}% <= {100*st_cur:.1f}%")

    # Save results
    results = {
        "sample_n": len(sample_ids),
        "seed": seed,
        "neighbors_k": NEIGHBORS_K,
        "current": {
            "mean_degree": float(np.mean(deg_cur)),
            "std_degree": float(np.std(deg_cur)),
            "same_topic_rate": float(st_cur),
            "same_family_rate": float(sf_cur),
            "clustering_coeff": float(cc_cur),
        },
        "uniform": {
            "mean_degree": float(np.mean(deg_uni)),
            "std_degree": float(np.std(deg_uni)),
            "same_topic_rate": float(st_uni),
            "same_family_rate": float(sf_uni),
            "clustering_coeff": float(cc_uni),
        },
        "hermes_prediction_clustering": bool(cc_uni > cc_cur),
        "hermes_prediction_same_topic": bool(st_uni > st_cur),
        "timestamp": int(time.time()),
    }
    out_path = os.path.expanduser("~/chronicle/data/counterfactual_graph_results.json")
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to {out_path}")


if __name__ == "__main__":
    main()
