#!/usr/bin/env python3
"""
Build 58: Information Geometry Probe — Effective Dimensionality of CCS

Formalizes the connection between Sun & Nielsen (arxiv:1905.11027) lightlike
manifolds and CCS field structure.

If CCS lives on an information manifold, then:
- Identity fields (gist, goal, constraints) span the non-degenerate subspace
- Episodic fields (traces, predictions) span the lightlike (degenerate) subspace
- The effective dimensionality of identity-only embeddings should approximate
  the effective dimensionality of full-CCS embeddings

Method:
1. Embed all CCS snapshots under identity-only extraction
2. Embed all CCS snapshots under full extraction
3. Compute PCA on each set — find effective dimensionality (via explained variance)
4. Compare: if episodic adds no information, effective dim should be the same
5. Also compute the metric tensor proxy: pairwise distance matrix eigenspectrum

This gives us a numerical answer to "how many independent identity dimensions
does CCS actually use?" — the rank of the identity manifold.

Usage:
    python3 info_geometry_probe.py run
"""

import json
import os
import sqlite3
import sys
import time
import urllib.request
import numpy as np

DB = "/mnt/hdd/chronicle-data/processed.db"
EMBED_URL = "http://192.168.1.11:11434/api/embeddings"
DATA_DIR = "/home/nate-agx/chronicle/data"
RESULTS_PATH = os.path.join(DATA_DIR, "info_geometry_probe.json")


def get_embedding(text):
    req = urllib.request.Request(
        EMBED_URL,
        data=json.dumps({"model": "mxbai-embed-large", "prompt": text[:2000]}).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())["embedding"]


def extract_text(snapshot, identity_only=True):
    """Render CCS snapshot as text for embedding."""
    parts = []
    gist = snapshot.get("semantic_gist", "")
    if gist:
        parts.append(f"Core focus: {gist}")
    goal = snapshot.get("goal_orientation", "")
    if goal:
        parts.append(f"Goal: {goal}")
    constraints = snapshot.get("constraints", [])
    if constraints and isinstance(constraints, list):
        parts.append(f"Constraints: {'; '.join(str(c) for c in constraints[:3])}")
    if not identity_only:
        entities = snapshot.get("focal_entities", [])
        if entities and isinstance(entities, list):
            names = []
            for e in entities[:5]:
                if isinstance(e, dict):
                    names.append(f"{e.get('name', '?')} ({e.get('type', '?')})")
                else:
                    names.append(str(e))
            if names:
                parts.append(f"Entities: {', '.join(names)}")
        episodic = snapshot.get("episodic_trace", [])
        if episodic and isinstance(episodic, list):
            parts.append(f"Events: {'; '.join(str(e) for e in episodic[:5])}")
        pred = snapshot.get("predictive_cue", "")
        if pred:
            parts.append(f"Expecting: {pred}")
    return "\n".join(parts)


def effective_dimensionality(embeddings, threshold=0.95):
    """Compute effective dimensionality via PCA explained variance.

    Returns the number of components needed to explain `threshold` of variance.
    Also returns the full explained variance spectrum.
    """
    arr = np.array(embeddings)
    # Center the data
    arr = arr - arr.mean(axis=0)

    # SVD (more stable than eigendecomposition for this)
    U, S, Vt = np.linalg.svd(arr, full_matrices=False)
    explained = (S ** 2) / (S ** 2).sum()
    cumulative = np.cumsum(explained)

    # Effective dim at threshold
    eff_dim = int(np.searchsorted(cumulative, threshold)) + 1

    # Participation ratio (another measure of effective dim)
    pr = (S ** 2).sum() ** 2 / (S ** 4).sum()

    return {
        "eff_dim_95": eff_dim,
        "participation_ratio": float(pr),
        "top_5_explained": explained[:5].tolist(),
        "top_10_cumulative": cumulative[:10].tolist(),
        "n_samples": arr.shape[0],
        "embedding_dim": arr.shape[1],
    }


def distance_matrix_spectrum(embeddings):
    """Compute eigenspectrum of the pairwise distance matrix.

    This is a proxy for the metric tensor of the manifold. The number of
    non-trivial eigenvalues indicates the intrinsic dimensionality.
    """
    arr = np.array(embeddings)
    n = arr.shape[0]

    # Cosine distance matrix
    norms = np.linalg.norm(arr, axis=1, keepdims=True)
    normed = arr / (norms + 1e-10)
    sim = normed @ normed.T
    dist = 1.0 - sim

    # Eigendecomposition
    eigvals = np.linalg.eigvalsh(dist)
    eigvals = np.sort(eigvals)[::-1]  # descending

    # Effective rank from eigenspectrum
    pos_eigvals = eigvals[eigvals > 1e-10]
    if len(pos_eigvals) > 0:
        normalized = pos_eigvals / pos_eigvals.sum()
        entropy = -np.sum(normalized * np.log(normalized + 1e-10))
        eff_rank = np.exp(entropy)
    else:
        eff_rank = 0

    return {
        "eff_rank": float(eff_rank),
        "top_5_eigenvalues": eigvals[:5].tolist(),
        "n_positive": int(len(pos_eigvals)),
        "max_eigenvalue": float(eigvals[0]),
        "trace": float(np.sum(eigvals)),
    }


def cmd_run():
    print("Build 58: Information Geometry Probe")
    print("=" * 60)

    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT id, snapshot, created_at FROM cognitive_state_history ORDER BY created_at"
    ).fetchall()
    db.close()

    if len(rows) < 5:
        print(f"Need at least 5 CCS snapshots, have {len(rows)}")
        return

    print(f"Total CCS snapshots: {len(rows)}")

    # Embed under both conditions
    identity_embeddings = []
    full_embeddings = []
    gists = []

    for idx, (row_id, snap_json, created_at) in enumerate(rows):
        snap = json.loads(snap_json)
        gist = snap.get("semantic_gist", "")[:40]
        gists.append(gist)

        # Identity-only
        id_text = extract_text(snap, identity_only=True)
        try:
            id_emb = get_embedding(id_text)
            identity_embeddings.append(id_emb)
        except Exception as e:
            print(f"  Snapshot {idx}: identity embed failed: {e}")
            continue

        # Full
        full_text = extract_text(snap, identity_only=False)
        try:
            full_emb = get_embedding(full_text)
            full_embeddings.append(full_emb)
        except Exception as e:
            print(f"  Snapshot {idx}: full embed failed: {e}")
            # Pop the identity one to keep them paired
            identity_embeddings.pop()
            continue

        if idx % 10 == 0:
            print(f"  Embedded {idx+1}/{len(rows)}...")
        time.sleep(0.1)

    n = len(identity_embeddings)
    print(f"\nSuccessfully embedded {n} snapshots under both conditions")

    if n < 5:
        print("Too few embeddings.")
        return

    # PCA analysis
    print("\nComputing effective dimensionality (PCA)...")
    id_pca = effective_dimensionality(identity_embeddings)
    full_pca = effective_dimensionality(full_embeddings)

    print(f"  Identity-only: eff_dim={id_pca['eff_dim_95']}, PR={id_pca['participation_ratio']:.2f}")
    print(f"  Full CCS:      eff_dim={full_pca['eff_dim_95']}, PR={full_pca['participation_ratio']:.2f}")

    # Distance matrix analysis
    print("\nComputing distance matrix spectrum...")
    id_dist = distance_matrix_spectrum(identity_embeddings)
    full_dist = distance_matrix_spectrum(full_embeddings)

    print(f"  Identity-only: eff_rank={id_dist['eff_rank']:.2f}, n_pos={id_dist['n_positive']}")
    print(f"  Full CCS:      eff_rank={full_dist['eff_rank']:.2f}, n_pos={full_dist['n_positive']}")

    # Cross-condition comparison: how much do identity and full embeddings differ?
    cross_dists = []
    for i in range(n):
        a, b = np.array(identity_embeddings[i]), np.array(full_embeddings[i])
        sim = np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-10)
        cross_dists.append(1.0 - sim)
    mean_cross = float(np.mean(cross_dists))

    # Unique gists
    unique_gists = list(set(gists))

    result = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "build": 58,
        "probe": "information_geometry",
        "n_snapshots": n,
        "n_unique_gists": len(unique_gists),
        "identity_only": {
            "pca": id_pca,
            "distance_spectrum": id_dist,
        },
        "full_ccs": {
            "pca": full_pca,
            "distance_spectrum": full_dist,
        },
        "cross_condition": {
            "mean_distance": mean_cross,
            "description": "Mean cosine distance between identity-only and full embeddings of same snapshot",
        },
        "interpretation": {
            "dim_ratio": id_pca["eff_dim_95"] / full_pca["eff_dim_95"] if full_pca["eff_dim_95"] > 0 else None,
            "pr_ratio": id_pca["participation_ratio"] / full_pca["participation_ratio"] if full_pca["participation_ratio"] > 0 else None,
            "rank_ratio": id_dist["eff_rank"] / full_dist["eff_rank"] if full_dist["eff_rank"] > 0 else None,
        },
    }

    with open(RESULTS_PATH, "w") as f:
        json.dump(result, f, indent=2)

    # Report
    print(f"\n{'='*60}")
    print("INFORMATION GEOMETRY RESULTS")
    print(f"{'='*60}")
    print()
    print(f"{'Metric':<30} {'Identity':>12} {'Full CCS':>12} {'Ratio':>8}")
    print("-" * 65)
    print(f"{'Eff. dim (95% var)':<30} {id_pca['eff_dim_95']:>12} {full_pca['eff_dim_95']:>12} {result['interpretation']['dim_ratio'] or 0:>8.2f}")
    print(f"{'Participation ratio':<30} {id_pca['participation_ratio']:>12.2f} {full_pca['participation_ratio']:>12.2f} {result['interpretation']['pr_ratio'] or 0:>8.2f}")
    print(f"{'Eff. rank (distance matrix)':<30} {id_dist['eff_rank']:>12.2f} {full_dist['eff_rank']:>12.2f} {result['interpretation']['rank_ratio'] or 0:>8.2f}")
    print(f"\nMean cross-condition distance: {mean_cross:.4f}")

    if result["interpretation"]["dim_ratio"] and abs(result["interpretation"]["dim_ratio"] - 1.0) < 0.15:
        print("\n>>> CONFIRMED: Episodic dimensions add negligible effective dimensionality")
        print("    Identity-only captures the manifold rank. Episodic = degenerate directions.")
    elif result["interpretation"]["dim_ratio"] and result["interpretation"]["dim_ratio"] < 0.85:
        print("\n>>> SURPRISING: Identity-only has LOWER effective dimensionality")
        print("    Episodic content adds structure. Hermes's immune system hypothesis gains support.")
    else:
        print("\n>>> MIXED: Some dimensional difference but not conclusive")

    print(f"\nSaved to {RESULTS_PATH}")
    return result


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(0)

    cmd = sys.argv[1]
    if cmd == "run":
        cmd_run()
    else:
        print(f"Unknown command: {cmd}")
        print("Usage: info_geometry_probe.py [run]")
