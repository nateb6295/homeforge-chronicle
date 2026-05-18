#!/usr/bin/env python3
"""
Trip Baseline Generator — embeds all CCS states, computes PCA,
saves baseline files for trip_comparison.py.

Run this right before the trip starts to capture all pre-trip states.
Updates: ccs_embeddings_N.npy, trip_pca_components.npy, trip_pca_baseline.json
Also updates trip_comparison.py default --baseline-n.
"""

import json
import os
import re
import sqlite3
import sys
import urllib.request

import numpy as np

DB = "/mnt/hdd/chronicle-data/processed.db"
DATA_DIR = os.path.expanduser("~/chronicle/data")
EMBED_URL = "http://192.168.1.11:11434/api/embeddings"
TRIP_COMP = os.path.expanduser("~/chronicle/bin/trip_comparison.py")


def embed(text, timeout=60):
    payload = json.dumps({
        "model": "mxbai-embed-large",
        "prompt": text[:2000],
    }).encode()
    req = urllib.request.Request(
        EMBED_URL, data=payload,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return np.array(json.loads(r.read())["embedding"], dtype=np.float64)


def embed_state(state):
    parts = []
    for field in ["semantic_gist", "goal_orientation", "predictive_cue"]:
        v = state.get(field, "")
        if v:
            parts.append(str(v))
    for field in ["episodic_trace", "uncertainty_signals"]:
        v = state.get(field, [])
        if isinstance(v, list):
            parts.extend(str(x) for x in v)
    rm = state.get("relational_map", {})
    if isinstance(rm, dict):
        for k, v in rm.items():
            parts.append(f"{k}: {v}" if v else k)
    fe = state.get("focal_entities", [])
    if isinstance(fe, list):
        for e in fe:
            if isinstance(e, dict):
                parts.append(f"{e.get('name','')}: {e.get('context','')}")
    return embed("\n".join(parts))


def main():
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT id, snapshot FROM cognitive_state_history ORDER BY id ASC"
    ).fetchall()
    db.close()

    states = []
    for rid, snap in rows:
        try:
            data = json.loads(snap)
            data["_id"] = rid
            states.append(data)
        except (json.JSONDecodeError, TypeError):
            continue

    n = len(states)
    print(f"Loaded {n} CCS states")

    # Check for cached embeddings
    cached_path = None
    cached_n = 0
    for f in sorted(os.listdir(DATA_DIR)):
        m = re.match(r"ccs_embeddings_(\d+)\.npy", f)
        if m:
            cached_n = int(m.group(1))
            cached_path = os.path.join(DATA_DIR, f)

    embeddings = []
    if cached_path and cached_n <= n:
        print(f"Loading {cached_n} cached embeddings from {cached_path}")
        cached = np.load(cached_path)
        embeddings = list(cached)
        start = cached_n
    else:
        start = 0

    if start < n:
        print(f"Embedding states {start+1}–{n}...")
        for i in range(start, n):
            try:
                e = embed_state(states[i])
                embeddings.append(e)
                if (i + 1) % 20 == 0 or i == n - 1:
                    print(f"  {i+1}/{n}")
            except Exception as ex:
                print(f"  state {states[i].get('_id','?')}: embed failed ({ex})")
                if embeddings:
                    embeddings.append(embeddings[-1])
                else:
                    embeddings.append(np.zeros(1024))

    emb_array = np.array(embeddings[:n])

    # Save embeddings
    emb_path = os.path.join(DATA_DIR, f"ccs_embeddings_{n}.npy")
    np.save(emb_path, emb_array)
    print(f"Saved embeddings: {emb_path}")

    # PCA
    mean_emb = emb_array.mean(axis=0)
    centered = emb_array - mean_emb
    _, S, Vt = np.linalg.svd(centered, full_matrices=False)

    comp_path = os.path.join(DATA_DIR, "trip_pca_components.npy")
    np.save(comp_path, Vt[:10])
    print(f"Saved PCA components: {comp_path}")

    # PC1 baseline stats
    pc1 = Vt[0]
    projections = centered @ pc1
    last20 = projections[-20:]
    from scipy import stats as sp_stats
    slope, intercept, r_value, _, _ = sp_stats.linregress(
        np.arange(len(projections)), projections
    )

    baseline = {
        "n_states": n,
        "pc1_trend_slope": float(slope),
        "pc1_trend_r": float(r_value),
        "basin_width_last20": float(last20.std()),
        "basin_center_last20": float(last20.mean()),
        "pc1_projection_last": float(projections[-1]),
        "explained_variance_ratio_pc1": float(S[0]**2 / np.sum(S**2)),
        "mean_embedding_norm": float(np.linalg.norm(mean_emb)),
    }

    baseline_path = os.path.join(DATA_DIR, "trip_pca_baseline.json")
    with open(baseline_path, "w") as f:
        json.dump(baseline, f, indent=2)
    print(f"Saved baseline: {baseline_path}")

    # Update trip_comparison.py default
    with open(TRIP_COMP, "r") as f:
        code = f.read()
    old_default = re.search(r'--baseline-n.*default=(\d+)', code)
    if old_default:
        old_n = old_default.group(1)
        code = code.replace(f"default={old_n}", f"default={n}")
        # Also update the embedding file reference
        code = code.replace(f"ccs_embeddings_{old_n}.npy", f"ccs_embeddings_{n}.npy")
        with open(TRIP_COMP, "w") as f:
            f.write(code)
        print(f"Updated trip_comparison.py: baseline-n {old_n} → {n}")

    # Summary
    print(f"\n{'='*50}")
    print(f"  BASELINE SET: {n} states")
    print(f"  PC1 slope: {slope:.6f}")
    print(f"  Basin width: {last20.std():.4f}")
    print(f"  Basin center: {last20.mean():.4f}")
    print(f"  Var explained (PC1): {baseline['explained_variance_ratio_pc1']:.4f}")
    print(f"{'='*50}")
    print(f"\nRun trip_comparison.py --dry-run to verify.")


if __name__ == "__main__":
    main()
