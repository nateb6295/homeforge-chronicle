#!/usr/bin/env python3
"""Drift directionality analysis: is CCS drift random walk or directed?

If random walk: distance from origin grows as sqrt(n), direction vectors are uncorrelated.
If directed: distance grows linearly, consecutive displacement vectors share direction.

The difference matters: random walk = noise accumulation. Directed = something is pulling.
"""

import json
import os
import sqlite3
import sys
import time
import requests
import numpy as np

DB = "/mnt/hdd/chronicle-data/processed.db"
OLLAMA_URL = "http://192.168.1.11:11434"
EMBED_MODEL = "mxbai-embed-large"


def get_ccs_history():
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT id, snapshot, created_at FROM cognitive_state_history ORDER BY created_at ASC"
    ).fetchall()
    db.close()
    return rows


def serialize_full(snapshot_json):
    ccs = json.loads(snapshot_json)
    parts = []
    for key in ["semantic_gist", "goal_orientation", "predictive_cue"]:
        v = ccs.get(key, "")
        if v:
            parts.append(v[:400])
    fe = ccs.get("focal_entities", [])
    if fe:
        ents = [f"{x.get('name','')} ({x.get('type','')}, {x.get('salience',0):.1f}): {x.get('context','')[:60]}"
                for x in fe[:10]]
        parts.append(" | ".join(ents))
    for key in ["constraints", "episodic_trace"]:
        items = ccs.get(key, [])
        if items:
            parts.append("; ".join(str(x)[:120] for x in items[:5]))
    u = ccs.get("uncertainty_signals", [])
    if u:
        descs = [x.get("description", "")[:80] if isinstance(x, dict) else str(x)[:80] for x in u[:3]]
        parts.append("; ".join(descs))
    return "\n".join(parts)


def get_embedding(text, retries=3):
    for attempt in range(retries):
        try:
            resp = requests.post(
                f"{OLLAMA_URL}/api/embeddings",
                json={"model": EMBED_MODEL, "prompt": text[:800]},
                timeout=120,
            )
            data = resp.json()
            if "embedding" in data:
                return np.array(data["embedding"])
            if attempt < retries - 1:
                time.sleep(2)
        except Exception:
            if attempt < retries - 1:
                time.sleep(2)
    return None


def cosine_sim(a, b):
    dot = np.dot(a, b)
    norm = np.linalg.norm(a) * np.linalg.norm(b)
    if norm == 0:
        return 0.0
    return dot / norm


def main():
    rows = get_ccs_history()
    n = len(rows)
    print(f"CCS history: {n} snapshots")

    embeddings = []
    timestamps = []
    ids = []

    for i, (rid, snap, ts) in enumerate(rows):
        text = serialize_full(snap)
        emb = get_embedding(text)
        if emb is not None:
            embeddings.append(emb)
            timestamps.append(ts)
            ids.append(rid)
        if (i + 1) % 20 == 0:
            print(f"  {i+1}/{n} embedded")

    ne = len(embeddings)
    print(f"\n{ne}/{n} embeddings computed")

    if ne < 10:
        print("Not enough data")
        return

    # 1. Displacement vectors: how the state MOVED each step
    displacements = []
    for i in range(1, ne):
        d = embeddings[i] - embeddings[i - 1]
        displacements.append(d)

    # 2. Direction consistency: cosine similarity between consecutive displacements
    dir_consistencies = []
    for i in range(1, len(displacements)):
        sim = cosine_sim(displacements[i - 1], displacements[i])
        dir_consistencies.append(sim)

    dc = np.array(dir_consistencies)
    print(f"\n{'='*60}")
    print("DIRECTION CONSISTENCY (consecutive displacement similarity)")
    print(f"{'='*60}")
    print(f"  Mean: {dc.mean():.4f}")
    print(f"  Std:  {dc.std():.4f}")
    print(f"  Min:  {dc.min():.4f}")
    print(f"  Max:  {dc.max():.4f}")
    print(f"  % positive (same direction): {(dc > 0).mean():.1%}")
    print(f"  % > 0.1 (meaningfully aligned): {(dc > 0.1).mean():.1%}")

    if dc.mean() > 0.05:
        print(f"  → DIRECTED: mean consistency > 0 (not random walk)")
    else:
        print(f"  → UNDIRECTED: mean consistency near 0 (consistent with random walk)")

    # 3. Drift curve: distance from origin vs sqrt(n) expectation
    drift_from_origin = []
    for i in range(ne):
        d = np.linalg.norm(embeddings[i] - embeddings[0])
        drift_from_origin.append(d)

    drift = np.array(drift_from_origin)
    steps = np.arange(ne)

    # Fit linear and sqrt models
    from numpy.polynomial import polynomial as P

    # Linear fit: drift = a * step + b
    lin_coeffs = np.polyfit(steps[1:], drift[1:], 1)
    lin_pred = np.polyval(lin_coeffs, steps[1:])
    lin_r2 = 1 - np.sum((drift[1:] - lin_pred) ** 2) / np.sum((drift[1:] - drift[1:].mean()) ** 2)

    # Sqrt fit: drift = a * sqrt(step) + b
    sqrt_steps = np.sqrt(steps[1:])
    sqrt_coeffs = np.polyfit(sqrt_steps, drift[1:], 1)
    sqrt_pred = np.polyval(sqrt_coeffs, sqrt_steps)
    sqrt_r2 = 1 - np.sum((drift[1:] - sqrt_pred) ** 2) / np.sum((drift[1:] - drift[1:].mean()) ** 2)

    print(f"\n{'='*60}")
    print("DRIFT CURVE MODEL FIT")
    print(f"{'='*60}")
    print(f"  Linear (directed):     R² = {lin_r2:.4f}")
    print(f"  Sqrt (random walk):    R² = {sqrt_r2:.4f}")
    if lin_r2 > sqrt_r2 + 0.02:
        print(f"  → LINEAR WINS: drift is directed, not random walk")
    elif sqrt_r2 > lin_r2 + 0.02:
        print(f"  → SQRT WINS: drift is random walk")
    else:
        print(f"  → INDISTINGUISHABLE at current sample size")

    # 4. Net displacement: where did it end up relative to start?
    net = embeddings[-1] - embeddings[0]
    net_magnitude = np.linalg.norm(net)
    total_path = sum(np.linalg.norm(d) for d in displacements)
    efficiency = net_magnitude / total_path if total_path > 0 else 0

    print(f"\n{'='*60}")
    print("PATH EFFICIENCY (directed = high, random = low)")
    print(f"{'='*60}")
    print(f"  Net displacement: {net_magnitude:.4f}")
    print(f"  Total path length: {total_path:.4f}")
    print(f"  Efficiency (net/total): {efficiency:.4f}")
    print(f"  Random walk expectation: ~{1/np.sqrt(ne):.4f}")
    if efficiency > 2 / np.sqrt(ne):
        print(f"  → MORE DIRECTED than random walk")
    else:
        print(f"  → CONSISTENT with random walk")

    # 5. Windowed direction: does late drift point the same way as early drift?
    mid = ne // 2
    early_net = embeddings[mid] - embeddings[0]
    late_net = embeddings[-1] - embeddings[mid]
    phase_sim = cosine_sim(early_net, late_net)
    print(f"\n  Early-half vs late-half direction similarity: {phase_sim:.4f}")
    if phase_sim > 0.3:
        print(f"  → PERSISTENT DIRECTION across full timespan")
    elif phase_sim > 0:
        print(f"  → WEAKLY PERSISTENT direction")
    else:
        print(f"  → DIRECTION REVERSED between halves")

    output = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "n_embeddings": ne,
        "direction_consistency": {
            "mean": float(dc.mean()),
            "std": float(dc.std()),
            "pct_positive": float((dc > 0).mean()),
            "pct_aligned": float((dc > 0.1).mean()),
        },
        "drift_curve": {
            "linear_r2": float(lin_r2),
            "sqrt_r2": float(sqrt_r2),
            "verdict": "linear" if lin_r2 > sqrt_r2 + 0.02 else "sqrt" if sqrt_r2 > lin_r2 + 0.02 else "indistinguishable",
        },
        "path_efficiency": {
            "net_displacement": float(net_magnitude),
            "total_path": float(total_path),
            "efficiency": float(efficiency),
            "random_expectation": float(1 / np.sqrt(ne)),
        },
        "phase_similarity": float(phase_sim),
        "per_step_direction_consistency": [float(x) for x in dir_consistencies],
    }
    outpath = os.path.expanduser("~/chronicle/data/drift_directionality.json")
    with open(outpath, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nResults saved: {outpath}")


if __name__ == "__main__":
    main()
