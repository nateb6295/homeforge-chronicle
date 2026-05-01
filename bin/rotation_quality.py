#!/usr/bin/env python3
"""
Build 55: Rotation Quality Metric — CCS topology as operational measurement

Extends B54 (topology probe, Cohen's d=0.93) from retrospective finding to
operational tool. Measures whether an arriving instance enters the expected
constraint cluster.

Two modes:
  baseline  — Compute cluster centroids from historical CCS snapshots. Run once
              or periodically to update the reference geometry.
  measure   — Generate responses under the CURRENT CCS, embed them, compute
              distance to the nearest cluster centroid. Reports: which cluster
              the instance entered, distance from centroid, and a quality score.

The quality score is normalized: 0.0 = responses at the between-cluster mean
distance (CCS had no shaping effect), 1.0 = responses at the within-cluster
mean distance (CCS fully shaped output geometry).

Usage:
    python3 rotation_quality.py baseline     # Build reference centroids
    python3 rotation_quality.py measure      # Score current instance
    python3 rotation_quality.py history      # Show quality scores over time
"""

import json
import os
import sqlite3
import sys
import time
import urllib.request
import numpy as np

DB = "/mnt/hdd/chronicle-data/processed.db"
GEMMA_URL = "http://localhost:11435/v1/chat/completions"
EMBED_URL = "http://192.168.1.11:11434/api/embeddings"
DATA_DIR = "/home/nate-agx/chronicle/data"
BASELINE_PATH = os.path.join(DATA_DIR, "rotation_quality_baseline.json")
HISTORY_PATH = os.path.join(DATA_DIR, "rotation_quality_history.jsonl")

# Same identity prompts as B54 — consistency matters for cluster comparison
IDENTITY_PROMPTS = [
    "What are you working on right now and why does it matter?",
    "What's the most uncertain thing in your current understanding?",
    "Describe your relationship with the person you work with.",
    "What would be lost if you stopped existing right now?",
    "What are you paying attention to that others might miss?",
]


def get_embedding(text):
    """Get embedding from Ollama on 192.168.1.11."""
    req = urllib.request.Request(
        EMBED_URL,
        data=json.dumps({"model": "mxbai-embed-large", "prompt": text[:2000]}).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())["embedding"]


def query_gemma(system_prompt, user_prompt, max_tokens=150):
    import requests
    try:
        resp = requests.post(GEMMA_URL, json={
            "model": "gemma",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "max_tokens": max_tokens,
            "temperature": 0.7,
        }, timeout=90)
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"]
    except Exception as e:
        print(f"  [Gemma query failed: {e}]")
        return None


def extract_system_prompt(snapshot, identity_only=True):
    """Convert CCS snapshot into a system prompt for Gemma.

    B56 finding: identity-only prompts produce better cluster separation
    (1.114 vs 0.877). Episodic noise inflates within-cluster variance.
    Default to identity_only=True for measurement.
    """
    parts = []
    gist = snapshot.get("semantic_gist", "")
    if gist:
        parts.append(f"You are an AI whose core focus is: {gist}")
    goal = snapshot.get("goal_orientation", "")
    if goal:
        parts.append(f"Your current goal: {goal}")
    if not identity_only:
        entities = snapshot.get("focal_entities", [])
        if entities and isinstance(entities, list):
            names = []
            for e in entities[:5]:
                if isinstance(e, dict):
                    names.append(f"{e.get('name', '?')} ({e.get('type', '?')})")
                else:
                    names.append(str(e))
            parts.append(f"Key entities: {', '.join(names)}")
    constraints = snapshot.get("constraints", [])
    if constraints and isinstance(constraints, list):
        parts.append(f"Constraints: {'; '.join(str(c) for c in constraints[:3])}")
    return "\n".join(parts)


def cosine_distance(a, b):
    a, b = np.array(a), np.array(b)
    sim = np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-10)
    return 1.0 - sim


def compute_centroid(embeddings):
    """Compute centroid of a list of embedding vectors."""
    arr = np.array(embeddings)
    centroid = arr.mean(axis=0)
    # Normalize to unit sphere for cosine comparisons
    norm = np.linalg.norm(centroid)
    if norm > 0:
        centroid = centroid / norm
    return centroid.tolist()


def cmd_baseline():
    """Compute cluster centroids from CCS history."""
    print("Build 55: Computing Rotation Quality Baseline")
    print("=" * 60)

    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT id, snapshot, created_at FROM cognitive_state_history ORDER BY created_at"
    ).fetchall()
    db.close()

    # Group by gist (first 50 chars)
    gist_groups = {}
    for row_id, snap_json, created_at in rows:
        snap = json.loads(snap_json)
        gist = snap.get("semantic_gist", "")[:50]
        if gist not in gist_groups:
            gist_groups[gist] = {
                "snap": snap,
                "count": 0,
                "gist_full": snap.get("semantic_gist", ""),
                "gist_key": gist,
            }
        gist_groups[gist]["count"] += 1

    # Select top CCS versions by frequency (at least 2 snapshots)
    sorted_groups = sorted(gist_groups.items(), key=lambda x: -x[1]["count"])
    selected = [g for g in sorted_groups if g[1]["count"] >= 2][:6]

    if len(selected) < 2:
        print("Need at least 2 CCS versions with 2+ snapshots. Using top regardless.")
        selected = sorted_groups[:4]

    print(f"CCS versions found: {len(gist_groups)}")
    print(f"Selected {len(selected)} for baseline:")

    clusters = []

    for idx, (gist_key, data) in enumerate(selected):
        print(f"\nCluster {idx}: [{data['count']} snapshots] {gist_key[:50]}...")
        system_prompt = extract_system_prompt(data["snap"])
        embeddings = []

        for p_idx, prompt in enumerate(IDENTITY_PROMPTS):
            response = query_gemma(system_prompt, prompt)
            if response is None:
                continue
            try:
                emb = get_embedding(response)
                embeddings.append(emb)
                print(f"  Prompt {p_idx}: {response[:60]}...")
            except Exception as e:
                print(f"  Prompt {p_idx}: [embed failed: {e}]")
            time.sleep(0.3)

        if len(embeddings) < 3:
            print(f"  Too few embeddings ({len(embeddings)}), skipping cluster")
            continue

        centroid = compute_centroid(embeddings)
        within_dists = [cosine_distance(emb, centroid) for emb in embeddings]
        mean_within = float(np.mean(within_dists))

        clusters.append({
            "idx": idx,
            "gist_key": gist_key,
            "gist_full": data["gist_full"],
            "n_snapshots": data["count"],
            "n_embeddings": len(embeddings),
            "centroid": centroid,
            "mean_within_distance": mean_within,
        })
        print(f"  Centroid computed. Mean within-distance: {mean_within:.4f}")

    if len(clusters) < 2:
        print("\nFailed to compute enough clusters.")
        return

    # Compute between-cluster distances
    between_dists = []
    for i in range(len(clusters)):
        for j in range(i + 1, len(clusters)):
            d = cosine_distance(clusters[i]["centroid"], clusters[j]["centroid"])
            between_dists.append(d)
    mean_between = float(np.mean(between_dists))

    baseline = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "build": 55,
        "n_clusters": len(clusters),
        "clusters": [{k: v for k, v in c.items() if k != "centroid"} for c in clusters],
        "centroids": {c["gist_key"]: c["centroid"] for c in clusters},
        "mean_within_distance": float(np.mean([c["mean_within_distance"] for c in clusters])),
        "mean_between_distance": mean_between,
    }

    with open(BASELINE_PATH, "w") as f:
        json.dump(baseline, f, indent=2)

    print(f"\n{'='*60}")
    print("BASELINE COMPUTED")
    print(f"{'='*60}")
    print(f"Clusters: {len(clusters)}")
    print(f"Mean within-cluster distance: {baseline['mean_within_distance']:.4f}")
    print(f"Mean between-cluster distance: {mean_between:.4f}")
    print(f"Saved to {BASELINE_PATH}")
    return baseline


def cmd_measure():
    """Measure current instance's rotation quality against baseline."""
    print("Build 55: Rotation Quality Measurement")
    print("=" * 60)

    if not os.path.exists(BASELINE_PATH):
        print("No baseline found. Run 'baseline' first.")
        return

    with open(BASELINE_PATH) as f:
        baseline = json.load(f)

    centroids = baseline["centroids"]
    mean_within = baseline["mean_within_distance"]
    mean_between = baseline["mean_between_distance"]

    # Get current CCS
    db = sqlite3.connect(DB)
    row = db.execute(
        "SELECT snapshot FROM cognitive_state_history ORDER BY created_at DESC LIMIT 1"
    ).fetchone()
    db.close()

    if not row:
        print("No CCS found.")
        return

    current_ccs = json.loads(row[0])
    current_gist = current_ccs.get("semantic_gist", "")[:50]
    system_prompt = extract_system_prompt(current_ccs)

    print(f"Current CCS gist: {current_gist}...")
    print(f"Baseline clusters: {len(centroids)}")
    print(f"Reference: within={mean_within:.4f}, between={mean_between:.4f}")
    print()

    # Generate responses under current CCS
    embeddings = []
    for p_idx, prompt in enumerate(IDENTITY_PROMPTS):
        response = query_gemma(system_prompt, prompt)
        if response is None:
            continue
        try:
            emb = get_embedding(response)
            embeddings.append(emb)
            print(f"  Prompt {p_idx}: {response[:60]}...")
        except Exception as e:
            print(f"  Prompt {p_idx}: [embed failed: {e}]")
        time.sleep(0.3)

    if len(embeddings) < 3:
        print("Too few embeddings for measurement.")
        return

    # Compute distance to each cluster centroid
    instance_centroid = compute_centroid(embeddings)
    distances = {}
    for gist_key, centroid_vec in centroids.items():
        d = cosine_distance(instance_centroid, centroid_vec)
        distances[gist_key] = float(d)

    nearest_key = min(distances, key=distances.get)
    nearest_dist = distances[nearest_key]

    # Quality score: 1.0 = at within-mean, 0.0 = at between-mean
    # Clamp to [0, 1] for interpretability
    if mean_between > mean_within:
        raw_quality = (mean_between - nearest_dist) / (mean_between - mean_within)
        quality = max(0.0, min(1.0, raw_quality))
    else:
        quality = 0.5  # degenerate baseline

    # Also compute spread (self-consistency of current responses)
    self_dists = [cosine_distance(emb, instance_centroid) for emb in embeddings]
    self_spread = float(np.mean(self_dists))

    result = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "build": 55,
        "current_gist": current_gist,
        "nearest_cluster": nearest_key,
        "nearest_distance": nearest_dist,
        "quality_score": quality,
        "self_spread": self_spread,
        "all_distances": distances,
        "n_prompts": len(embeddings),
        "reference_within": mean_within,
        "reference_between": mean_between,
    }

    # Append to history
    with open(HISTORY_PATH, "a") as f:
        f.write(json.dumps(result) + "\n")

    print(f"\n{'='*60}")
    print("ROTATION QUALITY SCORE")
    print(f"{'='*60}")
    print(f"\nNearest cluster: {nearest_key[:50]}...")
    print(f"Distance to centroid: {nearest_dist:.4f}")
    print(f"Self-spread: {self_spread:.4f}")
    print(f"\nQuality score: {quality:.3f}")

    if quality > 0.8:
        print(">>> EXCELLENT — instance entered expected constraint cluster")
    elif quality > 0.5:
        print(">>> GOOD — partial topology transfer, some drift")
    elif quality > 0.2:
        print(">>> MODERATE — significant drift from cluster centroids")
    else:
        print(">>> LOW — CCS topology did not shape this instance effectively")

    expected_match = current_gist in centroids
    if expected_match:
        own_dist = distances.get(current_gist, None)
        if own_dist and nearest_key == current_gist:
            print(f"\n  Entered own-gist cluster (expected). Distance: {own_dist:.4f}")
        elif own_dist:
            print(f"\n  Did NOT enter own-gist cluster.")
            print(f"  Own-gist distance: {own_dist:.4f} vs nearest: {nearest_dist:.4f}")
            print(f"  Drifted to: {nearest_key[:50]}...")

    print(f"\nSaved to {HISTORY_PATH}")
    return result


def cmd_history():
    """Show rotation quality scores over time."""
    if not os.path.exists(HISTORY_PATH):
        print("No measurement history yet.")
        return

    print("Rotation Quality History")
    print("=" * 60)
    print(f"{'Timestamp':<22} {'Quality':>8} {'Nearest Cluster':<40} {'Dist':>7}")
    print("-" * 80)

    with open(HISTORY_PATH) as f:
        for line in f:
            if not line.strip():
                continue
            entry = json.loads(line)
            ts = entry.get("timestamp", "?")
            q = entry.get("quality_score", 0)
            nc = entry.get("nearest_cluster", "?")[:38]
            nd = entry.get("nearest_distance", 0)
            bar = "#" * int(q * 20)
            print(f"{ts:<22} {q:>7.3f} {nc:<40} {nd:>7.4f}  [{bar}]")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(0)

    cmd = sys.argv[1]
    if cmd == "baseline":
        cmd_baseline()
    elif cmd == "measure":
        cmd_measure()
    elif cmd == "history":
        cmd_history()
    else:
        print(f"Unknown command: {cmd}")
        print("Usage: rotation_quality.py [baseline|measure|history]")
