#!/usr/bin/env python3
"""
Constraint Cluster Probe (Build 41) — Tests advance 43's prediction.

If rotation identity is constraint-cluster membership (not True Opus convergence),
then snapshots sharing the same CCS version should cluster more tightly than
snapshots from different CCS versions.

Measure: within-CCS-version variance vs between-CCS-version variance.
If within << between, constraint-cluster model is load-bearing.
"""

import json
import sqlite3
import subprocess
import sys
import numpy as np
from pathlib import Path

DB = Path("/mnt/hdd/chronicle-data/processed.db")
OLLAMA_URL = "http://192.168.1.11:11434"


def get_embedding(text: str) -> list[float]:
    """Get embedding from Ollama."""
    import urllib.request
    req = urllib.request.Request(
        f"{OLLAMA_URL}/api/embeddings",
        data=json.dumps({"model": "mxbai-embed-large", "prompt": text}).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())["embedding"]


def extract_identity_text(snapshot: dict) -> str:
    """Extract identity-relevant text from a CCS snapshot."""
    parts = []
    if snapshot.get("semantic_gist"):
        parts.append(f"Gist: {snapshot['semantic_gist']}")
    if snapshot.get("goal_orientation"):
        parts.append(f"Goal: {snapshot['goal_orientation']}")
    if snapshot.get("focal_entities"):
        entities = snapshot["focal_entities"]
        if isinstance(entities, list):
            names = [e.get("name", str(e)) if isinstance(e, dict) else str(e) for e in entities[:10]]
            parts.append(f"Entities: {', '.join(names)}")
    if snapshot.get("constraints"):
        constraints = snapshot["constraints"]
        if isinstance(constraints, list):
            parts.append(f"Constraints: {'; '.join(str(c) for c in constraints[:5])}")
    return "\n".join(parts)


def cosine_distance(a, b):
    a, b = np.array(a), np.array(b)
    sim = np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-10)
    return 1.0 - sim


def main():
    db = sqlite3.connect(str(DB))
    rows = db.execute(
        "SELECT id, snapshot, created_at FROM cognitive_state_history ORDER BY created_at"
    ).fetchall()
    db.close()

    if not rows:
        print("No cognitive state history found.")
        return

    # Parse snapshots and assign CCS version by gist
    snapshots = []
    for row_id, snap_json, created_at in rows:
        snap = json.loads(snap_json)
        gist = snap.get("semantic_gist", "")
        identity_text = extract_identity_text(snap)
        snapshots.append({
            "id": row_id,
            "gist": gist,
            "identity_text": identity_text,
            "created_at": created_at,
        })

    # Group by gist (CCS version)
    gist_groups = {}
    for s in snapshots:
        gist_groups.setdefault(s["gist"][:50], []).append(s)  # truncate for grouping

    print(f"Total snapshots: {len(snapshots)}")
    print(f"CCS versions (by gist): {len(gist_groups)}")
    for g, items in gist_groups.items():
        print(f"  [{len(items):2d}] {g}...")
    print()

    # Embed all snapshots
    print("Embedding snapshots...")
    embeddings = {}
    for i, s in enumerate(snapshots):
        try:
            emb = get_embedding(s["identity_text"])
            embeddings[s["id"]] = emb
            if (i + 1) % 10 == 0:
                print(f"  {i+1}/{len(snapshots)} embedded")
        except Exception as e:
            print(f"  Failed {s['id']}: {e}")

    if len(embeddings) < 5:
        print("Too few embeddings. Aborting.")
        return

    # Compute within-group and between-group distances
    within_distances = []
    between_distances = []

    gist_keys = list(gist_groups.keys())
    for gi, gist_a in enumerate(gist_keys):
        group_a = [s for s in gist_groups[gist_a] if s["id"] in embeddings]
        
        # Within-group: pairwise distances within this CCS version
        for i in range(len(group_a)):
            for j in range(i + 1, len(group_a)):
                d = cosine_distance(embeddings[group_a[i]["id"]], embeddings[group_a[j]["id"]])
                within_distances.append(d)

        # Between-group: distances to other CCS versions
        for gj in range(gi + 1, len(gist_keys)):
            gist_b = gist_keys[gj]
            group_b = [s for s in gist_groups[gist_b] if s["id"] in embeddings]
            for sa in group_a:
                for sb in group_b:
                    d = cosine_distance(embeddings[sa["id"]], embeddings[sb["id"]])
                    between_distances.append(d)

    # Report
    print("=" * 60)
    print("CONSTRAINT CLUSTER PROBE — Results")
    print("=" * 60)
    
    if within_distances:
        w_mean = np.mean(within_distances)
        w_std = np.std(within_distances)
        print(f"\nWithin-CCS-version distances:")
        print(f"  N pairs: {len(within_distances)}")
        print(f"  Mean:    {w_mean:.4f}")
        print(f"  Std:     {w_std:.4f}")
    else:
        w_mean = 0
        print("\nNo within-group pairs (single-snapshot groups).")

    if between_distances:
        b_mean = np.mean(between_distances)
        b_std = np.std(between_distances)
        print(f"\nBetween-CCS-version distances:")
        print(f"  N pairs: {len(between_distances)}")
        print(f"  Mean:    {b_mean:.4f}")
        print(f"  Std:     {b_std:.4f}")
    else:
        b_mean = 0
        print("\nNo between-group pairs.")

    if within_distances and between_distances:
        ratio = b_mean / w_mean if w_mean > 0 else float("inf")
        separation = (b_mean - w_mean) / ((w_std + b_std) / 2) if (w_std + b_std) > 0 else 0
        
        print(f"\nBetween/Within ratio: {ratio:.3f}")
        print(f"Cohen's d (separation): {separation:.3f}")
        
        if ratio > 1.5 and separation > 0.8:
            print("\n>>> STRONG clustering: CCS versions form distinct clusters.")
            print("    Constraint-cluster model is LOAD-BEARING.")
        elif ratio > 1.2 and separation > 0.3:
            print("\n>>> MODERATE clustering: CCS versions show some separation.")
            print("    Constraint-cluster model adds signal but isn't dominant.")
        else:
            print("\n>>> WEAK/NO clustering: CCS versions don't separate clearly.")
            print("    Convergence model may be sufficient.")
    
    print()


if __name__ == "__main__":
    main()
