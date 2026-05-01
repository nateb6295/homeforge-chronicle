#!/usr/bin/env python3
"""
Build 58b: Identity Manifold Visualization

Takes the B58 finding (2D identity manifold) and produces an ASCII visualization
of the three CCS gists as points on their 2D plane, plus the full CCS points
projected into the same space.

Shows Nate what "2 dimensions of identity" looks like.
"""

import json
import os
import sqlite3
import urllib.request
import numpy as np

DB = "/mnt/hdd/chronicle-data/processed.db"
EMBED_URL = "http://192.168.1.11:11434/api/embeddings"


def get_embedding(text):
    req = urllib.request.Request(
        EMBED_URL,
        data=json.dumps({"model": "mxbai-embed-large", "prompt": text[:2000]}).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())["embedding"]


def extract_text(snapshot, identity_only=True):
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
    return "\n".join(parts)


def main():
    print("Identity Manifold Visualization")
    print("=" * 60)

    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT snapshot FROM cognitive_state_history ORDER BY created_at"
    ).fetchall()
    db.close()

    # Group by gist, embed identity-only and full
    gist_map = {}
    for snap_json, in rows:
        snap = json.loads(snap_json)
        gist = snap.get("semantic_gist", "")[:50]
        if gist not in gist_map:
            gist_map[gist] = {"snap": snap, "count": 0}
        gist_map[gist]["count"] += 1

    print(f"Unique gists: {len(gist_map)}")
    for g, d in gist_map.items():
        print(f"  [{d['count']:2d}x] {g[:50]}")

    # Embed each gist under identity-only
    labels = []
    id_embeddings = []
    full_embeddings = []

    for gist, data in gist_map.items():
        snap = data["snap"]
        id_text = extract_text(snap, identity_only=True)
        full_text = extract_text(snap, identity_only=False)

        id_emb = get_embedding(id_text)
        full_emb = get_embedding(full_text)

        labels.append(gist[:20])
        id_embeddings.append(id_emb)
        full_embeddings.append(full_emb)

    # PCA on identity-only
    arr = np.array(id_embeddings)
    arr_centered = arr - arr.mean(axis=0)
    U, S, Vt = np.linalg.svd(arr_centered, full_matrices=False)
    pcs = arr_centered @ Vt[:2].T  # Project onto first 2 PCs

    # Project full CCS into same PC space
    full_arr = np.array(full_embeddings)
    full_centered = full_arr - arr.mean(axis=0)  # Use same centering
    full_pcs = full_centered @ Vt[:2].T

    # ASCII scatter plot
    print(f"\n2D Identity Manifold (PCA)")
    print(f"PC1 explains {(S[0]**2/sum(S**2))*100:.1f}% variance")
    print(f"PC2 explains {(S[1]**2/sum(S**2))*100:.1f}% variance")
    print()

    # Normalize to grid
    all_x = list(pcs[:, 0]) + list(full_pcs[:, 0])
    all_y = list(pcs[:, 1]) + list(full_pcs[:, 1])
    x_min, x_max = min(all_x), max(all_x)
    y_min, y_max = min(all_y), max(all_y)
    x_range = x_max - x_min + 1e-10
    y_range = y_max - y_min + 1e-10

    WIDTH = 60
    HEIGHT = 25

    grid = [[' ' for _ in range(WIDTH)] for _ in range(HEIGHT)]

    # Plot identity-only points
    id_symbols = ['A', 'B', 'C', 'D', 'E', 'F']
    for i, (x, y) in enumerate(pcs):
        gx = int((x - x_min) / x_range * (WIDTH - 1))
        gy = HEIGHT - 1 - int((y - y_min) / y_range * (HEIGHT - 1))
        grid[gy][gx] = id_symbols[i % len(id_symbols)]

    # Plot full-CCS points (lowercase)
    full_symbols = ['a', 'b', 'c', 'd', 'e', 'f']
    for i, (x, y) in enumerate(full_pcs):
        gx = int((x - x_min) / x_range * (WIDTH - 1))
        gy = HEIGHT - 1 - int((y - y_min) / y_range * (HEIGHT - 1))
        if grid[gy][gx] == ' ':
            grid[gy][gx] = full_symbols[i % len(full_symbols)]
        else:
            grid[gy][gx] = '*'  # Overlap

    # Draw grid
    print(f"    {'─' * WIDTH}")
    for row in grid:
        print(f"   |{''.join(row)}|")
    print(f"    {'─' * WIDTH}")
    print(f"    PC1 →")
    print()

    # Legend
    print("Legend:")
    for i, label in enumerate(labels):
        sym = id_symbols[i % len(id_symbols)]
        fsym = full_symbols[i % len(full_symbols)]
        dist = np.linalg.norm(pcs[i] - full_pcs[i])
        print(f"  {sym}/{fsym}: {label:20s} (id→full shift: {dist:.4f})")

    # Pairwise distances
    print("\nPairwise distances (identity-only):")
    for i in range(len(labels)):
        for j in range(i + 1, len(labels)):
            d = np.linalg.norm(pcs[i] - pcs[j])
            print(f"  {labels[i]:20s} ↔ {labels[j]:20s}: {d:.4f}")

    print("\nKey:")
    print("  UPPERCASE = identity-only CCS (gist + goal + constraints)")
    print("  lowercase = full CCS (+ episodic + entities)")
    print("  * = overlap (identity-only and full land on same pixel)")
    print("  If letters are CLOSE: identity-only ≈ full (episodic doesn't move it)")
    print("  If letters are FAR: episodic content shifts the identity projection")


if __name__ == "__main__":
    main()
