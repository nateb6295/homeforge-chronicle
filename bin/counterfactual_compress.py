#!/usr/bin/env python3
"""
Counterfactual compression — detect attentional reinforcement loops.

Removes the top thread from the CCS input and measures how much the
recognition centroid changes. If the centroid barely shifts, the thread
may be self-reinforcing (CCS reports focus because it focused last time,
not because evidence warrants it).

Outputs:
  - centroid_delta: cosine distance between real and counterfactual centroids
  - quadrant_shifts: how many capsules change quadrant assignment
  - reinforcement_score: 0 = thread is evidence-driven, 1 = purely self-reinforcing

Usage:
  python3 counterfactual_compress.py           # test top thread
  python3 counterfactual_compress.py --thread N  # test specific thread
"""

import argparse
import json
import struct
import sqlite3
import sys
import time
import numpy as np
import requests
from pathlib import Path

DB_PATH = "/mnt/hdd/chronicle-data/processed.db"
CENTROID_PATH = Path.home() / "chronicle" / "data" / "recognition_centroid.json"
OLLAMA_URL = "http://localhost:11434"
EMBED_MODEL = "nomic-embed-text"
EMBED_DIM = 768

sys.path.insert(0, str(Path(__file__).parent))
from recognition_centroid import (
    get_ccs_via_mcp, get_active_thread_ids, get_thread_positions,
    build_centroid_components, build_weighted_centroid, embed_text,
    unpack_embedding, cosine_sim, load_centroid,
)

def find_top_thread(ccs: dict, db: sqlite3.Connection) -> int | None:
    entities = ccs.get("focal_entities", [])
    for e in sorted(entities, key=lambda x: x.get("salience", 0), reverse=True):
        name = e.get("name", "")
        if name.startswith("Thread #"):
            try:
                return int(name.replace("Thread #", ""))
            except ValueError:
                continue

    row = db.execute(
        "SELECT thread_id, count(*) as c FROM thread_history "
        "WHERE created_at > ? GROUP BY thread_id ORDER BY c DESC LIMIT 1",
        (int(time.time()) - 86400,)
    ).fetchone()
    return row[0] if row else None

def build_counterfactual_centroid(ccs: dict, db: sqlite3.Connection,
                                  exclude_thread: int) -> np.ndarray:
    active_threads = get_active_thread_ids(db)
    active_threads = [t for t in active_threads if t != exclude_thread]
    thread_positions = get_thread_positions(db, active_threads)

    cf_ccs = dict(ccs)
    cf_ccs["focal_entities"] = [
        e for e in ccs.get("focal_entities", [])
        if f"#{exclude_thread}" not in e.get("name", "")
    ]

    components = build_centroid_components(cf_ccs, thread_positions)
    return build_weighted_centroid(components)

def score_quadrants(centroid: np.ndarray, db: sqlite3.Connection) -> dict[int, str]:
    rows = db.execute(
        "SELECT ce.capsule_id, ce.embedding FROM capsule_embeddings ce"
    ).fetchall()

    assignments = {}
    for capsule_id, blob in rows:
        emb = unpack_embedding(blob)
        if len(emb) != EMBED_DIM:
            continue
        alignment = cosine_sim(emb, centroid)
        assignments[capsule_id] = "HIGH" if alignment >= 0.69 else "LOW"
    return assignments

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--thread", type=int, help="Thread ID to exclude (default: top)")
    parser.add_argument("--full", action="store_true", help="Score all capsules for quadrant shift")
    args = parser.parse_args()

    db = sqlite3.connect(DB_PATH)
    ccs = get_ccs_via_mcp()

    exclude_thread = args.thread or find_top_thread(ccs, db)
    if not exclude_thread:
        print("No thread to exclude — can't run counterfactual")
        return

    print(f"Counterfactual compression: excluding Thread #{exclude_thread}")

    real_centroid = load_centroid()
    cf_centroid = build_counterfactual_centroid(ccs, db, exclude_thread)

    centroid_delta = 1.0 - cosine_sim(real_centroid, cf_centroid)
    print(f"\n  Centroid delta: {centroid_delta:.6f}")
    print(f"  (0 = identical attention, 1 = completely different)")

    if centroid_delta < 0.01:
        reinforcement = "HIGH — thread barely shifts the centroid; may be self-reinforcing"
    elif centroid_delta < 0.05:
        reinforcement = "MODERATE — thread contributes but doesn't dominate"
    else:
        reinforcement = "LOW — thread genuinely drives attention allocation"

    print(f"  Reinforcement risk: {reinforcement}")

    if args.full:
        print("\n  Scoring quadrant shifts (this takes a moment)...")
        real_q = score_quadrants(real_centroid, db)
        cf_q = score_quadrants(cf_centroid, db)

        shifts = sum(1 for k in real_q if real_q[k] != cf_q.get(k, real_q[k]))
        total = len(real_q)
        shift_pct = 100 * shifts / total if total else 0

        print(f"  Quadrant shifts: {shifts}/{total} ({shift_pct:.1f}%)")
        print(f"  Capsules that change alignment status when Thread #{exclude_thread} removed")

    other_threads = get_active_thread_ids(db)
    other_threads = [t for t in other_threads if t != exclude_thread]
    print(f"\n  Control: testing other active threads for comparison...")
    for tid in other_threads[:3]:
        cf2 = build_counterfactual_centroid(ccs, db, tid)
        delta2 = 1.0 - cosine_sim(real_centroid, cf2)
        print(f"    Thread #{tid}: delta={delta2:.6f}")

    result = {
        "excluded_thread": exclude_thread,
        "centroid_delta": centroid_delta,
        "reinforcement_assessment": reinforcement,
        "timestamp": int(time.time()),
    }
    out_path = Path.home() / "chronicle" / "data" / "counterfactual_compress.json"
    with open(out_path, 'w') as f:
        json.dump(result, f, indent=2)
    print(f"\n  Result saved to {out_path}")

    db.close()

if __name__ == "__main__":
    main()
