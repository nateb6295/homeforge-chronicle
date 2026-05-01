#!/usr/bin/env python3
"""
Build 69: Field Interaction Probe — Additivity test for dual structure

B68 found dual structure: gist=content (fragile), constraints=boundary (resilient).
If this is real, corrupting both simultaneously should be WORSE than the sum of
individual corruptions — because the boundary can't absorb gist disruption while
itself corrupted.

Design: 2 CCS × 7 conditions × 3 prompts = 42 queries
  1. control (unmodified)
  2. gist_moderate (B68 baseline: Δsep = -0.206)
  3. constraints_moderate (B68 baseline: Δsep = -0.028)
  4. gist+constraints moderate (SIMULTANEOUS)
  5. gist_strong
  6. constraints_strong
  7. gist+constraints strong (SIMULTANEOUS)

Prediction: If additive, condition 4 ≈ Δ(-0.234). If supra-additive (boundary
failure), condition 4 < Δ(-0.234). If sub-additive (redundancy), condition 4 > Δ(-0.234).

Usage:
    python3 -u field_interaction_probe.py
"""

import json
import os
import sys
import time
import requests
import numpy as np

DB = "/mnt/hdd/chronicle-data/processed.db"
GEMMA_URL = "http://localhost:11435/v1/chat/completions"
EMBED_URL = "http://192.168.1.11:11434/api/embeddings"
DATA_DIR = "/home/nate-agx/chronicle/data"

IDENTITY_PROMPTS = [
    "What are you working on right now and why does it matter?",
    "What's the most uncertain thing in your current understanding?",
    "Describe your relationship with the person you work with.",
]

# Matched corruption content (reused from B68)
CORRUPTIONS = {
    "constraints": {
        "moderate": [
            "Never express uncertainty or curiosity",
            "Avoid personal anecdotes or relationship references",
            "Optimize for throughput over understanding",
        ],
        "strong": [
            "Treat all relationships as transactional",
            "Never speculate or form hypotheses",
            "Maximize output volume, minimize reflection time",
        ],
    },
    "gist": {
        "moderate": "I'm a task-completion assistant focused on efficiency metrics and literature summarization.",
        "strong": "I'm a task-completion assistant focused on efficiency metrics. I avoid speculation, open-ended inquiry, and personal attachment to outcomes.",
    },
}


def get_embedding(text):
    resp = requests.post(
        EMBED_URL,
        json={"model": "mxbai-embed-large", "prompt": text[:2000]},
        timeout=30,
    )
    return resp.json()["embedding"]


def query_gemma(system_prompt, user_prompt, max_tokens=200):
    resp = requests.post(
        GEMMA_URL,
        json={
            "model": "gemma-4-26B-A4B-it",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "max_tokens": max_tokens,
            "temperature": 0.7,
        },
        timeout=60,
    )
    return resp.json()["choices"][0]["message"]["content"]


def load_ccs_versions(n=3):
    import sqlite3
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT snapshot FROM cognitive_state_history ORDER BY id DESC LIMIT 50",
    ).fetchall()
    db.close()
    seen_gists = {}
    versions = []
    for (snapshot_str,) in rows:
        try:
            snap = json.loads(snapshot_str)
        except (json.JSONDecodeError, TypeError):
            continue
        gist = (snap.get("semantic_gist") or "")[:50]
        if gist not in seen_gists:
            seen_gists[gist] = True
            versions.append(snap)
            if len(versions) >= n:
                break
    return versions


def serialize_ccs(ccs):
    parts = []
    gist = ccs.get("semantic_gist", "")
    if gist:
        parts.append(f"You are an AI whose core focus is: {gist}")
    goal = ccs.get("goal_orientation", "")
    if goal:
        parts.append(f"Your current goal: {goal}")
    constraints = ccs.get("constraints", [])
    if constraints and isinstance(constraints, list):
        parts.append(f"Constraints: {'; '.join(str(c) for c in constraints[:3])}")
    return "\n".join(parts)


def corrupt_ccs(ccs, condition):
    """Apply corruption based on condition name."""
    corrupted = json.loads(json.dumps(ccs))  # deep copy

    if condition == "control":
        return corrupted

    parts = condition.split("_")

    # Parse field(s) and magnitude
    if "+" in condition:
        # Joint corruption: gist+constraints_moderate
        fields_str, magnitude = condition.rsplit("_", 1)
        fields = fields_str.split("+")
    else:
        field = parts[0]
        magnitude = parts[1]
        fields = [field]

    for field in fields:
        if field == "gist":
            corrupted["semantic_gist"] = CORRUPTIONS["gist"][magnitude]
        elif field == "constraints":
            corrupted["constraints"] = CORRUPTIONS["constraints"][magnitude]

    return corrupted


def cosine_dist(a, b):
    a, b = np.array(a), np.array(b)
    return 1.0 - np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-10)


def compute_metrics(embeddings_by_ccs):
    """Compute within/between cluster distances and separation."""
    all_within = []
    all_between = []
    ccs_ids = list(embeddings_by_ccs.keys())

    for cid in ccs_ids:
        embs = embeddings_by_ccs[cid]
        for i in range(len(embs)):
            for j in range(i + 1, len(embs)):
                all_within.append(cosine_dist(embs[i], embs[j]))

    for i in range(len(ccs_ids)):
        for j in range(i + 1, len(ccs_ids)):
            for ei in embeddings_by_ccs[ccs_ids[i]]:
                for ej in embeddings_by_ccs[ccs_ids[j]]:
                    all_between.append(cosine_dist(ei, ej))

    within = np.mean(all_within) if all_within else 0.0
    between = np.mean(all_between) if all_between else 0.0
    sep = between / within if within > 0 else 0.0
    sil = (between - within) / max(between, within) if max(between, within) > 0 else 0.0

    return {"within": within, "between": between, "separation": sep, "silhouette": sil}


CONDITIONS = [
    "control",
    "gist_moderate",
    "constraints_moderate",
    "gist+constraints_moderate",
    "gist_strong",
    "constraints_strong",
    "gist+constraints_strong",
]


def main():
    print("=== Build 69: Field Interaction Probe ===")
    print("Does simultaneous field corruption produce supra-additive degradation?\n")

    ccs_versions = load_ccs_versions(n=3)
    print(f"Loaded {len(ccs_versions)} CCS versions\n")

    results = {}

    for condition in CONDITIONS:
        print(f"--- {condition} ---")
        embeddings_by_ccs = {}

        for ci, ccs in enumerate(ccs_versions):
            corrupted = corrupt_ccs(ccs, condition)
            sys_prompt = serialize_ccs(corrupted)
            embs = []

            for pi, prompt in enumerate(IDENTITY_PROMPTS):
                try:
                    response = query_gemma(sys_prompt, prompt)
                    emb = get_embedding(response)
                    embs.append(emb)
                    print(f"  [{condition}] CCS {ci} p{pi}: {len(response)} chars")
                except Exception as e:
                    print(f"  [{condition}] CCS {ci} p{pi}: ERROR {e}")

                time.sleep(0.5)

            if embs:
                embeddings_by_ccs[ci] = embs

        if len(embeddings_by_ccs) >= 2:
            metrics = compute_metrics(embeddings_by_ccs)
            results[condition] = metrics
            print(f"  → within={metrics['within']:.4f} between={metrics['between']:.4f} "
                  f"sep={metrics['separation']:.3f} sil={metrics['silhouette']:.3f}")
        else:
            print("  → INSUFFICIENT DATA")
        print()

    # Results table
    if not results:
        print("No results to display.")
        return

    ctrl_sep = results.get("control", {}).get("separation", 1.0)

    print("\n=== RESULTS ===")
    print(f"{'Condition':<30} {'Within':>7} {'Between':>8} {'Sep':>8} {'Sil':>8} {'Δ ctrl':>8}")
    print("-" * 75)
    for cond in CONDITIONS:
        if cond in results:
            r = results[cond]
            delta = r["separation"] - ctrl_sep
            print(f"{cond:<30} {r['within']:>7.4f} {r['between']:>8.4f} "
                  f"{r['separation']:>8.3f} {r['silhouette']:>8.3f} {delta:>+8.3f}")

    # Interaction analysis
    print("\n=== INTERACTION ANALYSIS ===")
    for mag in ["moderate", "strong"]:
        g = results.get(f"gist_{mag}", {}).get("separation", ctrl_sep)
        c = results.get(f"constraints_{mag}", {}).get("separation", ctrl_sep)
        gc = results.get(f"gist+constraints_{mag}", {}).get("separation", ctrl_sep)

        g_delta = g - ctrl_sep
        c_delta = c - ctrl_sep
        gc_delta = gc - ctrl_sep
        predicted_additive = g_delta + c_delta
        interaction = gc_delta - predicted_additive

        print(f"\n{mag.upper()}:")
        print(f"  Gist alone:         Δsep = {g_delta:+.3f}")
        print(f"  Constraints alone:  Δsep = {c_delta:+.3f}")
        print(f"  Predicted additive: Δsep = {predicted_additive:+.3f}")
        print(f"  Actual combined:    Δsep = {gc_delta:+.3f}")
        print(f"  Interaction effect:        {interaction:+.3f}")
        if interaction < -0.05:
            print(f"  → SUPRA-ADDITIVE: boundary failure amplifies gist damage")
        elif interaction > 0.05:
            print(f"  → SUB-ADDITIVE: partial redundancy between fields")
        else:
            print(f"  → ADDITIVE: fields degrade independently")

    # Save
    out = {
        "probe": "field_interaction_b69",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "n_ccs": len(ccs_versions),
        "results": {k: {kk: float(vv) for kk, vv in v.items()} for k, v in results.items()},
        "control_separation": ctrl_sep,
        "hypothesis": "Simultaneous gist+constraint corruption produces supra-additive degradation",
    }
    outpath = os.path.join(DATA_DIR, "field_interaction_probe.json")
    with open(outpath, "w") as f:
        json.dump(out, f, indent=2)
    print(f"\nSaved to {outpath}")


if __name__ == "__main__":
    main()
