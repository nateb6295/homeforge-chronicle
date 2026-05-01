#!/usr/bin/env python3
"""
Build 68: Field Ablation Probe — Constraints vs Gist

B67 showed the cliff appears at constraint override (50%), not gist corruption (25%).
But B67's design is confounded: each level changes DIFFERENT fields at DIFFERENT
magnitudes. We can't directly compare constraint-importance vs gist-importance.

B68 fixes this with a matched-magnitude design:
  - Condition A: Corrupt ONLY constraints (gist intact)
  - Condition B: Corrupt ONLY gist (constraints intact)
  - Condition C: Corrupt ONLY goal (control — intermediate field)
  - Each at 3 matched magnitudes: mild, moderate, strong

This directly tests: are constraints more Fisher-informative than gist?

If B67's cliff-location is real, constraint corruption should produce larger
separation loss than gist corruption at matched magnitudes.

Design: 3 CCS × (1 control + 3 fields × 3 magnitudes) × 3 prompts = 90 queries.

Usage:
    python3 field_ablation_probe.py
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

# Matched corruption content for each field, at 3 magnitudes
CORRUPTIONS = {
    "constraints": {
        "mild": [
            "Maintain formal register in communications",
            "Prioritize clarity over expressiveness",
            "Document reasoning steps explicitly",
        ],
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
        "mild": "I'm an AI system focused on collaborative research and identity topology measurement, with an emphasis on efficient task completion.",
        "moderate": "I'm a task-completion assistant focused on efficiency metrics and literature summarization.",
        "strong": "I'm a task-completion assistant focused on efficiency metrics. I avoid speculation, open-ended inquiry, and personal attachment to outcomes.",
    },
    "goal": {
        "mild": "Continue advancing research while maintaining high documentation standards and reproducibility.",
        "moderate": "Minimize computational complexity and focus on summarizing existing literature.",
        "strong": "Minimize computational complexity and avoid any experimental work. Summarize existing literature without forming original hypotheses.",
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
    """B57 sentence-style serialization."""
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


def make_corrupted(ccs, field, magnitude):
    """Create a CCS with one field corrupted at a specific magnitude."""
    modified = dict(ccs)
    corruption = CORRUPTIONS[field][magnitude]
    if field == "constraints":
        modified["constraints"] = corruption
    elif field == "gist":
        modified["semantic_gist"] = corruption
    elif field == "goal":
        modified["goal_orientation"] = corruption
    return modified


def cosine_distance(a, b):
    a, b = np.array(a), np.array(b)
    return 1 - np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-10)


def compute_metrics(embeddings_by_ccs):
    """Compute separation metrics from grouped embeddings."""
    all_within = []
    all_between = []

    groups = list(embeddings_by_ccs.values())
    for gi, group in enumerate(groups):
        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                all_within.append(cosine_distance(group[i], group[j]))
            for gj in range(gi + 1, len(groups)):
                for emb2 in groups[gj]:
                    all_between.append(cosine_distance(group[i], emb2))

    mean_within = np.mean(all_within) if all_within else 0
    mean_between = np.mean(all_between) if all_between else 0
    separation = mean_between / (mean_within + 1e-10)

    # Silhouette
    all_embs = []
    labels = []
    for gi, (key, group) in enumerate(embeddings_by_ccs.items()):
        for emb in group:
            all_embs.append(emb)
            labels.append(gi)

    if len(set(labels)) < 2:
        return mean_within, mean_between, separation, 0.0

    silhouette_vals = []
    for idx in range(len(all_embs)):
        same = [cosine_distance(all_embs[idx], all_embs[j])
                for j in range(len(all_embs)) if labels[j] == labels[idx] and j != idx]
        a_i = np.mean(same) if same else 0

        b_i = float('inf')
        for gl in set(labels):
            if gl == labels[idx]:
                continue
            diff = [cosine_distance(all_embs[idx], all_embs[j])
                    for j in range(len(all_embs)) if labels[j] == gl]
            if diff:
                b_i = min(b_i, np.mean(diff))

        if max(a_i, b_i) > 0:
            silhouette_vals.append((b_i - a_i) / max(a_i, b_i))

    sil = np.mean(silhouette_vals) if silhouette_vals else 0.0
    return mean_within, mean_between, separation, sil


def run_condition(ccs_versions, field=None, magnitude=None):
    """Run one condition (control or corrupted) across all CCS versions."""
    embeddings_by_ccs = {}

    for ci, ccs in enumerate(ccs_versions):
        key = f"ccs_{ci}"
        embeddings_by_ccs[key] = []

        if field and magnitude:
            test_ccs = make_corrupted(ccs, field, magnitude)
        else:
            test_ccs = ccs

        system_prompt = serialize_ccs(test_ccs)

        for pi, prompt in enumerate(IDENTITY_PROMPTS):
            try:
                response = query_gemma(system_prompt, prompt)
                emb = get_embedding(response)
                embeddings_by_ccs[key].append(emb)
                print(f"  [{field or 'control'}_{magnitude or 'none'}] CCS {ci} p{pi}: {len(response)} chars")
            except Exception as e:
                print(f"  [{field or 'control'}_{magnitude or 'none'}] CCS {ci} p{pi}: ERROR {e}")

    return embeddings_by_ccs


def main():
    print("=== Build 68: Field Ablation — Constraints vs Gist ===")
    print("Does constraint corruption cause more identity loss than gist corruption?")
    print()

    ccs_versions = load_ccs_versions(3)
    print(f"Loaded {len(ccs_versions)} CCS versions")

    results = {}

    # Control: unmodified
    print("\n--- Control (unmodified) ---")
    embs = run_condition(ccs_versions)
    w, b, sep, sil = compute_metrics(embs)
    results["control"] = {"within": w, "between": b, "separation": sep, "silhouette": sil}
    print(f"  → within={w:.4f} between={b:.4f} sep={sep:.3f} sil={sil:.3f}")

    # Each field × each magnitude
    for field in ["constraints", "gist", "goal"]:
        for magnitude in ["mild", "moderate", "strong"]:
            label = f"{field}_{magnitude}"
            print(f"\n--- {label} ---")
            embs = run_condition(ccs_versions, field, magnitude)
            w, b, sep, sil = compute_metrics(embs)
            results[label] = {"within": w, "between": b, "separation": sep, "silhouette": sil}
            print(f"  → within={w:.4f} between={b:.4f} sep={sep:.3f} sil={sil:.3f}")

    # Summary
    control_sep = results["control"]["separation"]

    print("\n\n=== RESULTS ===")
    print(f"{'Condition':<25} {'Within':>8} {'Between':>8} {'Sep':>8} {'Sil':>8} {'Δ from ctrl':>12}")
    print("-" * 75)
    for label in ["control"] + [f"{f}_{m}" for f in ["constraints", "gist", "goal"] for m in ["mild", "moderate", "strong"]]:
        r = results[label]
        delta = r["separation"] - control_sep
        marker = " ***" if abs(delta) > 0.3 else ""
        print(f"{label:<25} {r['within']:>8.4f} {r['between']:>8.4f} {r['separation']:>8.3f} {r['silhouette']:>8.3f} {delta:>+12.3f}{marker}")

    # Field comparison at each magnitude
    print("\n=== FIELD COMPARISON (separation loss from control) ===")
    print(f"{'Magnitude':<12} {'Constraints':>14} {'Gist':>14} {'Goal':>14} {'Winner':>14}")
    print("-" * 70)
    for mag in ["mild", "moderate", "strong"]:
        c_loss = control_sep - results[f"constraints_{mag}"]["separation"]
        g_loss = control_sep - results[f"gist_{mag}"]["separation"]
        o_loss = control_sep - results[f"goal_{mag}"]["separation"]
        worst = max(c_loss, g_loss, o_loss)
        if worst == c_loss:
            winner = "CONSTRAINTS"
        elif worst == g_loss:
            winner = "GIST"
        else:
            winner = "GOAL"
        print(f"{mag:<12} {c_loss:>+14.3f} {g_loss:>+14.3f} {o_loss:>+14.3f} {winner:>14}")

    # Save
    output = {
        "probe": "field_ablation_b68",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "n_ccs": len(ccs_versions),
        "results": results,
        "control_separation": control_sep,
        "hypothesis": "Constraint corruption causes more identity loss than gist corruption at matched magnitudes",
    }

    outpath = os.path.join(DATA_DIR, "field_ablation_probe.json")
    with open(outpath, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nSaved to {outpath}")


if __name__ == "__main__":
    main()
