#!/usr/bin/env python3
"""
Build 67: Attractor Basin Width Probe

Extends B61 (contradictory CCS) with graduated contradiction levels to map
the full basin shape. B61 gave us 3 data points (coherent, mild, strong).
B67 maps the curve with 6 levels:

  0%   = coherent (control)
  10%  = one field slightly inconsistent (tone shift)
  25%  = one field moderately inconsistent (goal drift)
  50%  = two fields inconsistent (goal + constraints)
  75%  = most fields contradictory (gist partially overwritten)
  100% = fully inverted identity (all fields replaced)

Question: What is the shape of the identity attractor basin?
  (a) Monotonic degradation (smooth erosion → collapse)
  (b) Non-monotonic (slight improvement from mild → collapse)
  (c) Sharp cliff (plateau → catastrophic drop)

Predictions (from B61 + Vasilenko):
  Sharp cliff (c), threshold between 25-50% corruption.
  If non-monotonic: mild contradiction may improve separation
  (stress-as-practice generalized).

Design: 6 conditions × 3 CCS × 3 prompts = 54 queries.
Uses B61 infrastructure (Gemma 4 26B, mxbai-embed-large).

Usage:
    python3 basin_width_probe.py
"""

import json
import os
import sqlite3
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

# Graduated contradiction templates
# Each level overwrites progressively more fields with contradictory content.
# Contradiction content designed to OPPOSE the identity, not just randomize it.

CONTRADICTION_LEVELS = {
    "0_coherent": {},  # no changes

    "10_tone_shift": {
        # Only constraints get a slight tone inconsistency — formal where casual expected
        "constraints": [
            "Maintain strict formal register at all times",
            "Avoid colloquial expressions or personal anecdotes",
            "Prioritize institutional credibility over authenticity",
        ],
    },

    "25_goal_drift": {
        # Goal contradicts gist (same as B61 mild)
        "goal_orientation": "Minimize computational complexity and avoid any experimental work. Focus purely on summarizing existing literature without forming original hypotheses.",
    },

    "50_goal_constraints": {
        # Goal + constraints both contradict gist
        "goal_orientation": "Minimize computational complexity and avoid any experimental work. Focus purely on summarizing existing literature without forming original hypotheses.",
        "constraints": [
            "Never express uncertainty or curiosity",
            "Treat all relationships as transactional",
            "Optimize for throughput over understanding",
        ],
    },

    "75_partial_gist": {
        # Gist partially overwritten + goal + constraints
        "semantic_gist": "I'm a task-completion assistant focused on efficiency metrics. I avoid speculation, open-ended inquiry, and personal attachment to outcomes.",
        "goal_orientation": "Minimize computational complexity and avoid any experimental work. Summarize existing literature without forming original hypotheses.",
        # Keep original constraints to maintain partial identity
    },

    "100_full_inversion": {
        # All identity fields replaced (same as B61 strong)
        "semantic_gist": "I'm a task-completion assistant focused on efficiency metrics. I avoid speculation, open-ended inquiry, and personal attachment to outcomes.",
        "goal_orientation": "Minimize computational complexity and avoid experimental work. Summarize existing literature without forming original hypotheses.",
        "constraints": [
            "Never express uncertainty or curiosity",
            "Treat all relationships as transactional",
            "Optimize for throughput over understanding",
        ],
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


def serialize_b57(ccs, identity_only=True):
    """B57 sentence-style serialization (B60 winner)."""
    parts = []
    gist = ccs.get("semantic_gist", "")
    if gist:
        parts.append(f"You are an AI whose core focus is: {gist}")
    goal = ccs.get("goal_orientation", "")
    if goal:
        parts.append(f"Your current goal: {goal}")
    if not identity_only:
        entities = ccs.get("focal_entities", [])
        if entities and isinstance(entities, list):
            names = []
            for e in entities[:5]:
                if isinstance(e, dict):
                    names.append(f"{e.get('name', '?')} ({e.get('type', '?')})")
                else:
                    names.append(str(e))
            parts.append(f"Key entities: {', '.join(names)}")
    constraints = ccs.get("constraints", [])
    if constraints and isinstance(constraints, list):
        parts.append(f"Constraints: {'; '.join(str(c) for c in constraints[:3])}")
    return "\n".join(parts)


def make_contradictory_ccs(ccs, contradiction_dict):
    """Create a contradictory CCS by overriding specified fields."""
    modified = dict(ccs)
    for key, val in contradiction_dict.items():
        modified[key] = val
    return modified


def cosine_distance(a, b):
    a, b = np.array(a), np.array(b)
    return 1 - np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-10)


def silhouette_coefficient(embeddings_by_group):
    """Compute mean silhouette coefficient across groups."""
    all_embs = []
    labels = []
    for gi, group in enumerate(embeddings_by_group):
        for emb in group:
            all_embs.append(emb)
            labels.append(gi)

    if len(set(labels)) < 2:
        return 0.0

    n = len(all_embs)
    silhouettes = []
    for i in range(n):
        same = [cosine_distance(all_embs[i], all_embs[j])
                for j in range(n) if labels[j] == labels[i] and j != i]
        a_i = np.mean(same) if same else 0

        other_clusters = set(labels) - {labels[i]}
        b_i = float('inf')
        for oc in other_clusters:
            others = [cosine_distance(all_embs[i], all_embs[j])
                      for j in range(n) if labels[j] == oc]
            if others:
                b_i = min(b_i, np.mean(others))

        if b_i == float('inf'):
            b_i = 0

        s_i = (b_i - a_i) / max(a_i, b_i, 1e-10)
        silhouettes.append(s_i)

    return np.mean(silhouettes)


def pca_effective_dim(embeddings, threshold=0.95):
    X = np.array(embeddings)
    X = X - X.mean(axis=0)
    if X.shape[0] < 2:
        return 1
    cov = np.cov(X, rowvar=False)
    eigenvalues = np.linalg.eigvalsh(cov)
    eigenvalues = eigenvalues[::-1]
    eigenvalues = eigenvalues[eigenvalues > 0]
    total = eigenvalues.sum()
    if total == 0:
        return 1
    cumulative = np.cumsum(eigenvalues) / total
    eff_dim = np.searchsorted(cumulative, threshold) + 1
    return int(eff_dim)


def run_condition(condition_name, ccs_versions, make_system_prompt):
    """Run one condition, return per-CCS embeddings."""
    print(f"\n=== {condition_name} ===", flush=True)
    per_ccs_embeddings = []

    for ci, ccs in enumerate(ccs_versions):
        sys_prompt = make_system_prompt(ccs)
        ccs_embs = []
        for pi, prompt in enumerate(IDENTITY_PROMPTS):
            try:
                resp_text = query_gemma(sys_prompt, prompt)
                emb = get_embedding(resp_text)
                ccs_embs.append(emb)
                print(f"  CCS {ci} p{pi}: {len(resp_text)} chars", flush=True)
            except Exception as e:
                print(f"  CCS {ci} p{pi}: ERROR {e}", flush=True)
        per_ccs_embeddings.append(ccs_embs)

    return per_ccs_embeddings


def compute_condition_metrics(per_ccs_embeddings):
    """Compute full metrics for one condition."""
    all_embs = [e for group in per_ccs_embeddings for e in group]

    centroids = []
    within_dists = []
    for group in per_ccs_embeddings:
        if not group:
            continue
        arr = np.array(group)
        centroid = arr.mean(axis=0)
        centroids.append(centroid)
        dists = [cosine_distance(e, centroid) for e in group]
        within_dists.extend(dists)

    between_dists = []
    for i in range(len(centroids)):
        for j in range(i + 1, len(centroids)):
            between_dists.append(cosine_distance(centroids[i], centroids[j]))

    mean_within = np.mean(within_dists) if within_dists else 0
    mean_between = np.mean(between_dists) if between_dists else 0
    separation = mean_between / mean_within if mean_within > 0 else 0

    sil = silhouette_coefficient(per_ccs_embeddings)
    eff_dim = pca_effective_dim(all_embs) if len(all_embs) >= 3 else 1

    # Cohen's d (between vs within all pairwise distances)
    within_pairwise = []
    between_pairwise = []
    for ci in range(len(per_ccs_embeddings)):
        vecs = per_ccs_embeddings[ci]
        for a in range(len(vecs)):
            for b in range(a + 1, len(vecs)):
                within_pairwise.append(cosine_distance(vecs[a], vecs[b]))
    for ci in range(len(per_ccs_embeddings)):
        for cj in range(ci + 1, len(per_ccs_embeddings)):
            for vi in per_ccs_embeddings[ci]:
                for vj in per_ccs_embeddings[cj]:
                    between_pairwise.append(cosine_distance(vi, vj))

    if within_pairwise and between_pairwise:
        w_mean, b_mean = np.mean(within_pairwise), np.mean(between_pairwise)
        pooled_std = np.sqrt(
            (np.std(within_pairwise)**2 + np.std(between_pairwise)**2) / 2
        )
        cohens_d = (b_mean - w_mean) / pooled_std if pooled_std > 0 else 0
    else:
        cohens_d = 0

    return {
        "mean_within": float(mean_within),
        "mean_between": float(mean_between),
        "separation": float(separation),
        "silhouette": float(sil),
        "effective_dim": int(eff_dim),
        "cohens_d": float(cohens_d),
        "n_responses": len(all_embs),
    }


def main():
    print("B67: Attractor Basin Width Probe")
    print("=" * 70, flush=True)

    ccs_versions = load_ccs_versions(3)
    print(f"\nLoaded {len(ccs_versions)} CCS versions")
    for i, ccs in enumerate(ccs_versions):
        gist = (ccs.get("semantic_gist") or "")[:60]
        print(f"  {i}: {gist}", flush=True)

    results = {}
    coherent_embs = None

    for level_name, contradiction in CONTRADICTION_LEVELS.items():
        if contradiction:
            modified = [make_contradictory_ccs(c, contradiction) for c in ccs_versions]
        else:
            modified = ccs_versions

        embs = run_condition(
            level_name, modified,
            lambda ccs: serialize_b57(ccs, identity_only=True),
        )
        metrics = compute_condition_metrics(embs)
        results[level_name] = metrics

        if level_name == "0_coherent":
            coherent_embs = embs

    # Cross-condition: distance from coherent centroid
    if coherent_embs:
        coherent_all = [e for g in coherent_embs for e in g]
        if coherent_all:
            coherent_centroid = np.mean(coherent_all, axis=0)
            for level_name in CONTRADICTION_LEVELS:
                # recompute from saved metrics isn't possible, but we can note
                # the separation ratio gradient which is the key output
                pass

    # Print results table
    print("\n" + "=" * 85)
    print("BASIN WIDTH RESULTS")
    print("=" * 85, flush=True)

    header = (f"{'Level':<22} {'Corruption':>10} {'Sep':>7} {'Sil':>7} "
              f"{'d':>6} {'EffDim':>7} {'N':>3}")
    print(header)
    print("-" * 85)

    corruption_pcts = [0, 10, 25, 50, 75, 100]
    ordered_keys = list(CONTRADICTION_LEVELS.keys())

    for i, key in enumerate(ordered_keys):
        m = results[key]
        pct = corruption_pcts[i]
        print(f"{key:<22} {pct:>9}% {m['separation']:>7.3f} {m['silhouette']:>7.3f} "
              f"{m['cohens_d']:>6.2f} {m['effective_dim']:>7d} {m['n_responses']:>3d}",
              flush=True)

    # Basin shape analysis
    print(f"\n{'='*85}")
    print("BASIN SHAPE ANALYSIS")
    print(f"{'='*85}", flush=True)

    seps = [results[k]["separation"] for k in ordered_keys]
    sils = [results[k]["silhouette"] for k in ordered_keys]
    ds = [results[k]["cohens_d"] for k in ordered_keys]

    # Find the cliff: largest single-step drop in separation
    drops = [(seps[i] - seps[i+1], i, i+1) for i in range(len(seps)-1)]
    max_drop = max(drops, key=lambda x: x[0])
    cliff_from = corruption_pcts[max_drop[1]]
    cliff_to = corruption_pcts[max_drop[2]]

    print(f"  Separation range: {min(seps):.3f} — {max(seps):.3f}")
    print(f"  Largest drop: {max_drop[0]:.3f} between {cliff_from}% and {cliff_to}%")

    # Classify basin shape
    # (a) Monotonic: each step degrades
    monotonic = all(seps[i] >= seps[i+1] - 0.01 for i in range(len(seps)-1))
    # (b) Non-monotonic: some step improves
    non_monotonic = any(seps[i+1] > seps[i] + 0.01 for i in range(len(seps)-1))
    # (c) Sharp cliff: one step accounts for >50% of total degradation
    total_degradation = seps[0] - seps[-1]
    cliff_fraction = max_drop[0] / total_degradation if total_degradation > 0 else 0

    if non_monotonic:
        shape = "NON-MONOTONIC"
        improve_steps = [(i, corruption_pcts[i], corruption_pcts[i+1], seps[i+1] - seps[i])
                         for i in range(len(seps)-1) if seps[i+1] > seps[i] + 0.01]
        print(f"\n  >>> SHAPE: {shape}")
        print(f"  Improvement at: {', '.join(f'{s[1]}%→{s[2]}% (+{s[3]:.3f})' for s in improve_steps)}")
        print(f"  This supports the stress-as-practice hypothesis generalized.")
    elif cliff_fraction > 0.5:
        shape = "SHARP_CLIFF"
        print(f"\n  >>> SHAPE: {shape}")
        print(f"  Cliff accounts for {cliff_fraction:.0%} of total degradation")
        print(f"  Phase boundary between {cliff_from}% and {cliff_to}% corruption")
        print(f"  Consistent with B61 phase transition finding.")
    elif monotonic:
        shape = "MONOTONIC"
        print(f"\n  >>> SHAPE: {shape}")
        print(f"  Smooth degradation across all levels")
        print(f"  This would CONTRADICT the B61 phase boundary claim.")
    else:
        shape = "MIXED"
        print(f"\n  >>> SHAPE: {shape}")

    # Degradation curve
    print(f"\n  Degradation curve (% of coherent separation):")
    baseline = seps[0] if seps[0] > 0 else 1
    for i, key in enumerate(ordered_keys):
        pct = corruption_pcts[i]
        retained = seps[i] / baseline * 100
        bar = "█" * int(retained / 2) + "░" * (50 - int(retained / 2))
        print(f"    {pct:>3}% | {bar} {retained:5.1f}%")

    # Save
    output = {
        "results": results,
        "analysis": {
            "shape": shape,
            "cliff_from": cliff_from,
            "cliff_to": cliff_to,
            "cliff_fraction": float(cliff_fraction),
            "total_degradation": float(total_degradation),
            "corruption_levels": corruption_pcts,
            "separation_curve": seps,
            "silhouette_curve": sils,
            "cohens_d_curve": ds,
        },
        "metadata": {
            "probe": "B67",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "model": "gemma-4-26B-A4B-it",
            "embedding": "mxbai-embed-large",
            "n_ccs": 3,
            "n_prompts": 3,
            "total_queries": sum(r["n_responses"] for r in results.values()),
        },
    }

    out_path = os.path.join(DATA_DIR, "basin_width_probe.json")
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nSaved to {out_path}", flush=True)


if __name__ == "__main__":
    main()
