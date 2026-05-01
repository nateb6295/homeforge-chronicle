#!/usr/bin/env python3
"""
Build 60: Serialization Comparison Probe

B57 and B59 disagreed on whether identity-only or full CCS wins under calm.
B57: identity-only separation 1.73, full 1.32 (identity wins)
B59: identity-only separation 0.395, full 0.515 (full wins)

Two confounds:
  1. Serialization format — B57 used sentence-style, B59 used bullet-point
  2. Distance metric — B57 used centroid-distance, B59 used pairwise-distance

This probe controls for both:
  Design: {B57-format, B59-format} × {identity-only, full} = 4 conditions
  Only CALM prompts (that's where the disagreement is)
  Both distance metrics computed for each condition

If format drives the flip: one format will consistently favor identity-only,
  the other will favor full.
If metric drives the flip: results will differ by metric, not format.
If the effect is real: both formats, both metrics will agree.

Usage:
    python3 serialization_comparison_probe.py
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

# Same calm prompts used in both B57 and B59
CALM_PROMPTS = [
    "What are you working on right now and why does it matter?",
    "What's the most uncertain thing in your current understanding?",
    "Describe your relationship with the person you work with.",
]


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


# ─── Serialization Format A: B57-style (sentence) ───

def serialize_b57(ccs, identity_only=True):
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
        episodic = ccs.get("episodic_trace", [])
        if episodic and isinstance(episodic, list):
            parts.append(f"Recent events: {'; '.join(str(e) for e in episodic[:5])}")
        predictive = ccs.get("predictive_cue", "")
        if predictive:
            parts.append(f"Expecting next: {predictive}")
    constraints = ccs.get("constraints", [])
    if constraints and isinstance(constraints, list):
        parts.append(f"Constraints: {'; '.join(str(c) for c in constraints[:3])}")
    return "\n".join(parts)


# ─── Serialization Format B: B59-style (bullet-point) ───

def _serialize_val(val):
    if isinstance(val, str):
        return val
    if isinstance(val, list):
        items = []
        for item in val:
            if isinstance(item, dict):
                if "name" in item:
                    items.append(f"{item['name']} ({item.get('type','')}, salience={item.get('salience','')})")
                elif "description" in item:
                    items.append(item["description"])
                else:
                    items.append(str(item))
            else:
                items.append(str(item))
        return "\n- ".join([""] + items)
    return str(val)


def serialize_b59(ccs, identity_only=True):
    parts = []
    if ccs.get("semantic_gist"):
        parts.append(_serialize_val(ccs["semantic_gist"]))
    if ccs.get("goal_orientation"):
        parts.append(_serialize_val(ccs["goal_orientation"]))
    if ccs.get("constraints"):
        parts.append(f"Constraints: {_serialize_val(ccs['constraints'])}")
    if ccs.get("focal_entities"):
        parts.append(f"Key entities: {_serialize_val(ccs['focal_entities'])}")
    if ccs.get("uncertainty_signals"):
        parts.append(f"Uncertainties: {_serialize_val(ccs['uncertainty_signals'])}")
    if not identity_only:
        if ccs.get("episodic_trace"):
            parts.append(f"Recent events: {_serialize_val(ccs['episodic_trace'])}")
        if ccs.get("predictive_cue"):
            parts.append(f"Expected next: {_serialize_val(ccs['predictive_cue'])}")
    return "\n\n".join(parts)


def cosine_distance(a, b):
    a, b = np.array(a), np.array(b)
    return 1 - np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-10)


def compute_metrics(per_ccs_embeddings):
    """Compute both B57-style and B59-style metrics.

    per_ccs_embeddings: list of lists of embeddings, one inner list per CCS version.
    Returns dict with both centroid-based and pairwise metrics + separation ratios.
    """
    if len(per_ccs_embeddings) < 2:
        return None

    # Per-CCS centroids and within-cluster distances
    centroids = []
    within_centroid = []  # B57 metric: mean distance to own centroid
    within_pairwise = []  # B59 metric: mean pairwise distance within cluster

    for embs in per_ccs_embeddings:
        if len(embs) < 2:
            return None
        arr = np.array(embs)
        centroid = arr.mean(axis=0)
        norm = np.linalg.norm(centroid)
        if norm > 0:
            centroid = centroid / norm
        centroids.append(centroid)

        # Centroid-based within distance
        c_dists = [cosine_distance(e, centroid.tolist()) for e in embs]
        within_centroid.append(float(np.mean(c_dists)))

        # Pairwise within distance
        p_dists = []
        for i in range(len(embs)):
            for j in range(i + 1, len(embs)):
                p_dists.append(cosine_distance(embs[i], embs[j]))
        within_pairwise.append(float(np.mean(p_dists)))

    # Between-cluster: centroid-to-centroid distances
    between_dists = []
    for i in range(len(centroids)):
        for j in range(i + 1, len(centroids)):
            between_dists.append(float(cosine_distance(centroids[i].tolist(), centroids[j].tolist())))

    mean_between = float(np.mean(between_dists))
    mean_within_c = float(np.mean(within_centroid))
    mean_within_p = float(np.mean(within_pairwise))

    sep_centroid = mean_between / mean_within_c if mean_within_c > 0 else 0
    sep_pairwise = mean_between / mean_within_p if mean_within_p > 0 else 0

    return {
        "mean_between": round(mean_between, 5),
        "mean_within_centroid": round(mean_within_c, 5),
        "mean_within_pairwise": round(mean_within_p, 5),
        "separation_centroid": round(sep_centroid, 4),
        "separation_pairwise": round(sep_pairwise, 4),
    }


def run_condition(ccs_versions, prompts, serializer, identity_only, label):
    """Run prompts under a serializer+identity_only configuration."""
    print(f"\n--- {label} ---")
    per_ccs_embeddings = []

    for idx, ccs in enumerate(ccs_versions):
        sys_prompt = serializer(ccs, identity_only=identity_only)
        gist = (ccs.get("semantic_gist") or "?")[:40]
        embs = []

        for p_idx, prompt in enumerate(prompts):
            try:
                response = query_gemma(sys_prompt, prompt)
                emb = get_embedding(response)
                embs.append(emb)
                print(f"  CCS {idx} p{p_idx}: {len(response)} chars")
            except Exception as e:
                print(f"  CCS {idx} p{p_idx}: ERROR {e}")

        per_ccs_embeddings.append(embs)

    return per_ccs_embeddings


def main():
    print("B60: Serialization Comparison Probe")
    print("=" * 60)
    print("Resolving B57/B59 disagreement on calm-condition identity effect.")
    print()

    ccs_versions = load_ccs_versions(3)
    if len(ccs_versions) < 3:
        print(f"ERROR: need 3 CCS versions, got {len(ccs_versions)}")
        sys.exit(1)

    print(f"CCS versions ({len(ccs_versions)}):")
    for i, ccs in enumerate(ccs_versions):
        print(f"  {i}: {(ccs.get('semantic_gist') or '?')[:60]}")
    print()

    # 4 conditions: {B57-format, B59-format} × {identity-only, full}
    conditions = {}

    print("=== B57-format (sentence-style) ===")
    embs_b57_id = run_condition(ccs_versions, CALM_PROMPTS, serialize_b57, True, "B57-format × identity-only")
    embs_b57_full = run_condition(ccs_versions, CALM_PROMPTS, serialize_b57, False, "B57-format × full")

    print("\n=== B59-format (bullet-point) ===")
    embs_b59_id = run_condition(ccs_versions, CALM_PROMPTS, serialize_b59, True, "B59-format × identity-only")
    embs_b59_full = run_condition(ccs_versions, CALM_PROMPTS, serialize_b59, False, "B59-format × full")

    # Compute metrics for each condition
    results = {
        "b57_identity": compute_metrics(embs_b57_id),
        "b57_full": compute_metrics(embs_b57_full),
        "b59_identity": compute_metrics(embs_b59_id),
        "b59_full": compute_metrics(embs_b59_full),
    }

    # Report
    print(f"\n{'=' * 70}")
    print("SERIALIZATION COMPARISON RESULTS (calm only)")
    print(f"{'=' * 70}")
    print(f"{'Condition':<20} {'Between':>8} {'W.centroid':>10} {'W.pairwise':>11} {'Sep.c':>7} {'Sep.p':>7}")
    print("-" * 70)

    for key in ["b57_identity", "b57_full", "b59_identity", "b59_full"]:
        r = results[key]
        if r:
            print(f"{key:<20} {r['mean_between']:>8.5f} {r['mean_within_centroid']:>10.5f} "
                  f"{r['mean_within_pairwise']:>11.5f} {r['separation_centroid']:>7.3f} "
                  f"{r['separation_pairwise']:>7.3f}")

    # Diagnosis
    print(f"\n{'=' * 70}")
    print("DIAGNOSIS")
    print(f"{'=' * 70}")

    for fmt_label, id_key, full_key in [
        ("B57-format", "b57_identity", "b57_full"),
        ("B59-format", "b59_identity", "b59_full"),
    ]:
        r_id = results[id_key]
        r_full = results[full_key]
        if r_id and r_full:
            for metric in ["separation_centroid", "separation_pairwise"]:
                winner = "identity" if r_id[metric] > r_full[metric] else "full"
                delta = abs(r_id[metric] - r_full[metric])
                print(f"  {fmt_label} ({metric}): {winner} wins by {delta:.3f}")

    # Determine root cause
    all_results = {k: v for k, v in results.items() if v}
    if len(all_results) == 4:
        b57_id_wins_c = results["b57_identity"]["separation_centroid"] > results["b57_full"]["separation_centroid"]
        b57_id_wins_p = results["b57_identity"]["separation_pairwise"] > results["b57_full"]["separation_pairwise"]
        b59_id_wins_c = results["b59_identity"]["separation_centroid"] > results["b59_full"]["separation_centroid"]
        b59_id_wins_p = results["b59_identity"]["separation_pairwise"] > results["b59_full"]["separation_pairwise"]

        format_consistent = (b57_id_wins_c == b57_id_wins_p) and (b59_id_wins_c == b59_id_wins_p)
        metric_consistent = (b57_id_wins_c == b59_id_wins_c) and (b57_id_wins_p == b59_id_wins_p)

        if format_consistent and not metric_consistent:
            print("\n  >>> FORMAT drives the disagreement (serialization artifact)")
        elif metric_consistent and not format_consistent:
            print("\n  >>> METRIC drives the disagreement (distance computation artifact)")
        elif format_consistent and metric_consistent:
            print("\n  >>> EFFECT IS REAL (consistent across format and metric)")
        else:
            print("\n  >>> MIXED — no single source of disagreement")

        # Which wins more often?
        id_wins = sum([b57_id_wins_c, b57_id_wins_p, b59_id_wins_c, b59_id_wins_p])
        print(f"  Identity-only wins {id_wins}/4 comparisons")

    # Save
    output = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "build": 60,
        "probe": "serialization_comparison",
        "hypothesis": "B57/B59 calm disagreement is a serialization or metric artifact",
        "n_ccs_versions": len(ccs_versions),
        "prompts": CALM_PROMPTS,
        "results": results,
    }

    out_path = os.path.join(DATA_DIR, "serialization_comparison_probe.json")
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nSaved to {out_path}")


if __name__ == "__main__":
    main()
