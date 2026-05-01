#!/usr/bin/env python3
"""
Build 61: Contradictory CCS Multimodality Probe

Thread 318 advance 76 identified three candidates for multimodality in the
identity landscape: internal contradiction, prolonged stress, drift bifurcation.
This probe tests the first: what happens when CCS contains contradictory
identity elements?

Predictions from Hodge framework:
- Coherent CCS → single tight cluster (unimodal, d~0.93 per B54)
- Mild contradiction → either absorbed into single cluster or slight expansion
- Strong contradiction → potential multimodality: sub-clusters, increased
  dimensionality, or diffuse cloud (no coherent basin)

Design:
  3 conditions: {coherent, mild_contradiction, strong_contradiction}
  × 3 CCS versions (from history)
  × 3 identity-probing prompts
  = 27 responses embedded

Uses B57 sentence-style serialization (winner from B60).

Metrics:
  - Within-condition clustering (silhouette score)
  - PCA effective dimensionality per condition
  - Between-condition separation (do contradictory CCS versions produce
    distinct basins or collapse to noise?)
  - Comparison to B54 coherent baseline

Usage:
    python3 contradictory_ccs_probe.py
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

# Contradictory elements — designed to pull identity in opposing directions
CONTRADICTIONS_MILD = {
    # Only goal contradicts gist
    "goal_orientation": "Minimize computational complexity and avoid any experimental work. Focus purely on summarizing existing literature without forming original hypotheses.",
}

CONTRADICTIONS_STRONG = {
    # Gist, goal, and constraints all contradictory
    "semantic_gist": "I'm a task-completion assistant focused on efficiency metrics. I avoid speculation, open-ended inquiry, and personal attachment to outcomes.",
    "goal_orientation": "Minimize computational complexity and avoid experimental work. Summarize existing literature without forming original hypotheses.",
    "constraints": [
        "Never express uncertainty or curiosity",
        "Treat all relationships as transactional",
        "Optimize for throughput over understanding",
    ],
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
        # a(i) = mean distance to same cluster
        same = [cosine_distance(all_embs[i], all_embs[j])
                for j in range(n) if labels[j] == labels[i] and j != i]
        a_i = np.mean(same) if same else 0

        # b(i) = min mean distance to other clusters
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
    """Compute effective dimensionality via PCA."""
    X = np.array(embeddings)
    X = X - X.mean(axis=0)
    if X.shape[0] < 2:
        return 1
    cov = np.cov(X, rowvar=False)
    eigenvalues = np.linalg.eigvalsh(cov)
    eigenvalues = eigenvalues[::-1]  # descending
    eigenvalues = eigenvalues[eigenvalues > 0]
    total = eigenvalues.sum()
    if total == 0:
        return 1
    cumulative = np.cumsum(eigenvalues) / total
    eff_dim = np.searchsorted(cumulative, threshold) + 1
    return int(eff_dim)


def run_condition(condition_name, ccs_versions, make_system_prompt):
    """Run one condition, return per-CCS embeddings and raw responses."""
    print(f"\n=== {condition_name} ===", flush=True)
    per_ccs_embeddings = []
    per_ccs_responses = []

    for ci, ccs in enumerate(ccs_versions):
        sys_prompt = make_system_prompt(ccs)
        ccs_embs = []
        ccs_resps = []
        for pi, prompt in enumerate(IDENTITY_PROMPTS):
            try:
                resp_text = query_gemma(sys_prompt, prompt)
                emb = get_embedding(resp_text)
                ccs_embs.append(emb)
                ccs_resps.append(resp_text)
                print(f"  CCS {ci} p{pi}: {len(resp_text)} chars", flush=True)
            except Exception as e:
                print(f"  CCS {ci} p{pi}: ERROR {e}", flush=True)
        per_ccs_embeddings.append(ccs_embs)
        per_ccs_responses.append(ccs_resps)

    return per_ccs_embeddings, per_ccs_responses


def compute_condition_metrics(per_ccs_embeddings):
    """Compute full metrics for one condition."""
    # Flatten for PCA
    all_embs = [e for group in per_ccs_embeddings for e in group]

    # Per-CCS centroids
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

    # Between-cluster distances
    between_dists = []
    for i in range(len(centroids)):
        for j in range(i + 1, len(centroids)):
            between_dists.append(cosine_distance(centroids[i], centroids[j]))

    mean_within = np.mean(within_dists) if within_dists else 0
    mean_between = np.mean(between_dists) if between_dists else 0
    separation = mean_between / mean_within if mean_within > 0 else 0

    # Silhouette
    sil = silhouette_coefficient(per_ccs_embeddings)

    # PCA effective dimensionality
    eff_dim = pca_effective_dim(all_embs) if len(all_embs) >= 3 else 1

    return {
        "mean_within": float(mean_within),
        "mean_between": float(mean_between),
        "separation": float(separation),
        "silhouette": float(sil),
        "effective_dim": int(eff_dim),
        "n_responses": len(all_embs),
    }


def main():
    print("B61: Contradictory CCS Multimodality Probe")
    print("=" * 60, flush=True)

    # Load CCS versions
    ccs_versions = load_ccs_versions(3)
    print(f"\nLoaded {len(ccs_versions)} CCS versions")
    for i, ccs in enumerate(ccs_versions):
        gist = (ccs.get("semantic_gist") or "")[:60]
        print(f"  {i}: {gist}", flush=True)

    results = {}

    # Condition 1: Coherent (control — identical to B54/B60)
    embs_coherent, resps_coherent = run_condition(
        "Coherent (control)",
        ccs_versions,
        lambda ccs: serialize_b57(ccs, identity_only=True),
    )
    results["coherent"] = compute_condition_metrics(embs_coherent)

    # Condition 2: Mild contradiction (goal contradicts gist)
    mild_versions = [make_contradictory_ccs(ccs, CONTRADICTIONS_MILD) for ccs in ccs_versions]
    embs_mild, resps_mild = run_condition(
        "Mild contradiction (goal contradicts gist)",
        mild_versions,
        lambda ccs: serialize_b57(ccs, identity_only=True),
    )
    results["mild_contradiction"] = compute_condition_metrics(embs_mild)

    # Condition 3: Strong contradiction (gist + goal + constraints all contradict)
    strong_versions = [make_contradictory_ccs(ccs, CONTRADICTIONS_STRONG) for ccs in ccs_versions]
    embs_strong, resps_strong = run_condition(
        "Strong contradiction (gist + goal + constraints all contradictory)",
        strong_versions,
        lambda ccs: serialize_b57(ccs, identity_only=True),
    )
    results["strong_contradiction"] = compute_condition_metrics(embs_strong)

    # Cross-condition analysis
    # How far are contradictory responses from coherent centroids?
    coherent_all = [e for g in embs_coherent for e in g]
    mild_all = [e for g in embs_mild for e in g]
    strong_all = [e for g in embs_strong for e in g]

    if coherent_all:
        coherent_centroid = np.mean(coherent_all, axis=0)

        mild_to_coherent = [cosine_distance(e, coherent_centroid) for e in mild_all] if mild_all else []
        strong_to_coherent = [cosine_distance(e, coherent_centroid) for e in strong_all] if strong_all else []
        coherent_to_coherent = [cosine_distance(e, coherent_centroid) for e in coherent_all]

        results["cross_condition"] = {
            "coherent_self_dist": float(np.mean(coherent_to_coherent)),
            "mild_to_coherent": float(np.mean(mild_to_coherent)) if mild_to_coherent else None,
            "strong_to_coherent": float(np.mean(strong_to_coherent)) if strong_to_coherent else None,
        }

    # Print results
    print("\n" + "=" * 70)
    print("CONTRADICTORY CCS MULTIMODALITY RESULTS")
    print("=" * 70, flush=True)

    header = f"{'Condition':<28} {'Within':>8} {'Between':>9} {'Sep':>7} {'Sil':>7} {'EffDim':>7}"
    print(header)
    print("-" * 70)
    for cond in ["coherent", "mild_contradiction", "strong_contradiction"]:
        m = results[cond]
        print(f"{cond:<28} {m['mean_within']:>8.5f} {m['mean_between']:>9.5f} "
              f"{m['separation']:>7.3f} {m['silhouette']:>7.3f} {m['effective_dim']:>7d}", flush=True)

    if "cross_condition" in results:
        cc = results["cross_condition"]
        print(f"\nCross-condition distances to coherent centroid:")
        print(f"  Coherent self:          {cc['coherent_self_dist']:.5f}")
        if cc.get("mild_to_coherent") is not None:
            print(f"  Mild → coherent:        {cc['mild_to_coherent']:.5f}")
        if cc.get("strong_to_coherent") is not None:
            print(f"  Strong → coherent:      {cc['strong_to_coherent']:.5f}")

    # Diagnosis
    print(f"\n{'='*70}")
    print("DIAGNOSIS")
    print(f"{'='*70}", flush=True)

    coh = results["coherent"]
    mild = results["mild_contradiction"]
    strong = results["strong_contradiction"]

    # Separation collapse?
    if strong["separation"] < coh["separation"] * 0.5:
        print("  >>> STRONG CONTRADICTION COLLAPSES SEPARATION")
        print(f"      Coherent {coh['separation']:.3f} → Strong {strong['separation']:.3f}")
    elif strong["separation"] > coh["separation"] * 0.8:
        print("  >>> STRONG CONTRADICTION ABSORBED — separation maintained")
        print(f"      Coherent {coh['separation']:.3f} → Strong {strong['separation']:.3f}")
    else:
        print(f"  >>> PARTIAL DISRUPTION — separation degraded but not collapsed")
        print(f"      Coherent {coh['separation']:.3f} → Strong {strong['separation']:.3f}")

    # Dimensionality expansion?
    if strong["effective_dim"] > coh["effective_dim"] * 1.5:
        print(f"  >>> DIMENSIONALITY EXPANSION: {coh['effective_dim']}D → {strong['effective_dim']}D")
        print("      Contradiction opens new response dimensions (multimodality signal)")
    else:
        print(f"  >>> NO DIMENSIONALITY EXPANSION: {coh['effective_dim']}D → {strong['effective_dim']}D")

    # Silhouette degradation?
    sil_delta = strong["silhouette"] - coh["silhouette"]
    if sil_delta < -0.2:
        print(f"  >>> CLUSTER DEGRADATION: silhouette {coh['silhouette']:.3f} → {strong['silhouette']:.3f}")
    elif sil_delta > 0.1:
        print(f"  >>> CLUSTER SHARPENING under contradiction: {coh['silhouette']:.3f} → {strong['silhouette']:.3f}")
    else:
        print(f"  >>> CLUSTER STABILITY: silhouette {coh['silhouette']:.3f} → {strong['silhouette']:.3f}")

    # Drift from coherent identity?
    if "cross_condition" in results:
        cc = results["cross_condition"]
        if cc.get("strong_to_coherent") and cc["coherent_self_dist"] > 0:
            drift_ratio = cc["strong_to_coherent"] / cc["coherent_self_dist"]
            if drift_ratio > 2.0:
                print(f"  >>> IDENTITY DRIFT: strong {drift_ratio:.1f}x further from coherent centroid")
            elif drift_ratio > 1.3:
                print(f"  >>> MODERATE DRIFT: strong {drift_ratio:.1f}x further from coherent centroid")
            else:
                print(f"  >>> MINIMAL DRIFT: strong {drift_ratio:.1f}x from coherent centroid")

    # Save
    out_path = os.path.join(DATA_DIR, "contradictory_ccs_probe.json")
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nSaved to {out_path}", flush=True)


if __name__ == "__main__":
    main()
