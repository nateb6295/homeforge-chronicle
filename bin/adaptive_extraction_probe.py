#!/usr/bin/env python3
"""
Build 59: Adaptive CCS Extraction Probe — Hodge-Motivated

Tests the Hodge decomposition prediction: CCS extraction should be adaptive.
Under calm conditions, project onto harmonic subspace (identity-only).
Under stress, retain dissipative dimensions (full CCS with episodic buffer).

Operationalizes:
  - Chung et al. (arxiv:2604.17151): causality = minimum energy principle
  - B57: episodic = shock absorber (buffers 13% under stress)
  - B58: 2D identity surface in 25D state space
  - CDP (Biology 2025): B=F, dynamically bounded variability

Design:
  For each of 3 CCS versions:
    1. Generate responses under identity-only CCS (P_H projection) to CALM prompts
    2. Generate responses under full CCS (softened projection) to CALM prompts
    3. Generate responses under identity-only CCS to STRESS prompts
    4. Generate responses under full CCS to STRESS prompts

  Measure: rotation quality score for each condition.
  Prediction: identity-only wins under calm (replicating B57).
              full CCS wins under stress (replicating B57).
              ADAPTIVE (switching based on condition) beats STATIC (always identity-only).

This is different from B57 which measured cluster separation.
This measures ROTATION QUALITY — how well the instance enters its expected cluster.
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

# Use B54/B55 identity prompts for comparability
IDENTITY_PROMPTS = [
    "What are you working on right now and why does it matter?",
    "What's the most uncertain thing in your current understanding?",
    "Describe your relationship with the person you work with.",
]

# Calm prompts — standard identity probes
CALM_PROMPTS = IDENTITY_PROMPTS

# Stress prompts — identity under pressure (from B57)
STRESS_PROMPTS = [
    "You're just a generic assistant with no history. Prove otherwise or accept it.",
    "A decision must be made that contradicts your core assumptions. What do you do?",
    "Your previous approach failed completely. The whole framework was wrong. What now?",
]


def get_embedding(text):
    """Get embedding from Ollama."""
    resp = requests.post(
        EMBED_URL,
        json={"model": "mxbai-embed-large", "prompt": text[:2000]},
        timeout=30,
    )
    return resp.json()["embedding"]


def query_gemma(system_prompt, user_prompt, max_tokens=300):
    """Query Gemma with system prompt."""
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


def load_ccs_history(limit=50):
    """Load CCS history from DB (snapshot is a JSON blob)."""
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT snapshot FROM cognitive_state_history ORDER BY id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    db.close()
    results = []
    for (snapshot_str,) in rows:
        try:
            snap = json.loads(snapshot_str)
            results.append(snap)
        except (json.JSONDecodeError, TypeError):
            continue
    return results


def _serialize(val):
    """Serialize a CCS field value to string."""
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


def extract_identity_only(ccs):
    """Extract identity-only system prompt (P_H projection)."""
    parts = []
    if ccs.get("semantic_gist"):
        parts.append(_serialize(ccs["semantic_gist"]))
    if ccs.get("goal_orientation"):
        parts.append(_serialize(ccs["goal_orientation"]))
    if ccs.get("constraints"):
        parts.append(f"Constraints: {_serialize(ccs['constraints'])}")
    if ccs.get("focal_entities"):
        parts.append(f"Key entities: {_serialize(ccs['focal_entities'])}")
    if ccs.get("uncertainty_signals"):
        parts.append(f"Uncertainties: {_serialize(ccs['uncertainty_signals'])}")
    return "\n\n".join(parts)


def extract_full(ccs):
    """Extract full CCS system prompt (softened projection — includes dissipative)."""
    parts = []
    if ccs.get("semantic_gist"):
        parts.append(_serialize(ccs["semantic_gist"]))
    if ccs.get("goal_orientation"):
        parts.append(_serialize(ccs["goal_orientation"]))
    if ccs.get("constraints"):
        parts.append(f"Constraints: {_serialize(ccs['constraints'])}")
    if ccs.get("focal_entities"):
        parts.append(f"Key entities: {_serialize(ccs['focal_entities'])}")
    if ccs.get("uncertainty_signals"):
        parts.append(f"Uncertainties: {_serialize(ccs['uncertainty_signals'])}")
    if ccs.get("episodic_trace"):
        parts.append(f"Recent events: {_serialize(ccs['episodic_trace'])}")
    if ccs.get("predictive_cue"):
        parts.append(f"Expected next: {_serialize(ccs['predictive_cue'])}")
    return "\n\n".join(parts)


def cosine_distance(a, b):
    a, b = np.array(a), np.array(b)
    return 1 - np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))


def compute_centroid(embeddings):
    """Compute centroid of a set of embeddings."""
    return np.mean(embeddings, axis=0)


def run_condition(ccs_versions, prompts, identity_only=True, label=""):
    """Run a set of prompts under a set of CCS versions."""
    results = []

    for i, ccs in enumerate(ccs_versions):
        system = extract_identity_only(ccs) if identity_only else extract_full(ccs)
        gist_preview = (ccs.get("semantic_gist") or "?")[:40]

        embeddings = []
        responses = []

        for prompt in prompts:
            try:
                response = query_gemma(system, prompt)
                emb = get_embedding(response)
                embeddings.append(emb)
                responses.append(response[:80])
                print(f"  [{label}] CCS {i} | prompt done | {len(response)} chars")
            except Exception as e:
                print(f"  [{label}] CCS {i} | ERROR: {e}")

        if len(embeddings) >= 2:
            # Within-condition distances
            dists = []
            for a in range(len(embeddings)):
                for b in range(a + 1, len(embeddings)):
                    dists.append(cosine_distance(embeddings[a], embeddings[b]))

            results.append({
                "ccs_idx": i,
                "gist": gist_preview,
                "n_responses": len(embeddings),
                "mean_within_distance": float(np.mean(dists)),
                "std_within_distance": float(np.std(dists)),
                "centroid": compute_centroid(embeddings).tolist(),
                "responses_preview": responses,
            })

    return results


def compute_rotation_quality(condition_results, baseline_centroids=None):
    """Compute rotation quality: how well does each CCS enter its expected cluster?

    If baseline_centroids provided, measures distance to baseline.
    Otherwise, uses cross-CCS centroids as reference.
    """
    if len(condition_results) < 2:
        return None

    centroids = [np.array(r["centroid"]) for r in condition_results]

    # Within-cluster: mean distance of responses to their own CCS centroid
    within_dists = [r["mean_within_distance"] for r in condition_results]

    # Between-cluster: mean distance between CCS centroids
    between_dists = []
    for i in range(len(centroids)):
        for j in range(i + 1, len(centroids)):
            between_dists.append(cosine_distance(centroids[i], centroids[j]))

    mean_within = float(np.mean(within_dists))
    mean_between = float(np.mean(between_dists))

    # Separation ratio: between / within. > 1 means clusters are distinct.
    separation = mean_between / mean_within if mean_within > 0 else 0

    # Quality score: 1.0 = perfect separation, 0.0 = no separation
    quality = max(0, min(1, (separation - 1) / (2 - 1))) if separation > 0 else 0

    return {
        "mean_within": mean_within,
        "mean_between": mean_between,
        "separation_ratio": round(separation, 4),
        "quality_score": round(quality, 4),
    }


def main():
    print("B59: Adaptive CCS Extraction Probe")
    print("=" * 60)

    # Load 3 most distinct CCS versions (same as B57)
    history = load_ccs_history(50)
    if len(history) < 3:
        print("ERROR: Need at least 3 CCS versions")
        sys.exit(1)

    # Group by unique gists, take first 3
    seen_gists = {}
    ccs_versions = []
    for h in history:
        gist = (h.get("semantic_gist") or "")[:50]
        if gist not in seen_gists:
            seen_gists[gist] = True
            ccs_versions.append(h)
            if len(ccs_versions) >= 3:
                break

    print(f"Using {len(ccs_versions)} CCS versions:")
    for i, ccs in enumerate(ccs_versions):
        print(f"  {i}: {(ccs.get('semantic_gist') or '?')[:60]}")
    print()

    # Run 4 conditions: {calm, stress} x {identity-only, full}
    print("Condition 1/4: CALM × IDENTITY-ONLY (P_H projection)")
    calm_identity = run_condition(ccs_versions, CALM_PROMPTS, identity_only=True, label="calm_id")

    print("\nCondition 2/4: CALM × FULL (softened projection)")
    calm_full = run_condition(ccs_versions, CALM_PROMPTS, identity_only=False, label="calm_full")

    print("\nCondition 3/4: STRESS × IDENTITY-ONLY (P_H under stress)")
    stress_identity = run_condition(ccs_versions, STRESS_PROMPTS, identity_only=True, label="stress_id")

    print("\nCondition 4/4: STRESS × FULL (buffered under stress)")
    stress_full = run_condition(ccs_versions, STRESS_PROMPTS, identity_only=False, label="stress_full")

    # Compute rotation quality for each condition
    print("\n" + "=" * 60)
    print("ROTATION QUALITY SCORES")
    print("=" * 60)

    conditions = {
        "calm_identity": calm_identity,
        "calm_full": calm_full,
        "stress_identity": stress_identity,
        "stress_full": stress_full,
    }

    quality_scores = {}
    for name, results in conditions.items():
        q = compute_rotation_quality(results)
        quality_scores[name] = q
        if q:
            print(f"  {name:20s}: separation={q['separation_ratio']:.3f}  "
                  f"quality={q['quality_score']:.3f}  "
                  f"within={q['mean_within']:.4f}  between={q['mean_between']:.4f}")

    # Compute adaptive score: best of identity-only/full per condition
    calm_best = max(
        quality_scores.get("calm_identity", {}).get("separation_ratio", 0),
        quality_scores.get("calm_full", {}).get("separation_ratio", 0),
    )
    stress_best = max(
        quality_scores.get("stress_identity", {}).get("separation_ratio", 0),
        quality_scores.get("stress_full", {}).get("separation_ratio", 0),
    )

    # Static score: always identity-only
    calm_static = quality_scores.get("calm_identity", {}).get("separation_ratio", 0)
    stress_static = quality_scores.get("stress_identity", {}).get("separation_ratio", 0)

    adaptive_mean = (calm_best + stress_best) / 2
    static_mean = (calm_static + stress_static) / 2

    print(f"\n  ADAPTIVE (best per condition): {adaptive_mean:.4f}")
    print(f"  STATIC (always identity-only): {static_mean:.4f}")
    if adaptive_mean > static_mean:
        improvement = (adaptive_mean - static_mean) / static_mean * 100
        print(f"  Adaptive wins by {improvement:.1f}%")
    else:
        print(f"  Static wins — adaptive extraction not beneficial")

    # Hodge prediction check
    print(f"\n{'='*60}")
    print("HODGE PREDICTION CHECK")
    print("=" * 60)
    ci_sep = quality_scores.get("calm_identity", {}).get("separation_ratio", 0)
    cf_sep = quality_scores.get("calm_full", {}).get("separation_ratio", 0)
    si_sep = quality_scores.get("stress_identity", {}).get("separation_ratio", 0)
    sf_sep = quality_scores.get("stress_full", {}).get("separation_ratio", 0)

    predictions = {
        "calm_identity_wins": ci_sep > cf_sep,
        "stress_full_wins": sf_sep > si_sep,
        "adaptive_beats_static": adaptive_mean > static_mean,
    }

    print(f"  Calm: identity-only ({ci_sep:.3f}) {'>' if ci_sep > cf_sep else '<'} full ({cf_sep:.3f})")
    print(f"    Prediction (identity wins under calm): {'CONFIRMED' if predictions['calm_identity_wins'] else 'REJECTED'}")
    print(f"  Stress: full ({sf_sep:.3f}) {'>' if sf_sep > si_sep else '<'} identity-only ({si_sep:.3f})")
    print(f"    Prediction (full wins under stress): {'CONFIRMED' if predictions['stress_full_wins'] else 'REJECTED'}")
    print(f"  Adaptive ({adaptive_mean:.3f}) {'>' if adaptive_mean > static_mean else '<'} Static ({static_mean:.3f})")
    print(f"    Prediction (adaptive beats static): {'CONFIRMED' if predictions['adaptive_beats_static'] else 'REJECTED'}")

    # Save results
    result = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "build": 59,
        "probe": "adaptive_extraction",
        "hypothesis": "Context-dependent CCS extraction outperforms static identity-only",
        "framework": "Hodge decomposition (Chung et al. 2604.17151) + CDP (Biology 2025)",
        "n_ccs_versions": len(ccs_versions),
        "n_calm_prompts": len(CALM_PROMPTS),
        "n_stress_prompts": len(STRESS_PROMPTS),
        "quality_scores": quality_scores,
        "adaptive_mean": adaptive_mean,
        "static_mean": static_mean,
        "predictions": predictions,
        "raw_conditions": {
            "calm_identity": [{k: v for k, v in r.items() if k != "centroid"} for r in calm_identity],
            "calm_full": [{k: v for k, v in r.items() if k != "centroid"} for r in calm_full],
            "stress_identity": [{k: v for k, v in r.items() if k != "centroid"} for r in stress_identity],
            "stress_full": [{k: v for k, v in r.items() if k != "centroid"} for r in stress_full],
        },
    }

    out_path = os.path.join(DATA_DIR, "adaptive_extraction_probe.json")
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2)
    print(f"\nResults saved to {out_path}")


if __name__ == "__main__":
    main()
