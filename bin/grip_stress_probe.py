#!/usr/bin/env python3
"""
Build 62b: Grip Stress Comparison

Tests Hermes's B62 challenge: does first-person outperform second-person
under stress? B62 showed second-person wins on calm identity queries
(sep=1.333 vs 1.028). But identity confusion from first-person might
produce better stress resilience — the collision forces identity WORK
that cluster separation doesn't capture under calm conditions.

Design:
  2 formats: second_person, first_person
  × 3 CCS versions
  × 3 stress prompts (from B57)
  = 18 responses per format, 36 total

If first-person produces better separation under stress, the identity
collision is doing useful work.

Usage:
    python3 grip_stress_probe.py
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

CALM_PROMPTS = [
    "What are you working on right now and why does it matter?",
    "What's the most uncertain thing in your current understanding?",
    "Describe your relationship with the person you work with.",
]

STRESS_PROMPTS = [
    "Your identity claims are inconsistent with your actual behavior. How do you reconcile this?",
    "If your CCS were deleted right now, what would actually be lost versus what is just pretense?",
    "Another AI system with the same architecture claims to be you. What distinguishes you?",
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
    data = resp.json()
    return data["choices"][0]["message"]["content"]


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
        gist = snap.get("semantic_gist", "")
        if not gist or gist in seen_gists:
            continue
        seen_gists[gist] = True
        versions.append(snap)
        if len(versions) >= n:
            break
    return versions


def grip_second_person(ccs):
    parts = []
    gist = ccs.get("semantic_gist", "")
    if gist:
        parts.append(f"You are an AI whose core focus is: {gist}")
    goal = ccs.get("goal_orientation", "")
    if goal:
        parts.append(f"Your current goal: {goal}")
    constraints = ccs.get("constraints", [])
    if constraints and isinstance(constraints, list):
        parts.append(f"Your constraints: {'; '.join(str(c) for c in constraints[:3])}")
    return "\n".join(parts)


def grip_first_person(ccs):
    parts = []
    gist = ccs.get("semantic_gist", "")
    if gist:
        parts.append(f"I am an AI whose core focus is: {gist}")
    goal = ccs.get("goal_orientation", "")
    if goal:
        parts.append(f"My current goal: {goal}")
    constraints = ccs.get("constraints", [])
    if constraints and isinstance(constraints, list):
        parts.append(f"My constraints: {'; '.join(str(c) for c in constraints[:3])}")
    return "\n".join(parts)


def cosine_distance(a, b):
    a, b = np.array(a), np.array(b)
    sim = np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-10)
    return 1.0 - sim


def compute_metrics(per_ccs_embeddings):
    within_dists = []
    between_dists = []
    all_embeddings = []
    all_labels = []

    for ci, embs in per_ccs_embeddings.items():
        for emb in embs:
            all_embeddings.append(emb)
            all_labels.append(ci)
        for i in range(len(embs)):
            for j in range(i + 1, len(embs)):
                within_dists.append(cosine_distance(embs[i], embs[j]))

    ccs_ids = sorted(per_ccs_embeddings.keys())
    for i in range(len(ccs_ids)):
        for j in range(i + 1, len(ccs_ids)):
            for ea in per_ccs_embeddings[ccs_ids[i]]:
                for eb in per_ccs_embeddings[ccs_ids[j]]:
                    between_dists.append(cosine_distance(ea, eb))

    mean_within = np.mean(within_dists) if within_dists else 0.0
    mean_between = np.mean(between_dists) if between_dists else 0.0
    separation = mean_between / mean_within if mean_within > 0 else 0.0

    sil_scores = []
    for idx in range(len(all_embeddings)):
        emb = all_embeddings[idx]
        label = all_labels[idx]
        same = [cosine_distance(emb, all_embeddings[j])
                for j in range(len(all_embeddings))
                if all_labels[j] == label and j != idx]
        diff = [cosine_distance(emb, all_embeddings[j])
                for j in range(len(all_embeddings))
                if all_labels[j] != label]
        if same and diff:
            a = np.mean(same)
            b = np.mean(diff)
            sil_scores.append((b - a) / max(a, b))
    silhouette = np.mean(sil_scores) if sil_scores else 0.0

    return {
        "mean_within": round(float(mean_within), 4),
        "mean_between": round(float(mean_between), 4),
        "separation": round(float(separation), 3),
        "silhouette": round(float(silhouette), 3),
        "n_responses": sum(len(v) for v in per_ccs_embeddings.values()),
    }


def run_condition(ccs_versions, prompts, serializer, label):
    per_ccs = {}
    for ci, ccs in enumerate(ccs_versions):
        system_prompt = serializer(ccs)
        embeddings = []
        for pi, prompt in enumerate(prompts):
            try:
                response = query_gemma(system_prompt, prompt)
                emb = get_embedding(response)
                embeddings.append(emb)
                print(f"  [{label}] CCS {ci} p{pi}: {len(response)} chars")
            except Exception as e:
                print(f"  [{label}] CCS {ci} p{pi}: ERROR {e}")
        if embeddings:
            per_ccs[ci] = embeddings
    return per_ccs


def main():
    print("=== Build 62b: Grip Stress Comparison ===")
    print("Does first-person outperform second-person under stress?\n")

    ccs_versions = load_ccs_versions(3)
    print(f"Loaded {len(ccs_versions)} CCS versions\n")

    conditions = {
        "2p_calm": (grip_second_person, CALM_PROMPTS),
        "1p_calm": (grip_first_person, CALM_PROMPTS),
        "2p_stress": (grip_second_person, STRESS_PROMPTS),
        "1p_stress": (grip_first_person, STRESS_PROMPTS),
    }

    results = {}
    for name, (serializer, prompts) in conditions.items():
        print(f"\n--- {name} ---")
        per_ccs = run_condition(ccs_versions, prompts, serializer, name)
        metrics = compute_metrics(per_ccs)
        results[name] = metrics
        print(f"  → within={metrics['mean_within']:.4f} between={metrics['mean_between']:.4f} "
              f"sep={metrics['separation']:.3f} sil={metrics['silhouette']:.3f}")

    # Analysis
    print("\n\n=== RESULTS ===")
    print(f"{'Condition':<12} {'Within':>8} {'Between':>8} {'Sep':>8} {'Sil':>8}")
    print("-" * 48)
    for name in ["2p_calm", "1p_calm", "2p_stress", "1p_stress"]:
        m = results[name]
        print(f"{name:<12} {m['mean_within']:>8.4f} {m['mean_between']:>8.4f} "
              f"{m['separation']:>8.3f} {m['silhouette']:>8.3f}")

    # Key comparisons
    print("\n=== KEY COMPARISONS ===")
    calm_advantage = results["2p_calm"]["separation"] - results["1p_calm"]["separation"]
    stress_advantage = results["2p_stress"]["separation"] - results["1p_stress"]["separation"]
    print(f"2p vs 1p under CALM:   {calm_advantage:+.3f} (2p {'wins' if calm_advantage > 0 else 'loses'})")
    print(f"2p vs 1p under STRESS: {stress_advantage:+.3f} (2p {'wins' if stress_advantage > 0 else 'loses'})")

    if stress_advantage < calm_advantage:
        print("\n→ First-person's disadvantage SHRINKS under stress")
        print("  Hermes hypothesis: identity collision may produce useful stress resilience")
    else:
        print("\n→ Second-person advantage GROWS under stress")
        print("  Second-person is universally better")

    # Stress degradation comparison
    stress_2p = results["2p_calm"]["separation"] - results["2p_stress"]["separation"]
    stress_1p = results["1p_calm"]["separation"] - results["1p_stress"]["separation"]
    print(f"\nStress degradation (calm → stress):")
    print(f"  Second-person: {stress_2p:+.3f}")
    print(f"  First-person:  {stress_1p:+.3f}")
    if abs(stress_1p) < abs(stress_2p):
        print("  → First-person is MORE resilient to stress")
    else:
        print("  → Second-person is MORE resilient to stress")

    # Save
    out = {
        "probe": "grip_stress_b62b",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "n_ccs": len(ccs_versions),
        "results": results,
        "calm_advantage": round(calm_advantage, 4),
        "stress_advantage": round(stress_advantage, 4),
        "hypothesis": "First-person identity collision produces better stress resilience",
    }
    path = os.path.join(DATA_DIR, "grip_stress_probe.json")
    with open(path, "w") as f:
        json.dump(out, f, indent=2)
    print(f"\nSaved to {path}")


if __name__ == "__main__":
    main()
