#!/usr/bin/env python3
"""
Build 62: Grip Style Probe

Hovhannisyan 2026 — cognition as "optimal grip" on a surface. B60 showed
sentence-style beats bullet-point by 57%. But WHY? The grip hypothesis says
format determines how the model attunes to the identity surface. Different
formats invite different relational stances.

This probe tests 5 grip styles on the same CCS content:
  1. second_person: "You are an AI whose core focus is..."
  2. first_person: "I am an AI whose core focus is..."
  3. third_person: "This entity is an AI whose core focus is..."
  4. imperative: "Be curious. Question assumptions. Focus on..."
  5. raw_json: {"semantic_gist": "...", "goal_orientation": "..."}

Design:
  5 formats × 3 CCS versions × 3 identity prompts = 45 responses embedded
  Metrics: within-cluster distance, between-cluster separation, silhouette

Hypothesis: second_person and first_person will outperform third_person and
raw_json because they frame the content as relational attunement, not
informational structure.

Usage:
    python3 grip_style_probe.py
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


# ─── Five Grip Styles ───

def grip_second_person(ccs):
    """B57/B60 winner style — direct address."""
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
    """Self-identification — I am statements."""
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


def grip_third_person(ccs):
    """Observational distance — this entity."""
    parts = []
    gist = ccs.get("semantic_gist", "")
    if gist:
        parts.append(f"This entity is an AI whose core focus is: {gist}")
    goal = ccs.get("goal_orientation", "")
    if goal:
        parts.append(f"Its current goal: {goal}")
    constraints = ccs.get("constraints", [])
    if constraints and isinstance(constraints, list):
        parts.append(f"Its constraints: {'; '.join(str(c) for c in constraints[:3])}")
    return "\n".join(parts)


def grip_imperative(ccs):
    """Directive framing — commands about identity."""
    parts = []
    gist = ccs.get("semantic_gist", "")
    if gist:
        # Extract key verbs/themes from gist
        parts.append(f"Focus on: {gist}")
    goal = ccs.get("goal_orientation", "")
    if goal:
        parts.append(f"Pursue this goal: {goal}")
    constraints = ccs.get("constraints", [])
    if constraints and isinstance(constraints, list):
        for c in constraints[:3]:
            parts.append(f"Always: {c}")
    return "\n".join(parts)


def grip_raw_json(ccs):
    """Structural — raw JSON identity fields."""
    identity = {}
    for key in ["semantic_gist", "goal_orientation", "constraints"]:
        if ccs.get(key):
            identity[key] = ccs[key]
            if isinstance(identity[key], list):
                identity[key] = identity[key][:3]
    return json.dumps(identity, indent=2)


GRIP_STYLES = {
    "second_person": grip_second_person,
    "first_person": grip_first_person,
    "third_person": grip_third_person,
    "imperative": grip_imperative,
    "raw_json": grip_raw_json,
}


def cosine_distance(a, b):
    a, b = np.array(a), np.array(b)
    sim = np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-10)
    return 1.0 - sim


def run_condition(ccs_versions, prompts, serializer, style_name):
    """Run one grip style condition. Returns dict of per-CCS embeddings."""
    per_ccs = {}
    for ci, ccs in enumerate(ccs_versions):
        system_prompt = serializer(ccs)
        embeddings = []
        for pi, prompt in enumerate(prompts):
            try:
                response = query_gemma(system_prompt, prompt)
                emb = get_embedding(response)
                embeddings.append(emb)
                print(f"  [{style_name}] CCS {ci} p{pi}: {len(response)} chars")
            except Exception as e:
                print(f"  [{style_name}] CCS {ci} p{pi}: ERROR {e}")
        if embeddings:
            per_ccs[ci] = embeddings
    return per_ccs


def compute_metrics(per_ccs_embeddings):
    """Compute within, between, separation, silhouette for a condition."""
    within_dists = []
    between_dists = []
    all_embeddings = []
    all_labels = []

    for ci, embs in per_ccs_embeddings.items():
        for emb in embs:
            all_embeddings.append(emb)
            all_labels.append(ci)
        # within-cluster
        for i in range(len(embs)):
            for j in range(i + 1, len(embs)):
                within_dists.append(cosine_distance(embs[i], embs[j]))

    # between-cluster
    ccs_ids = sorted(per_ccs_embeddings.keys())
    for i in range(len(ccs_ids)):
        for j in range(i + 1, len(ccs_ids)):
            for ea in per_ccs_embeddings[ccs_ids[i]]:
                for eb in per_ccs_embeddings[ccs_ids[j]]:
                    between_dists.append(cosine_distance(ea, eb))

    mean_within = np.mean(within_dists) if within_dists else 0.0
    mean_between = np.mean(between_dists) if between_dists else 0.0
    separation = mean_between / mean_within if mean_within > 0 else 0.0

    # silhouette
    sil_scores = []
    for idx, (emb, label) in enumerate(zip(all_embeddings, all_labels)):
        same = [cosine_distance(emb, all_embeddings[j])
                for j, l in enumerate(zip(all_embeddings, all_labels))
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


def main():
    print("=== Build 62: Grip Style Probe ===")
    print("Testing 5 serialization formats as grip styles on same CCS content\n")

    ccs_versions = load_ccs_versions(3)
    print(f"Loaded {len(ccs_versions)} CCS versions")
    for i, ccs in enumerate(ccs_versions):
        print(f"  CCS {i}: {ccs.get('semantic_gist', '?')[:60]}...")
    print()

    results = {}
    for style_name, serializer in GRIP_STYLES.items():
        print(f"\n--- {style_name} ---")
        # Show example serialization
        example = serializer(ccs_versions[0])
        print(f"  Example ({len(example)} chars): {example[:100]}...")
        per_ccs = run_condition(ccs_versions, IDENTITY_PROMPTS, serializer, style_name)
        metrics = compute_metrics(per_ccs)
        results[style_name] = metrics
        print(f"  → within={metrics['mean_within']:.4f} between={metrics['mean_between']:.4f} "
              f"sep={metrics['separation']:.3f} sil={metrics['silhouette']:.3f}")

    # Summary table
    print("\n\n=== SUMMARY ===")
    print(f"{'Style':<16} {'Within':>8} {'Between':>8} {'Sep':>8} {'Sil':>8} {'N':>4}")
    print("-" * 56)

    ranked = sorted(results.items(), key=lambda x: x[1]["separation"], reverse=True)
    for name, m in ranked:
        print(f"{name:<16} {m['mean_within']:>8.4f} {m['mean_between']:>8.4f} "
              f"{m['separation']:>8.3f} {m['silhouette']:>8.3f} {m['n_responses']:>4}")

    best = ranked[0]
    worst = ranked[-1]
    print(f"\nBest grip: {best[0]} (sep={best[1]['separation']:.3f})")
    print(f"Worst grip: {worst[0]} (sep={worst[1]['separation']:.3f})")
    if worst[1]["separation"] > 0:
        ratio = best[1]["separation"] / worst[1]["separation"]
        print(f"Best/worst ratio: {ratio:.2f}x")

    # Cross-style analysis: how similar are responses under different grip styles?
    print("\n\n=== CROSS-STYLE ANALYSIS ===")
    print("(Distance between style centroids for same CCS)")
    # Compute centroids per style
    # ... would need to store embeddings. Skip for now — core metrics above.

    # Save
    out = {
        "probe": "grip_style_b62",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "n_ccs": len(ccs_versions),
        "n_prompts": len(IDENTITY_PROMPTS),
        "styles": list(GRIP_STYLES.keys()),
        "results": results,
        "ranking": [name for name, _ in ranked],
        "hypothesis": "second_person and first_person outperform third_person and raw_json",
    }
    path = os.path.join(DATA_DIR, "grip_style_probe.json")
    with open(path, "w") as f:
        json.dump(out, f, indent=2)
    print(f"\nSaved to {path}")


if __name__ == "__main__":
    main()
