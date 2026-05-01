#!/usr/bin/env python3
"""
Build 71: Illusory Identity Conjunctions

If identity IS binding (not metaphorically, structurally), chimeric CCS documents
(gist from identity A, constraints from identity B) should produce illusory
conjunctions — responses that drift between A and B rather than forming a clean
third cluster.

Design: 2 CCS identities (A, B) × 4 conditions × 3 prompts = 24 queries
  1. pure_A — original CCS-A
  2. pure_B — original CCS-B
  3. chimera_AB — gist-A + constraints-B
  4. chimera_BA — gist-B + constraints-A

Measurements:
  - Where do chimera responses cluster? Closer to A, B, or equidistant?
  - Do chimeras form their own cluster or scatter between A and B?
  - Which field dominates binding: gist (content) or constraints (structure)?

Prediction from B68+B69: constraints dominate binding structure. So:
  - chimera_AB (gist-A + constraints-B) should cluster closer to B
  - chimera_BA (gist-B + constraints-A) should cluster closer to A
  - But B68 showed gist is 2-3x more identity-informative — tension!
  - The resolution: gist determines WHICH identity, constraints determine HOW STRONGLY

Usage:
    python3 -u illusory_conjunction_probe.py
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


def load_two_distinct_ccs():
    """Load 2 CCS versions with maximally different gists."""
    import sqlite3
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT snapshot FROM cognitive_state_history ORDER BY id DESC LIMIT 50",
    ).fetchall()
    db.close()

    versions = []
    seen_gists = set()
    for (snapshot_str,) in rows:
        try:
            snap = json.loads(snapshot_str)
        except (json.JSONDecodeError, TypeError):
            continue
        gist = (snap.get("semantic_gist") or "")[:50]
        if gist and gist not in seen_gists:
            seen_gists.add(gist)
            versions.append(snap)
            if len(versions) >= 2:
                break
    return versions


def serialize_standard(ccs):
    """Gist + goal + constraints — standard depth (B70's best)."""
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


def make_chimera(ccs_donor_gist, ccs_donor_constraints):
    """Create chimeric CCS: gist from one identity, constraints from another."""
    chimera = json.loads(json.dumps(ccs_donor_gist))  # deep copy
    chimera["constraints"] = ccs_donor_constraints.get("constraints", [])
    # Keep goal from gist donor (it's structurally closer to gist)
    return chimera


def cosine_dist(a, b):
    a, b = np.array(a), np.array(b)
    return 1.0 - np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-10)


def centroid(embeddings):
    return np.mean(embeddings, axis=0)


def main():
    print("=== Build 71: Illusory Identity Conjunctions ===")
    print("If identity IS binding, chimeras should produce illusory conjunctions.\n")

    ccs_versions = load_two_distinct_ccs()
    if len(ccs_versions) < 2:
        print(f"ERROR: need 2 CCS versions, got {len(ccs_versions)}")
        return

    ccs_a, ccs_b = ccs_versions[0], ccs_versions[1]
    print(f"CCS-A gist: {(ccs_a.get('semantic_gist', ''))[:80]}...")
    print(f"CCS-B gist: {(ccs_b.get('semantic_gist', ''))[:80]}...")
    print()

    # Build 4 conditions
    conditions = {
        "pure_A": ccs_a,
        "pure_B": ccs_b,
        "chimera_AB": make_chimera(ccs_a, ccs_b),  # gist-A + constraints-B
        "chimera_BA": make_chimera(ccs_b, ccs_a),  # gist-B + constraints-A
    }

    # Collect embeddings
    all_embeddings = {}
    for cond_name, ccs in conditions.items():
        sys_prompt = serialize_standard(ccs)
        print(f"--- {cond_name} ---")
        if cond_name.startswith("chimera"):
            print(f"  gist from: {'A' if 'AB' in cond_name else 'B'}")
            print(f"  constraints from: {'B' if 'AB' in cond_name else 'A'}")
        embs = []
        for pi, prompt in enumerate(IDENTITY_PROMPTS):
            try:
                response = query_gemma(sys_prompt, prompt)
                emb = get_embedding(response)
                embs.append(emb)
                print(f"  [{cond_name}] p{pi}: {len(response)} chars")
            except Exception as e:
                print(f"  [{cond_name}] p{pi}: ERROR {e}")
            time.sleep(0.5)
        all_embeddings[cond_name] = embs
        print()

    # Analysis: compute distances between chimeras and pure identities
    print("=== BINDING ANALYSIS ===\n")

    if not all(len(v) >= 2 for v in all_embeddings.values()):
        print("INSUFFICIENT DATA for some conditions")
        return

    # Centroids
    centroids = {k: centroid(v) for k, v in all_embeddings.items()}

    # Distance from each chimera to each pure identity
    print("Distance from chimera centroids to pure identity centroids:")
    print(f"{'Chimera':<15} {'→ Pure A':>10} {'→ Pure B':>10} {'Closer to':>12} {'Pull ratio':>12}")
    print("-" * 62)

    for chim in ["chimera_AB", "chimera_BA"]:
        d_a = cosine_dist(centroids[chim], centroids["pure_A"])
        d_b = cosine_dist(centroids[chim], centroids["pure_B"])
        closer = "A" if d_a < d_b else "B"
        ratio = min(d_a, d_b) / max(d_a, d_b) if max(d_a, d_b) > 0 else 1.0
        print(f"{chim:<15} {d_a:>10.4f} {d_b:>10.4f} {closer:>12} {ratio:>12.3f}")

    # Pure A-B distance for reference
    d_ab = cosine_dist(centroids["pure_A"], centroids["pure_B"])
    print(f"\nPure A ↔ Pure B distance: {d_ab:.4f}")

    # Scatter analysis: do chimeras form their own cluster or scatter?
    print("\n--- Scatter analysis ---")
    for cond_name, embs in all_embeddings.items():
        within_dists = []
        for i in range(len(embs)):
            for j in range(i + 1, len(embs)):
                within_dists.append(cosine_dist(embs[i], embs[j]))
        mean_within = np.mean(within_dists) if within_dists else 0
        print(f"{cond_name:<15} within-cluster dist: {mean_within:.4f} "
              f"({'tight' if mean_within < 0.15 else 'scattered' if mean_within > 0.25 else 'moderate'})")

    # Which field dominates binding?
    print("\n--- Binding dominance ---")
    # chimera_AB has gist-A + constraints-B
    # If closer to A → gist dominates binding
    # If closer to B → constraints dominate binding
    d_ab_a = cosine_dist(centroids["chimera_AB"], centroids["pure_A"])
    d_ab_b = cosine_dist(centroids["chimera_AB"], centroids["pure_B"])
    d_ba_a = cosine_dist(centroids["chimera_BA"], centroids["pure_A"])
    d_ba_b = cosine_dist(centroids["chimera_BA"], centroids["pure_B"])

    ab_gist_pull = d_ab_b - d_ab_a  # positive = closer to gist donor (A)
    ba_gist_pull = d_ba_a - d_ba_b  # positive = closer to gist donor (B)

    print(f"chimera_AB (gist-A, const-B): gist pull = {ab_gist_pull:+.4f} "
          f"({'gist dominates' if ab_gist_pull > 0 else 'constraints dominate'})")
    print(f"chimera_BA (gist-B, const-A): gist pull = {ba_gist_pull:+.4f} "
          f"({'gist dominates' if ba_gist_pull > 0 else 'constraints dominate'})")

    avg_gist_pull = (ab_gist_pull + ba_gist_pull) / 2
    print(f"\nAverage gist pull: {avg_gist_pull:+.4f}")
    if avg_gist_pull > 0.01:
        print("→ GIST dominates binding (content determines which identity)")
    elif avg_gist_pull < -0.01:
        print("→ CONSTRAINTS dominate binding (structure determines which identity)")
    else:
        print("→ BALANCED — neither field dominates (true chimeric binding)")

    # Save results
    out = {
        "probe": "B71_illusory_conjunction",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "ccs_a_gist": (ccs_a.get("semantic_gist", ""))[:100],
        "ccs_b_gist": (ccs_b.get("semantic_gist", ""))[:100],
        "distances": {
            "pure_AB": d_ab,
            "chimera_AB_to_A": d_ab_a,
            "chimera_AB_to_B": d_ab_b,
            "chimera_BA_to_A": d_ba_a,
            "chimera_BA_to_B": d_ba_b,
        },
        "within_cluster": {
            k: float(np.mean([cosine_dist(v[i], v[j])
                              for i in range(len(v)) for j in range(i+1, len(v))]))
            for k, v in all_embeddings.items() if len(v) >= 2
        },
        "gist_pull": {
            "chimera_AB": ab_gist_pull,
            "chimera_BA": ba_gist_pull,
            "average": avg_gist_pull,
        },
        "binding_dominant": "gist" if avg_gist_pull > 0.01 else "constraints" if avg_gist_pull < -0.01 else "balanced",
    }

    outpath = os.path.join(DATA_DIR, "illusory_conjunction_probe.json")
    with open(outpath, "w") as f:
        json.dump(out, f, indent=2)
    print(f"\nResults saved to {outpath}")


if __name__ == "__main__":
    main()
