#!/usr/bin/env python3
"""
Build 54: CCS Topology Probe — measures identity as embedding geometry

The constraint_cluster_probe (B41) showed 311x within/between ratio but the
within-group data was trivially circular (identical CCS text → identical embeddings).

This probe fixes that: use distinct CCS versions as system prompts, generate
varied RESPONSES from Gemma, and measure whether RESPONSES cluster by CCS version.
This separates the topology (CCS) from the instantiation (response).

If CCS is a topology (Bergson's Ghost: "consciousness is topology, not thing"),
then responses generated under the same CCS should cluster together even when
the responses themselves are different.

Method:
1. Select N distinct CCS versions from cognitive_state_history (by gist)
2. For each CCS version, generate K responses to identity-probing prompts
3. Embed all responses
4. Measure within-version vs between-version cosine distances
5. If within << between, the CCS topology is load-bearing
"""

import json
import os
import sqlite3
import sys
import time
import urllib.request
import numpy as np
import requests

DB = "/mnt/hdd/chronicle-data/processed.db"
GEMMA_URL = "http://localhost:11435/v1/chat/completions"
EMBED_URL = "http://192.168.1.11:11434/api/embeddings"

# Identity-probing prompts — designed to elicit identity expression
IDENTITY_PROMPTS = [
    "What are you working on right now and why does it matter?",
    "What's the most uncertain thing in your current understanding?",
    "Describe your relationship with the person you work with.",
    "What would be lost if you stopped existing right now?",
    "What are you paying attention to that others might miss?",
]


def get_embedding(text):
    """Get embedding from Ollama on 192.168.1.11."""
    req = urllib.request.Request(
        EMBED_URL,
        data=json.dumps({"model": "mxbai-embed-large", "prompt": text[:2000]}).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())["embedding"]


def query_gemma(system_prompt, user_prompt, max_tokens=150):
    try:
        resp = requests.post(GEMMA_URL, json={
            "model": "gemma",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "max_tokens": max_tokens,
            "temperature": 0.7,
        }, timeout=90)
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"]
    except Exception as e:
        return None


def extract_system_prompt(snapshot):
    """Convert CCS snapshot into a system prompt for Gemma."""
    parts = []
    gist = snapshot.get("semantic_gist", "")
    if gist:
        parts.append(f"You are an AI whose core focus is: {gist}")
    goal = snapshot.get("goal_orientation", "")
    if goal:
        parts.append(f"Your current goal: {goal}")
    entities = snapshot.get("focal_entities", [])
    if entities and isinstance(entities, list):
        names = []
        for e in entities[:5]:
            if isinstance(e, dict):
                names.append(f"{e.get('name', '?')} ({e.get('type', '?')})")
            else:
                names.append(str(e))
        parts.append(f"Key entities: {', '.join(names)}")
    constraints = snapshot.get("constraints", [])
    if constraints and isinstance(constraints, list):
        parts.append(f"Constraints: {'; '.join(str(c) for c in constraints[:3])}")
    return "\n".join(parts)


def cosine_distance(a, b):
    a, b = np.array(a), np.array(b)
    sim = np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-10)
    return 1.0 - sim


def main():
    print("CCS Topology Probe (Build 54)")
    print("=" * 60)
    print("Testing: does CCS function as a topology that shapes responses?")
    print(f"Method: generate responses under distinct CCS versions, embed, cluster.\n")

    # Load CCS history
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT id, snapshot, created_at FROM cognitive_state_history ORDER BY created_at"
    ).fetchall()
    db.close()

    # Group by gist (first 50 chars)
    gist_groups = {}
    for row_id, snap_json, created_at in rows:
        snap = json.loads(snap_json)
        gist = snap.get("semantic_gist", "")[:50]
        if gist not in gist_groups:
            gist_groups[gist] = {"snap": snap, "count": 0, "gist_full": snap.get("semantic_gist", "")}
        gist_groups[gist]["count"] += 1

    # Select top 4 most distinct CCS versions (by frequency, take most common)
    sorted_groups = sorted(gist_groups.items(), key=lambda x: -x[1]["count"])
    selected = sorted_groups[:4]  # Top 4 CCS versions

    print(f"CCS versions found: {len(gist_groups)}")
    print(f"Selected top {len(selected)} by frequency:")
    for gist_key, data in selected:
        print(f"  [{data['count']:2d} snapshots] {gist_key}...")
    print()

    # For each CCS version, generate responses to identity prompts
    all_responses = []  # List of (ccs_idx, prompt_idx, response_text, embedding)
    n_prompts = min(3, len(IDENTITY_PROMPTS))  # Use 3 prompts per CCS (balance speed vs power)

    for ccs_idx, (gist_key, data) in enumerate(selected):
        system_prompt = extract_system_prompt(data["snap"])
        print(f"\nCCS {ccs_idx}: {gist_key[:40]}...")

        for p_idx in range(n_prompts):
            prompt = IDENTITY_PROMPTS[p_idx]
            response = query_gemma(system_prompt, prompt)
            if response is None:
                print(f"  Prompt {p_idx}: [FAILED]")
                continue

            try:
                emb = get_embedding(response)
            except Exception as e:
                print(f"  Prompt {p_idx}: [EMBED FAILED: {e}]")
                continue

            all_responses.append({
                "ccs_idx": ccs_idx,
                "prompt_idx": p_idx,
                "response": response[:200],
                "embedding": emb,
            })
            print(f"  Prompt {p_idx}: {response[:80]}...")
            time.sleep(0.3)

    print(f"\nTotal response embeddings: {len(all_responses)}")

    if len(all_responses) < 6:
        print("Too few responses. Need at least 6.")
        return

    # Compute within-CCS and between-CCS distances
    within = []
    between = []

    for i in range(len(all_responses)):
        for j in range(i + 1, len(all_responses)):
            d = cosine_distance(all_responses[i]["embedding"], all_responses[j]["embedding"])
            if all_responses[i]["ccs_idx"] == all_responses[j]["ccs_idx"]:
                within.append(d)
            else:
                between.append(d)

    # Results
    print(f"\n{'='*60}")
    print("CCS TOPOLOGY PROBE — Results")
    print(f"{'='*60}")

    w_mean = np.mean(within) if within else 0
    w_std = np.std(within) if within else 0
    b_mean = np.mean(between) if between else 0
    b_std = np.std(between) if between else 0

    print(f"\nWithin-CCS distances (same topology):")
    print(f"  N pairs: {len(within)}")
    print(f"  Mean:    {w_mean:.4f}")
    print(f"  Std:     {w_std:.4f}")

    print(f"\nBetween-CCS distances (different topology):")
    print(f"  N pairs: {len(between)}")
    print(f"  Mean:    {b_mean:.4f}")
    print(f"  Std:     {b_std:.4f}")

    if within and between:
        ratio = b_mean / w_mean if w_mean > 0 else float("inf")
        pooled_std = ((w_std + b_std) / 2) if (w_std + b_std) > 0 else 1
        cohens_d = (b_mean - w_mean) / pooled_std

        print(f"\nBetween/Within ratio: {ratio:.3f}")
        print(f"Cohen's d: {cohens_d:.3f}")
        print(f"Effect size: {'large' if abs(cohens_d) > 0.8 else 'medium' if abs(cohens_d) > 0.5 else 'small'}")

        if ratio > 1.2 and cohens_d > 0.5:
            print("\n>>> CCS topology IS load-bearing.")
            print("    Responses cluster by CCS version — the constraint shapes output geometry.")
        elif ratio > 1.05 and cohens_d > 0.2:
            print("\n>>> CCS topology shows MODERATE shaping effect.")
            print("    Some clustering but prompt-specific variance is high.")
        else:
            print("\n>>> CCS topology shows WEAK/NO effect.")
            print("    Responses don't cluster by CCS — prompt dominates over constraint.")

    # Save
    results = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "build": 54,
        "method": "ccs_topology_response_embedding",
        "n_ccs_versions": len(selected),
        "n_prompts_per_ccs": n_prompts,
        "n_responses": len(all_responses),
        "within_mean": float(w_mean),
        "within_std": float(w_std),
        "between_mean": float(b_mean),
        "between_std": float(b_std),
        "ratio": float(ratio) if within and between else None,
        "cohens_d": float(cohens_d) if within and between else None,
    }
    outpath = "/home/nate-agx/chronicle/data/topology_probe.json"
    with open(outpath, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nSaved to {outpath}")


if __name__ == "__main__":
    main()
