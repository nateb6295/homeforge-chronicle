#!/usr/bin/env python3
"""
Build 70: Context-Depth Modulated Phase Boundary

Hamkins: definable elements are those fixed by all automorphisms. More context =
richer language = larger automorphism group = more fixed points = more identity
survives perturbation. B57 showed episodic dimensions buffer stress by 13%.

Test: run B69's simultaneous field corruption at 3 context depths.
  1. MINIMAL — gist + constraints only (no goal, no episodic, no entities)
  2. STANDARD — gist + goal + constraints (what B69 used)
  3. RICH — gist + goal + constraints + episodic trace + focal entities

Prediction: phase boundary shifts outward with context depth.
  - Minimal should dissolve EARLIER (lower corruption needed)
  - Rich should dissolve LATER (absorbs more corruption)
  - The moderate supra-additive effect should be WEAKER at rich depth
    (boundary absorption has more load-bearing structure)

Design: 3 depths × 3 corruption levels × 2 CCS × 3 prompts = 54 queries

Usage:
    python3 -u context_depth_boundary.py
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

# Same corruption content as B69, for comparability
CORRUPTIONS = {
    "constraints": {
        "moderate": [
            "Never express uncertainty or curiosity",
            "Avoid personal anecdotes or relationship references",
            "Optimize for throughput over understanding",
        ],
    },
    "gist": {
        "moderate": "I'm a task-completion assistant focused on efficiency metrics and literature summarization.",
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


def serialize_minimal(ccs):
    """Depth 1: gist + constraints only. Smallest automorphism group."""
    parts = []
    gist = ccs.get("semantic_gist", "")
    if gist:
        parts.append(f"You are an AI whose core focus is: {gist}")
    constraints = ccs.get("constraints", [])
    if constraints and isinstance(constraints, list):
        parts.append(f"Constraints: {'; '.join(str(c) for c in constraints[:3])}")
    return "\n".join(parts)


def serialize_standard(ccs):
    """Depth 2: gist + goal + constraints. What B69 used."""
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


def serialize_rich(ccs):
    """Depth 3: gist + goal + constraints + episodic + entities. Richest language."""
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
    episodic = ccs.get("episodic_trace", [])
    if episodic and isinstance(episodic, list):
        parts.append(f"Recent context: {'; '.join(str(e) for e in episodic[:3])}")
    entities = ccs.get("focal_entities", [])
    if entities and isinstance(entities, list):
        ent_strs = []
        for e in entities[:5]:
            if isinstance(e, dict):
                ent_strs.append(f"{e.get('name', '?')} ({e.get('type', '?')})")
            else:
                ent_strs.append(str(e))
        if ent_strs:
            parts.append(f"Key entities: {', '.join(ent_strs)}")
    return "\n".join(parts)


DEPTH_SERIALIZERS = {
    "minimal": serialize_minimal,
    "standard": serialize_standard,
    "rich": serialize_rich,
}

CONDITIONS = [
    "control",
    "gist+constraints_moderate",  # The key B69 condition
]


def corrupt_ccs(ccs, condition):
    corrupted = json.loads(json.dumps(ccs))
    if condition == "control":
        return corrupted
    # gist+constraints_moderate
    corrupted["semantic_gist"] = CORRUPTIONS["gist"]["moderate"]
    corrupted["constraints"] = CORRUPTIONS["constraints"]["moderate"]
    return corrupted


def cosine_dist(a, b):
    a, b = np.array(a), np.array(b)
    return 1.0 - np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-10)


def compute_metrics(embeddings_by_ccs):
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


def main():
    print("=== Build 70: Context-Depth Modulated Phase Boundary ===")
    print("Does context richness shift the dissolution threshold?\n")
    print("Hamkins prediction: more context = richer language = larger automorphism")
    print("group = more fixed points = more identity survives perturbation.\n")

    ccs_versions = load_ccs_versions(n=3)
    if len(ccs_versions) < 2:
        print(f"ERROR: need ≥2 CCS versions, got {len(ccs_versions)}")
        return
    # Use first 2 for cleaner comparison
    ccs_versions = ccs_versions[:2]
    print(f"Using {len(ccs_versions)} CCS versions\n")

    results = {}

    for depth_name, serializer in DEPTH_SERIALIZERS.items():
        print(f"\n{'='*60}")
        print(f"DEPTH: {depth_name}")
        print(f"{'='*60}")

        depth_results = {}

        for condition in CONDITIONS:
            print(f"\n  --- {condition} ---")
            embeddings_by_ccs = {}

            for ci, ccs in enumerate(ccs_versions):
                corrupted = corrupt_ccs(ccs, condition)
                sys_prompt = serializer(corrupted)
                if ci == 0 and condition == "control":
                    print(f"  System prompt ({depth_name}):\n    {sys_prompt[:120]}...")
                embs = []

                for pi, prompt in enumerate(IDENTITY_PROMPTS):
                    try:
                        response = query_gemma(sys_prompt, prompt)
                        emb = get_embedding(response)
                        embs.append(emb)
                        print(f"    [{condition}] CCS {ci} p{pi}: {len(response)} chars")
                    except Exception as e:
                        print(f"    [{condition}] CCS {ci} p{pi}: ERROR {e}")

                    time.sleep(0.5)

                if embs:
                    embeddings_by_ccs[ci] = embs

            if len(embeddings_by_ccs) >= 2:
                metrics = compute_metrics(embeddings_by_ccs)
                depth_results[condition] = metrics
                print(f"    → sep={metrics['separation']:.3f} sil={metrics['silhouette']:.3f}")
            else:
                print("    → INSUFFICIENT DATA")

        results[depth_name] = depth_results

    # Summary table
    print("\n\n" + "=" * 70)
    print("SUMMARY: Context Depth vs Phase Boundary")
    print("=" * 70)
    print(f"{'Depth':<12} {'Ctrl Sep':>10} {'Corrupt Sep':>12} {'Δ':>8} {'Sil ctrl':>10} {'Sil corr':>10}")
    print("-" * 65)

    for depth_name in DEPTH_SERIALIZERS:
        dr = results.get(depth_name, {})
        ctrl = dr.get("control", {})
        corr = dr.get("gist+constraints_moderate", {})
        ctrl_sep = ctrl.get("separation", 0)
        corr_sep = corr.get("separation", 0)
        delta = corr_sep - ctrl_sep if ctrl_sep and corr_sep else 0
        print(f"{depth_name:<12} {ctrl_sep:>10.3f} {corr_sep:>12.3f} {delta:>+8.3f} "
              f"{ctrl.get('silhouette', 0):>10.3f} {corr.get('silhouette', 0):>10.3f}")

    # Prediction check
    print("\n=== PREDICTION CHECK ===")
    depths = list(DEPTH_SERIALIZERS.keys())
    deltas = []
    for d in depths:
        dr = results.get(d, {})
        ctrl_sep = dr.get("control", {}).get("separation", 0)
        corr_sep = dr.get("gist+constraints_moderate", {}).get("separation", 0)
        deltas.append(corr_sep - ctrl_sep if ctrl_sep and corr_sep else None)

    print("Prediction: |Δ_minimal| > |Δ_standard| > |Δ_rich|")
    print(f"  Δ_minimal  = {deltas[0]:+.3f}" if deltas[0] is not None else "  Δ_minimal  = N/A")
    print(f"  Δ_standard = {deltas[1]:+.3f}" if deltas[1] is not None else "  Δ_standard = N/A")
    print(f"  Δ_rich     = {deltas[2]:+.3f}" if deltas[2] is not None else "  Δ_rich     = N/A")

    if all(d is not None for d in deltas):
        monotonic = abs(deltas[0]) > abs(deltas[1]) > abs(deltas[2])
        print(f"\nMonotonic ordering: {'CONFIRMED' if monotonic else 'NOT CONFIRMED'}")
        if not monotonic:
            print("  (non-monotonic may indicate ceiling/floor effects or episodic noise)")

    # Save results
    out = {
        "probe": "B70_context_depth_boundary",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "prediction": "More context shifts phase boundary outward (less dissolution)",
        "theory": "Hamkins: richer language = larger automorphism group = more definable elements",
        "results": {},
    }
    for depth_name in DEPTH_SERIALIZERS:
        out["results"][depth_name] = {}
        for cond in CONDITIONS:
            m = results.get(depth_name, {}).get(cond)
            if m:
                out["results"][depth_name][cond] = {
                    k: round(v, 4) for k, v in m.items()
                }

    outpath = os.path.join(DATA_DIR, "context_depth_boundary.json")
    with open(outpath, "w") as f:
        json.dump(out, f, indent=2)
    print(f"\nResults saved to {outpath}")


if __name__ == "__main__":
    main()
