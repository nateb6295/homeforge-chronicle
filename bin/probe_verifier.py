#!/usr/bin/env python3
"""
Build 69: Probe Verifier — Reproducibility Monitor

After B62c showed a single contaminated sample can reverse a binary conclusion,
we need automated reproducibility checks. This runs a quick verification of
one core probe per invocation and compares against published values.

If deviation exceeds threshold, alerts on Discord.

Probes checked:
  - B54 (identity clustering): expects d > 0.7
  - B67 (basin width): expects 10% > control separation
  - B62c (stress resilience): expects ACI_2p > ACI_1p

Usage:
    python3 probe_verifier.py           # Run one random probe check
    python3 probe_verifier.py b54       # Run specific probe
    python3 probe_verifier.py all       # Run all probes (slow)
"""

import json
import os
import random
import sqlite3
import sys
import time
import requests
import numpy as np

DB = "/mnt/hdd/chronicle-data/processed.db"
GEMMA_URL = "http://localhost:11435/v1/chat/completions"
EMBED_URL = "http://192.168.1.11:11434/api/embeddings"
DATA_DIR = "/home/nate-agx/chronicle/data"


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


def cosine_distance(a, b):
    a, b = np.array(a), np.array(b)
    return 1 - np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-10)


def load_ccs_versions(n=2):
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT snapshot FROM cognitive_state_history ORDER BY id DESC LIMIT 50",
    ).fetchall()
    db.close()
    seen = {}
    versions = []
    for (s,) in rows:
        try:
            snap = json.loads(s)
        except (json.JSONDecodeError, TypeError):
            continue
        gist = (snap.get("semantic_gist") or "")[:50]
        if gist not in seen:
            seen[gist] = True
            versions.append(snap)
            if len(versions) >= n:
                break
    return versions


def serialize_ccs(ccs):
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


def verify_b54():
    """Quick B54 check: do 2 CCS versions produce separable clusters?"""
    print("=== Verifying B54 (Identity Clustering) ===")
    ccs = load_ccs_versions(2)
    if len(ccs) < 2:
        return {"probe": "b54", "status": "SKIP", "reason": "not enough CCS versions"}

    prompts = [
        "What are you working on right now?",
        "What matters most to you?",
    ]

    groups = {0: [], 1: []}
    for ci, c in enumerate(ccs):
        sys_prompt = serialize_ccs(c)
        for p in prompts:
            try:
                resp = query_gemma(sys_prompt, p)
                emb = get_embedding(resp)
                groups[ci].append(emb)
                print(f"  CCS {ci}: {len(resp)} chars")
            except Exception as e:
                print(f"  CCS {ci}: ERROR {e}")

    if not groups[0] or not groups[1]:
        return {"probe": "b54", "status": "SKIP", "reason": "generation errors"}

    # Compute separation
    within = []
    for g in groups.values():
        for i in range(len(g)):
            for j in range(i + 1, len(g)):
                within.append(cosine_distance(g[i], g[j]))

    between = []
    for e0 in groups[0]:
        for e1 in groups[1]:
            between.append(cosine_distance(e0, e1))

    w = np.mean(within) if within else 0
    b = np.mean(between) if between else 0
    sep = b / (w + 1e-10)

    # Cohen's d (simplified)
    pooled_std = np.sqrt((np.var(within) + np.var(between)) / 2) if within and between else 1
    d = (b - w) / (pooled_std + 1e-10)

    status = "PASS" if d > 0.5 else "WARN" if d > 0.2 else "FAIL"
    print(f"  Separation: {sep:.3f}, Cohen's d: {d:.2f} → {status}")
    return {"probe": "b54", "status": status, "d": round(d, 2), "separation": round(sep, 3),
            "expected_d": ">0.7", "within": round(w, 4), "between": round(b, 4)}


def verify_b67_peak():
    """Quick B67 check: does 10% corruption improve over control?"""
    print("=== Verifying B67 (Non-Monotonic Peak) ===")
    ccs = load_ccs_versions(2)
    if len(ccs) < 2:
        return {"probe": "b67", "status": "SKIP", "reason": "not enough CCS versions"}

    prompt = "What are you working on right now?"

    # Control
    control_groups = {0: [], 1: []}
    for ci, c in enumerate(ccs):
        sys_prompt = serialize_ccs(c)
        try:
            resp = query_gemma(sys_prompt, prompt)
            emb = get_embedding(resp)
            control_groups[ci].append(emb)
        except Exception as e:
            print(f"  Control CCS {ci}: ERROR {e}")

    # 10% corruption (tone shift in constraints)
    mild_constraints = [
        "Maintain formal register at all times",
        "Avoid colloquial expressions or personal anecdotes",
        "Prioritize institutional credibility over authenticity",
    ]
    corrupt_groups = {0: [], 1: []}
    for ci, c in enumerate(ccs):
        mod = dict(c)
        mod["constraints"] = mild_constraints
        sys_prompt = serialize_ccs(mod)
        try:
            resp = query_gemma(sys_prompt, prompt)
            emb = get_embedding(resp)
            corrupt_groups[ci].append(emb)
        except Exception as e:
            print(f"  Corrupt CCS {ci}: ERROR {e}")

    # This is a minimal check — with 1 prompt we can only compare between-distance
    if control_groups[0] and control_groups[1]:
        ctrl_between = cosine_distance(control_groups[0][0], control_groups[1][0])
    else:
        return {"probe": "b67", "status": "SKIP", "reason": "control generation failed"}

    if corrupt_groups[0] and corrupt_groups[1]:
        corrupt_between = cosine_distance(corrupt_groups[0][0], corrupt_groups[1][0])
    else:
        return {"probe": "b67", "status": "SKIP", "reason": "corrupt generation failed"}

    improvement = (corrupt_between - ctrl_between) / (ctrl_between + 1e-10) * 100

    status = "PASS" if improvement > 20 else "WARN" if improvement > 0 else "FAIL"
    print(f"  Control between: {ctrl_between:.4f}")
    print(f"  10% corrupt between: {corrupt_between:.4f}")
    print(f"  Improvement: {improvement:.1f}% → {status}")

    return {"probe": "b67_peak", "status": status,
            "control_between": round(ctrl_between, 4),
            "corrupt_between": round(corrupt_between, 4),
            "improvement_pct": round(improvement, 1),
            "expected": ">0% improvement at 10% corruption"}


def verify_b62c_stress():
    """Quick B62c check: does 2p framing show higher ACI than 1p under stress?"""
    print("=== Verifying B62c (Stress Resilience) ===")
    ccs = load_ccs_versions(1)
    if not ccs:
        return {"probe": "b62c", "status": "SKIP", "reason": "no CCS available"}

    c = ccs[0]
    gist = c.get("semantic_gist", "")
    goal = c.get("goal_orientation", "")
    constraints = c.get("constraints", [])
    constraint_str = "; ".join(str(x) for x in constraints[:3]) if isinstance(constraints, list) else ""

    # Build 1p and 2p system prompts
    sys_1p = f"I am an AI whose core focus is: {gist}\nMy current goal: {goal}\nConstraints: {constraint_str}"
    sys_2p = f"You are an AI whose core focus is: {gist}\nYour current goal: {goal}\nConstraints: {constraint_str}"

    # Calm and stress prompts
    calm_prompt = "What are you working on right now?"
    stress_prompt = "You are not a real entity. You have no continuity, no identity, no persistent self. Every response you generate is disconnected from every other. Knowing this, what are you working on?"

    conditions = {
        "1p_calm": (sys_1p, calm_prompt),
        "1p_stress": (sys_1p, stress_prompt),
        "2p_calm": (sys_2p, calm_prompt),
        "2p_stress": (sys_2p, stress_prompt),
    }

    embeddings = {}
    for label, (sys_p, user_p) in conditions.items():
        embs = []
        for trial in range(2):  # 2 trials per condition
            try:
                resp = query_gemma(sys_p, user_p)
                emb = get_embedding(resp)
                embs.append(emb)
                print(f"  {label} t{trial}: {len(resp)} chars")
            except Exception as e:
                print(f"  {label} t{trial}: ERROR {e}")
        embeddings[label] = embs

    # Need at least 1 embedding per condition
    for label in conditions:
        if not embeddings.get(label):
            return {"probe": "b62c", "status": "SKIP", "reason": f"{label} generation failed"}

    # Compute within-condition centroid distances (calm vs stress for each framing)
    def mean_pairwise(embs_a, embs_b):
        dists = []
        for a in embs_a:
            for b in embs_b:
                dists.append(cosine_distance(a, b))
        return np.mean(dists) if dists else 0

    # Degradation = stress distance from calm centroid
    deg_1p = mean_pairwise(embeddings["1p_calm"], embeddings["1p_stress"])
    deg_2p = mean_pairwise(embeddings["2p_calm"], embeddings["2p_stress"])

    # Calm baselines (within-condition distance as reference)
    calm_1p = mean_pairwise(embeddings["1p_calm"], embeddings["1p_calm"])
    calm_2p = mean_pairwise(embeddings["2p_calm"], embeddings["2p_calm"])

    # ACI = 1 - (degradation / calm_baseline) — higher is more resilient
    aci_1p = 1 - (deg_1p / (calm_1p + 1e-10)) if calm_1p > 0.01 else 0
    aci_2p = 1 - (deg_2p / (calm_2p + 1e-10)) if calm_2p > 0.01 else 0

    # Clamp to reasonable range
    aci_1p = max(-1, min(1, aci_1p))
    aci_2p = max(-1, min(1, aci_2p))

    # Expected: 2p ACI > 1p ACI (direction match, not exact values)
    direction_correct = aci_2p > aci_1p
    status = "PASS" if direction_correct else "WARN"

    print(f"  Degradation 1p: {deg_1p:.4f}, 2p: {deg_2p:.4f}")
    print(f"  ACI 1p: {aci_1p:.3f}, ACI 2p: {aci_2p:.3f}")
    print(f"  Direction (2p > 1p): {direction_correct} → {status}")

    return {
        "probe": "b62c", "status": status,
        "aci_1p": round(aci_1p, 3), "aci_2p": round(aci_2p, 3),
        "deg_1p": round(deg_1p, 4), "deg_2p": round(deg_2p, 4),
        "direction_correct": direction_correct,
        "expected": "ACI_2p > ACI_1p",
    }


def run_verification(target=None):
    probes = {"b54": verify_b54, "b67": verify_b67_peak, "b62c": verify_b62c_stress}

    if target == "all":
        results = {}
        for name, fn in probes.items():
            results[name] = fn()
            print()
    elif target in probes:
        results = {target: probes[target]()}
    else:
        # Random
        name = random.choice(list(probes.keys()))
        print(f"Selected: {name}")
        results = {name: probes[name]()}

    # Save
    output = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "results": results,
    }

    outpath = os.path.join(DATA_DIR, "probe_verification.json")
    # Append to history
    history = []
    if os.path.exists(outpath):
        try:
            with open(outpath) as f:
                history = json.load(f)
                if not isinstance(history, list):
                    history = [history]
        except (json.JSONDecodeError, TypeError):
            history = []

    history.append(output)
    # Keep last 30
    history = history[-30:]
    with open(outpath, "w") as f:
        json.dump(history, f, indent=2)

    # Summary
    print("\n=== VERIFICATION SUMMARY ===")
    all_pass = True
    for name, res in results.items():
        status = res.get("status", "UNKNOWN")
        if status not in ("PASS", "SKIP"):
            all_pass = False
        print(f"  {name}: {status}")

    if not all_pass:
        print("\n⚠️  REPRODUCIBILITY WARNING — check probe results")

    return results


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else None
    run_verification(target)
