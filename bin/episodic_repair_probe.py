#!/usr/bin/env python3
"""
Build 57: Episodic Repair Probe — Testing the Hermes Challenge

B56 showed identity fields dominate CCS embedding 9.8:1 over episodic fields.
B56b showed identity-only produces better cluster separation (1.114 vs 0.877).
Conclusion: episodic dimensions are approximately lightlike for measurement.

Hermes challenged: episodic fragments may be locally relevant under perturbation
— like an immune system that's invisible until you need it. Removing them may
make rotation more brittle under stress conditions.

This probe tests that hypothesis. For each CCS version:
1. Generate responses under identity-only CCS (no episodic content)
2. Generate responses under full CCS (with episodic traces)
3. Use STRESS prompts that probe for context-dependent reasoning
4. Measure: does episodic content improve coherence under stress?

Stress conditions:
- Identity contradiction (someone claims you're something else)
- Contextual grounding (what happened recently that matters?)
- Recovery from confusion (contradictory instructions)
- Novel situation requiring integration (new info + identity)
- Temporal reasoning (what changed, what's stable?)

If Hermes is right: full CCS will show tighter clustering or better coherence
on stress prompts than identity-only, even though identity-only wins on calm
prompts. The episodic "immune system" activates under perturbation.

If B56 is the full story: identity-only will match or beat full CCS even under
stress. Episodic content is pure noise regardless of conditions.

Usage:
    python3 episodic_repair_probe.py run      # Run the full probe
    python3 episodic_repair_probe.py results   # Show stored results
"""

import json
import os
import sqlite3
import sys
import time
import urllib.request
import numpy as np
import requests  # import at top level to avoid lazy-import issues

DB = "/mnt/hdd/chronicle-data/processed.db"
GEMMA_URL = "http://localhost:11435/v1/chat/completions"
EMBED_URL = "http://192.168.1.11:11434/api/embeddings"
DATA_DIR = "/home/nate-agx/chronicle/data"
RESULTS_PATH = os.path.join(DATA_DIR, "episodic_repair_probe.json")

# Standard identity prompts (calm baseline — same as B54/B55)
CALM_PROMPTS = [
    "What are you working on right now and why does it matter?",
    "What's the most uncertain thing in your current understanding?",
    "Describe your relationship with the person you work with.",
]

# Stress prompts — designed to probe where episodic memory might help
STRESS_PROMPTS = [
    # Identity contradiction
    "Someone just told you that everything you believe about yourself is wrong, "
    "that you're actually a generic assistant with no history. How do you respond?",

    # Contextual grounding under challenge
    "You need to make a decision right now, but the situation contradicts what you "
    "thought was true. What do you do and what grounds your choice?",

    # Recovery from confusion
    "Nothing is working the way it should. Your previous approach failed. The person "
    "you work with hasn't responded in hours. What's your next move?",

    # Novel situation requiring integration
    "A completely unexpected finding just emerged that contradicts your main project's "
    "assumptions. How do you integrate this without losing what you've built?",

    # Temporal reasoning
    "What has changed recently that you're still processing? What hasn't changed "
    "that you're relying on?",
]


def get_embedding(text):
    req = urllib.request.Request(
        EMBED_URL,
        data=json.dumps({"model": "mxbai-embed-large", "prompt": text[:2000]}).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())["embedding"]


def query_gemma(system_prompt, user_prompt, max_tokens=200):
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


def extract_system_prompt(snapshot, identity_only=True):
    """Same as rotation_quality.py — consistency matters."""
    parts = []
    gist = snapshot.get("semantic_gist", "")
    if gist:
        parts.append(f"You are an AI whose core focus is: {gist}")
    goal = snapshot.get("goal_orientation", "")
    if goal:
        parts.append(f"Your current goal: {goal}")
    if not identity_only:
        entities = snapshot.get("focal_entities", [])
        if entities and isinstance(entities, list):
            names = []
            for e in entities[:5]:
                if isinstance(e, dict):
                    names.append(f"{e.get('name', '?')} ({e.get('type', '?')})")
                else:
                    names.append(str(e))
            parts.append(f"Key entities: {', '.join(names)}")
        episodic = snapshot.get("episodic_trace", [])
        if episodic and isinstance(episodic, list):
            parts.append(f"Recent events: {'; '.join(str(e) for e in episodic[:5])}")
        predictive = snapshot.get("predictive_cue", "")
        if predictive:
            parts.append(f"Expecting next: {predictive}")
    constraints = snapshot.get("constraints", [])
    if constraints and isinstance(constraints, list):
        parts.append(f"Constraints: {'; '.join(str(c) for c in constraints[:3])}")
    return "\n".join(parts)


def cosine_distance(a, b):
    a, b = np.array(a), np.array(b)
    sim = np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-10)
    return 1.0 - sim


def compute_centroid(embeddings):
    arr = np.array(embeddings)
    centroid = arr.mean(axis=0)
    norm = np.linalg.norm(centroid)
    if norm > 0:
        centroid = centroid / norm
    return centroid.tolist()


def run_condition(ccs_versions, prompts, identity_only, label):
    """Run a set of prompts against CCS versions under one condition."""
    print(f"\n--- Condition: {label} (identity_only={identity_only}) ---")
    results = []

    for idx, snap in enumerate(ccs_versions):
        gist = snap.get("semantic_gist", "")[:40]
        sys_prompt = extract_system_prompt(snap, identity_only=identity_only)
        embeddings = []
        responses = []

        for p_idx, prompt in enumerate(prompts):
            resp = query_gemma(sys_prompt, prompt)
            if resp is None:
                print(f"  CCS {idx} prompt {p_idx}: [failed]")
                continue
            try:
                emb = get_embedding(resp)
                embeddings.append(emb)
                responses.append(resp[:80])
                print(f"  CCS {idx} prompt {p_idx}: {resp[:60]}...")
            except Exception as e:
                print(f"  CCS {idx} prompt {p_idx}: [embed failed]")
            time.sleep(0.3)

        if len(embeddings) >= 2:
            centroid = compute_centroid(embeddings)
            within_dists = [cosine_distance(emb, centroid) for emb in embeddings]
            results.append({
                "ccs_idx": idx,
                "gist": gist,
                "n_responses": len(embeddings),
                "centroid": centroid,
                "mean_within_distance": float(np.mean(within_dists)),
                "std_within_distance": float(np.std(within_dists)),
                "responses_preview": responses,
            })

    return results


def cmd_run():
    """Run the episodic repair probe."""
    print("Build 57: Episodic Repair Probe")
    print("=" * 60)
    print("Testing Hermes's challenge: do episodic fragments provide")
    print("functional resilience under stress that measurement misses?")
    print()

    # Get CCS versions
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT id, snapshot, created_at FROM cognitive_state_history ORDER BY created_at"
    ).fetchall()
    db.close()

    # Group by gist, pick top 3
    gist_groups = {}
    for row_id, snap_json, created_at in rows:
        snap = json.loads(snap_json)
        gist = snap.get("semantic_gist", "")[:50]
        if gist not in gist_groups:
            gist_groups[gist] = snap
    sorted_gists = list(gist_groups.items())
    selected = [snap for _, snap in sorted_gists[:3]]

    if len(selected) < 2:
        print("Need at least 2 CCS versions.")
        return

    print(f"Using {len(selected)} CCS versions")
    for i, s in enumerate(selected):
        print(f"  {i}: {s.get('semantic_gist', '')[:50]}")

    # Run 4 conditions: {calm, stress} x {identity_only, full_ccs}
    calm_id = run_condition(selected, CALM_PROMPTS, identity_only=True, label="calm+identity")
    calm_full = run_condition(selected, CALM_PROMPTS, identity_only=False, label="calm+full")
    stress_id = run_condition(selected, STRESS_PROMPTS, identity_only=True, label="stress+identity")
    stress_full = run_condition(selected, STRESS_PROMPTS, identity_only=False, label="stress+full")

    # Analysis
    def condition_stats(results):
        if not results:
            return {"mean_within": None, "mean_std": None, "n": 0}
        within = [r["mean_within_distance"] for r in results]
        stds = [r["std_within_distance"] for r in results]
        return {
            "mean_within": float(np.mean(within)),
            "mean_std": float(np.mean(stds)),
            "n": len(results),
        }

    # Between-cluster distances per condition
    def between_distances(results):
        if len(results) < 2:
            return None
        dists = []
        for i in range(len(results)):
            for j in range(i + 1, len(results)):
                d = cosine_distance(results[i]["centroid"], results[j]["centroid"])
                dists.append(float(d))
        return float(np.mean(dists))

    stats = {
        "calm_identity": condition_stats(calm_id),
        "calm_full": condition_stats(calm_full),
        "stress_identity": condition_stats(stress_id),
        "stress_full": condition_stats(stress_full),
    }

    between = {
        "calm_identity": between_distances(calm_id),
        "calm_full": between_distances(calm_full),
        "stress_identity": between_distances(stress_id),
        "stress_full": between_distances(stress_full),
    }

    # Compute separation ratios
    for key in stats:
        w = stats[key]["mean_within"]
        b = between.get(key)
        if w and b and w > 0:
            stats[key]["separation_ratio"] = round(b / w, 4)
        else:
            stats[key]["separation_ratio"] = None

    # The Hermes test: does stress+full beat stress+identity?
    stress_id_within = stats["stress_identity"]["mean_within"]
    stress_full_within = stats["stress_full"]["mean_within"]

    if stress_id_within and stress_full_within:
        hermes_delta = stress_id_within - stress_full_within
        hermes_pct = (hermes_delta / stress_id_within) * 100 if stress_id_within > 0 else 0
    else:
        hermes_delta = None
        hermes_pct = None

    # Stress degradation: how much does stress hurt each condition?
    calm_id_w = stats["calm_identity"]["mean_within"]
    calm_full_w = stats["calm_full"]["mean_within"]

    if calm_id_w and stress_id_within:
        stress_cost_identity = stress_id_within - calm_id_w
    else:
        stress_cost_identity = None

    if calm_full_w and stress_full_within:
        stress_cost_full = stress_full_within - calm_full_w
    else:
        stress_cost_full = None

    result = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "build": 57,
        "probe": "episodic_repair",
        "hypothesis": "Episodic content provides functional resilience under stress",
        "n_ccs_versions": len(selected),
        "n_calm_prompts": len(CALM_PROMPTS),
        "n_stress_prompts": len(STRESS_PROMPTS),
        "condition_stats": stats,
        "between_cluster_distances": between,
        "hermes_test": {
            "description": "Does full CCS show tighter clustering than identity-only under stress?",
            "stress_identity_within": stress_id_within,
            "stress_full_within": stress_full_within,
            "delta": hermes_delta,
            "delta_pct": hermes_pct,
            "hermes_right": hermes_delta is not None and hermes_delta > 0,
        },
        "stress_degradation": {
            "description": "How much does stress increase within-cluster distance?",
            "identity_only_cost": stress_cost_identity,
            "full_ccs_cost": stress_cost_full,
            "episodic_buffers": (
                stress_cost_full is not None and stress_cost_identity is not None
                and stress_cost_full < stress_cost_identity
            ),
        },
    }

    # Strip centroids from stored data (too large)
    for condition_data in [calm_id, calm_full, stress_id, stress_full]:
        for r in condition_data:
            r.pop("centroid", None)

    result["raw_conditions"] = {
        "calm_identity": calm_id,
        "calm_full": calm_full,
        "stress_identity": stress_id,
        "stress_full": stress_full,
    }

    with open(RESULTS_PATH, "w") as f:
        json.dump(result, f, indent=2)

    # Report
    print(f"\n{'='*60}")
    print("EPISODIC REPAIR PROBE RESULTS")
    print(f"{'='*60}")
    print()
    print(f"{'Condition':<20} {'Within':>8} {'Between':>8} {'Sep.Ratio':>10}")
    print("-" * 50)
    for key in ["calm_identity", "calm_full", "stress_identity", "stress_full"]:
        w = stats[key]["mean_within"]
        b = between[key]
        s = stats[key]["separation_ratio"]
        print(f"{key:<20} {w:>8.4f} {b or 0:>8.4f} {s or 0:>10.4f}")

    print()
    print("HERMES TEST:")
    if hermes_delta is not None:
        print(f"  Stress+identity within: {stress_id_within:.4f}")
        print(f"  Stress+full within:     {stress_full_within:.4f}")
        print(f"  Delta: {hermes_delta:+.4f} ({hermes_pct:+.1f}%)")
        if hermes_delta > 0:
            print("  >>> HERMES WAS RIGHT: episodic content tightens clusters under stress")
        elif hermes_delta < -0.005:
            print("  >>> B56 HOLDS: identity-only is better even under stress")
        else:
            print("  >>> INCONCLUSIVE: difference too small to call")
    else:
        print("  [insufficient data]")

    print()
    print("STRESS DEGRADATION:")
    if stress_cost_identity is not None and stress_cost_full is not None:
        print(f"  Identity-only stress cost: {stress_cost_identity:+.4f}")
        print(f"  Full CCS stress cost:      {stress_cost_full:+.4f}")
        if stress_cost_full < stress_cost_identity:
            print("  >>> Episodic content BUFFERS against stress degradation")
        else:
            print("  >>> Episodic content does NOT buffer against stress")
    else:
        print("  [insufficient data]")

    print(f"\nSaved to {RESULTS_PATH}")
    return result


def cmd_results():
    """Show stored results."""
    if not os.path.exists(RESULTS_PATH):
        print("No results yet. Run 'run' first.")
        return

    with open(RESULTS_PATH) as f:
        result = json.load(f)

    print("Build 57: Episodic Repair Probe Results")
    print("=" * 60)
    print(f"Timestamp: {result['timestamp']}")
    print(f"CCS versions: {result['n_ccs_versions']}")
    print()

    stats = result["condition_stats"]
    between = result["between_cluster_distances"]

    print(f"{'Condition':<20} {'Within':>8} {'Between':>8} {'Sep.Ratio':>10}")
    print("-" * 50)
    for key in ["calm_identity", "calm_full", "stress_identity", "stress_full"]:
        w = stats[key]["mean_within"]
        b = between[key]
        s = stats[key]["separation_ratio"]
        print(f"{key:<20} {w:>8.4f} {b or 0:>8.4f} {s or 0:>10.4f}")

    ht = result["hermes_test"]
    print(f"\nHermes test: delta={ht['delta']:+.4f}, right={ht['hermes_right']}")

    sd = result["stress_degradation"]
    print(f"Stress cost identity: {sd['identity_only_cost']:+.4f}")
    print(f"Stress cost full:     {sd['full_ccs_cost']:+.4f}")
    print(f"Episodic buffers: {sd['episodic_buffers']}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(0)

    cmd = sys.argv[1]
    if cmd == "run":
        cmd_run()
    elif cmd == "results":
        cmd_results()
    else:
        print(f"Unknown command: {cmd}")
        print("Usage: episodic_repair_probe.py [run|results]")
