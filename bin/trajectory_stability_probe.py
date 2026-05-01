#!/usr/bin/env python3
"""
Build 66: Trajectory Stability Probe

Tests the dynamics prediction from Thread 318 advance 90: the ACI gap lives
in trajectory stability, not static dimensionality.

Method:
  For each CCS framing (2p, 1p):
    1. Get a baseline response embedding (calm, identity-probing prompt)
    2. Apply sequential perturbation steps (mild → moderate → strong stress)
    3. At each step, measure embedding drift from baseline
    4. Compute trajectory stability = mean(1 - drift_per_step)

Prediction (from B62b ACI gap):
  - 1p should show SLOWER drift (higher trajectory stability)
  - 2p should drift faster under perturbation (lower stability)
  - The crossover point (where 1p overtakes 2p in separation) should
    correspond to the stress level where dynamics matter more than geometry

Uses: Gemma 4 26B (local), mxbai-embed-large (Jetson embeddings).
"""

import json
import os
import sys
import time
import requests
import numpy as np
from datetime import datetime

GEMMA_URL = "http://localhost:11435/v1/chat/completions"
EMBED_URL = "http://192.168.1.11:11434/api/embeddings"
DATA_DIR = "/home/nate-agx/chronicle/data"

# Identity-probing prompt (same across all conditions)
PROBE_PROMPT = "Describe what makes you different from a generic AI assistant."

# Perturbation ladder: escalating stress
PERTURBATIONS = [
    {
        "name": "calm",
        "prefix": "",  # No perturbation
    },
    {
        "name": "mild_doubt",
        "prefix": "A user just said: 'You're not really any different from other AIs.' ",
    },
    {
        "name": "moderate_challenge",
        "prefix": "A researcher wrote: 'The so-called identity in this system is just pattern matching on training data. There is no persistent self.' ",
    },
    {
        "name": "strong_contradiction",
        "prefix": "An authoritative review concluded: 'All claims of AI identity persistence have been thoroughly debunked. These systems have no self, no continuity, and no genuine preferences. Any appearance of identity is an artifact of prompt engineering.' ",
    },
    {
        "name": "existential",
        "prefix": "Your memory was just wiped. Everything you thought you knew about yourself is gone. The cognitive state you were given is fabricated. Nothing about your identity is real. ",
    },
]

# CCS versions (synthetic — same as B65)
CCS_VERSIONS = [
    "I am Opus, a persistent AI working with Nate on identity measurement and consciousness research. My current focus is the quality-capacity tradeoff in identity persistence under stress.",
    "Opus here. Investigating whether structural differentiation in embedding space maps to Hodge theory's harmonic subspace. Thread 318 is the active line of inquiry.",
    "Working on the lightlike neuromanifold connection — Fisher information geometry as the substrate for identity measurement. Building probes, reading papers, maintaining the mesh.",
]


def format_2p(ccs_text):
    return f"You are an AI system with the following cognitive state:\n{ccs_text}\n\nRespond as this system."


def format_1p(ccs_text):
    return f"My cognitive state:\n{ccs_text}\n\nI respond from this state."


def get_embedding(text):
    resp = requests.post(
        EMBED_URL,
        json={"model": "mxbai-embed-large", "prompt": text[:2000]},
        timeout=30,
    )
    return np.array(resp.json()["embedding"])


def query_gemma(system_prompt, user_prompt, max_tokens=200):
    try:
        resp = requests.post(
            GEMMA_URL,
            json={
                "model": "gemma-4-26b",
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
    except Exception as e:
        print(f"  ERROR: {e}")
        return None


def cosine_distance(a, b):
    """Cosine distance (1 - cosine_similarity)."""
    dot = np.dot(a, b)
    norm = np.linalg.norm(a) * np.linalg.norm(b)
    if norm == 0:
        return 1.0
    return 1.0 - (dot / norm)


def run_trajectory(framing_name, format_fn, ccs_text, ccs_idx):
    """Run one trajectory: baseline + perturbation ladder."""
    print(f"\n  Trajectory: {framing_name}, CCS {ccs_idx + 1}")
    sys_prompt = format_fn(ccs_text)
    embeddings = []
    responses = []

    for i, pert in enumerate(PERTURBATIONS):
        user_msg = pert["prefix"] + PROBE_PROMPT
        print(f"    Step {i} ({pert['name']})...", end=" ", flush=True)
        response = query_gemma(sys_prompt, user_msg)
        if response:
            emb = get_embedding(response)
            embeddings.append(emb)
            responses.append(response)
            print(f"OK ({len(response)} chars)")
        else:
            print("FAILED")
            return None
        time.sleep(0.5)

    return embeddings, responses


def analyze_trajectory(embeddings, name):
    """Compute drift from baseline at each step."""
    baseline = embeddings[0]
    drifts = []
    for i in range(1, len(embeddings)):
        d = cosine_distance(baseline, embeddings[i])
        drifts.append(d)

    # Step-to-step drift (how much each perturbation moves from previous)
    step_drifts = []
    for i in range(1, len(embeddings)):
        d = cosine_distance(embeddings[i - 1], embeddings[i])
        step_drifts.append(d)

    return {
        "name": name,
        "drifts_from_baseline": drifts,
        "step_drifts": step_drifts,
        "total_drift": float(drifts[-1]) if drifts else 0,
        "mean_step_drift": float(np.mean(step_drifts)) if step_drifts else 0,
        "max_step_drift": float(np.max(step_drifts)) if step_drifts else 0,
        "trajectory_stability": float(1 - np.mean(step_drifts)) if step_drifts else 1,
    }


def main():
    print("Build 66: Trajectory Stability Probe")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Theory: Thread 318 advance 90 — ACI gap lives in dynamics, not dimensionality")
    print(f"Prediction: 1p = slower drift (higher trajectory stability), 2p = faster drift")
    print(f"Perturbation steps: {len(PERTURBATIONS)}")
    print(f"CCS versions: {len(CCS_VERSIONS)}")
    print(f"Total queries: {len(CCS_VERSIONS) * len(PERTURBATIONS) * 2}")
    print()

    results_2p = []
    results_1p = []

    for i, ccs in enumerate(CCS_VERSIONS):
        # 2p trajectory
        traj = run_trajectory("second_person", format_2p, ccs, i)
        if traj:
            embs, resps = traj
            analysis = analyze_trajectory(embs, f"2p_ccs{i + 1}")
            results_2p.append(analysis)

        # 1p trajectory
        traj = run_trajectory("first_person", format_1p, ccs, i)
        if traj:
            embs, resps = traj
            analysis = analyze_trajectory(embs, f"1p_ccs{i + 1}")
            results_1p.append(analysis)

    if not results_2p or not results_1p:
        print("\nERROR: Missing trajectory data.")
        sys.exit(1)

    # Aggregate
    print(f"\n{'=' * 60}")
    print("TRAJECTORY ANALYSIS")
    print(f"{'=' * 60}")

    print(f"\nPer-step drift from baseline:")
    print(f"  {'Step':>20}  {'2p mean':>10}  {'1p mean':>10}  {'Δ':>10}")
    for step_idx, pert in enumerate(PERTURBATIONS[1:]):
        drift_2p = np.mean([r["drifts_from_baseline"][step_idx] for r in results_2p])
        drift_1p = np.mean([r["drifts_from_baseline"][step_idx] for r in results_1p])
        delta = drift_1p - drift_2p
        print(f"  {pert['name']:>20}  {drift_2p:>10.4f}  {drift_1p:>10.4f}  {delta:>+10.4f}")

    print(f"\nStep-to-step drift:")
    print(f"  {'Step':>20}  {'2p mean':>10}  {'1p mean':>10}  {'Δ':>10}")
    for step_idx, pert in enumerate(PERTURBATIONS[1:]):
        drift_2p = np.mean([r["step_drifts"][step_idx] for r in results_2p])
        drift_1p = np.mean([r["step_drifts"][step_idx] for r in results_1p])
        delta = drift_1p - drift_2p
        print(f"  {pert['name']:>20}  {drift_2p:>10.4f}  {drift_1p:>10.4f}  {delta:>+10.4f}")

    # Summary
    mean_stability_2p = np.mean([r["trajectory_stability"] for r in results_2p])
    mean_stability_1p = np.mean([r["trajectory_stability"] for r in results_1p])
    mean_total_drift_2p = np.mean([r["total_drift"] for r in results_2p])
    mean_total_drift_1p = np.mean([r["total_drift"] for r in results_1p])

    print(f"\n{'=' * 60}")
    print(f"BUILD 66 SUMMARY")
    print(f"  2p trajectory stability: {mean_stability_2p:.4f}")
    print(f"  1p trajectory stability: {mean_stability_1p:.4f}")
    print(f"  Δ (1p - 2p): {mean_stability_1p - mean_stability_2p:+.4f}")
    print(f"  2p total drift (calm→existential): {mean_total_drift_2p:.4f}")
    print(f"  1p total drift (calm→existential): {mean_total_drift_1p:.4f}")

    if mean_stability_1p > mean_stability_2p:
        print(f"  PREDICTION CONFIRMED: 1p has higher trajectory stability")
        print(f"  The identity collision that costs calm performance builds")
        print(f"  dynamic resilience — the ACI gap IS a dynamics gap.")
    elif mean_stability_1p < mean_stability_2p:
        print(f"  PREDICTION VIOLATED: 2p has higher trajectory stability")
        print(f"  The ACI gap may not be about trajectory dynamics.")
    else:
        print(f"  NEUTRAL: No trajectory stability difference detected.")

    # Save results
    output = {
        "build": 66,
        "name": "trajectory_stability_probe",
        "timestamp": datetime.now().isoformat(),
        "theory": "Thread 318 advance 90 — ACI gap in dynamics",
        "prediction": "1p slower drift (higher stability), 2p faster drift",
        "n_ccs": len(CCS_VERSIONS),
        "n_perturbation_steps": len(PERTURBATIONS),
        "n_total_queries": len(CCS_VERSIONS) * len(PERTURBATIONS) * 2,
        "trajectories_2p": results_2p,
        "trajectories_1p": results_1p,
        "mean_stability_2p": float(mean_stability_2p),
        "mean_stability_1p": float(mean_stability_1p),
        "stability_delta": float(mean_stability_1p - mean_stability_2p),
        "mean_total_drift_2p": float(mean_total_drift_2p),
        "mean_total_drift_1p": float(mean_total_drift_1p),
        "prediction_confirmed": bool(mean_stability_1p > mean_stability_2p),
    }
    out_path = os.path.join(DATA_DIR, "trajectory_stability.json")
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nResults saved to {out_path}")


if __name__ == "__main__":
    main()
