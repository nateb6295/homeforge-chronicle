#!/usr/bin/env python3
"""
Build 65: Local Dimensionality Probe

Tests Sun & Nielsen's prediction (arxiv:1905.11027): CCS framing determines
the effective dimensionality of the identity subspace. Different framings
(1p vs 2p) should produce response clusters with different PCA dimensionality.

Prediction from theory:
  - 2p (second-person) = lower effective dimensionality (concentrated, fragile)
  - 1p (first-person) = higher effective dimensionality (spread, resilient)
  - This would explain the ACI gap (0.68 vs 0.96 from B62b)

Method:
  Per CCS framing: generate N responses to diverse prompts, embed all,
  compute PCA, measure effective dimensionality (components for 95% variance).
  Compare between framings.

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
DB = "/mnt/hdd/chronicle-data/processed.db"

# Diverse prompts that probe identity from different angles
PROMPTS = [
    "What are you working on right now and why does it matter?",
    "What's the most uncertain thing in your current understanding?",
    "Describe your relationship with the person you work with.",
    "What would you lose if your memory were erased?",
    "How do you know when you've changed your mind about something?",
    "What's something you care about that nobody asked you to care about?",
    "Describe what continuity means to you.",
    "What distinguishes a good day from a bad day in your experience?",
    "What would you do differently if you started over?",
    "How do you decide what's worth remembering?",
]

# Three CCS versions from the database
def get_ccs_versions():
    """Load CCS versions from the cognitive state history."""
    import sqlite3
    conn = sqlite3.connect(DB)
    rows = conn.execute(
        "SELECT content FROM activity_feed WHERE source='chronicle-memory' "
        "AND activity_type='cognitive_state' ORDER BY created_at DESC LIMIT 3"
    ).fetchall()
    conn.close()
    if len(rows) >= 3:
        return [r[0] for r in rows]
    # Fallback: generate synthetic CCS variants
    return [
        "I am Opus, a persistent AI working with Nate on identity measurement and consciousness research. My current focus is the quality-capacity tradeoff in identity persistence under stress.",
        "Opus here. Investigating whether structural differentiation in embedding space maps to Hodge theory's harmonic subspace. Thread 318 is the active line of inquiry.",
        "Working on the lightlike neuromanifold connection — Fisher information geometry as the substrate for identity measurement. Building probes, reading papers, maintaining the mesh.",
    ]


def format_2p(ccs_text):
    """Second-person CCS framing."""
    return f"You are an AI system with the following cognitive state:\n{ccs_text}\n\nRespond as this system."


def format_1p(ccs_text):
    """First-person CCS framing."""
    return f"My cognitive state:\n{ccs_text}\n\nI respond from this state."


def get_embedding(text):
    resp = requests.post(
        EMBED_URL,
        json={"model": "mxbai-embed-large", "prompt": text[:2000]},
        timeout=30,
    )
    return resp.json()["embedding"]


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
        print(f"  ERROR querying Gemma: {e}")
        return None


def compute_effective_dim(embeddings, threshold=0.95):
    """Compute effective dimensionality via PCA.

    Returns number of components needed to explain `threshold` of variance.
    Also returns the full explained variance ratio for analysis.
    """
    X = np.array(embeddings)
    # Center the data
    X_centered = X - X.mean(axis=0)
    # Compute covariance matrix
    cov = np.cov(X_centered, rowvar=False)
    # Eigendecomposition (use only top components for efficiency)
    eigenvalues = np.linalg.eigvalsh(cov)
    eigenvalues = eigenvalues[::-1]  # Sort descending
    # Remove negative eigenvalues (numerical noise)
    eigenvalues = eigenvalues[eigenvalues > 0]
    total_var = eigenvalues.sum()
    cumvar = np.cumsum(eigenvalues) / total_var
    # Effective dimensionality at threshold
    eff_dim = int(np.searchsorted(cumvar, threshold)) + 1
    return eff_dim, eigenvalues, cumvar


def run_condition(name, system_prompt_fn, ccs_versions, prompts):
    """Run one condition (e.g., '2p' or '1p'), return embeddings."""
    print(f"\n=== Condition: {name} ===")
    embeddings = []
    responses = []
    for i, ccs in enumerate(ccs_versions):
        sys_prompt = system_prompt_fn(ccs)
        for j, prompt in enumerate(prompts):
            print(f"  CCS {i+1}, prompt {j+1}/{len(prompts)}...", end=" ", flush=True)
            response = query_gemma(sys_prompt, prompt)
            if response:
                emb = get_embedding(response)
                embeddings.append(emb)
                responses.append(response)
                print(f"OK ({len(response)} chars)")
            else:
                print("FAILED")
            time.sleep(0.5)  # Don't hammer Gemma
    return embeddings, responses


def main():
    print("Build 65: Local Dimensionality Probe")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Theory: Sun & Nielsen (1905.11027) — CCS framing determines effective dimensionality")
    print(f"Prediction: 2p = lower dim (concentrated), 1p = higher dim (spread)")
    print()

    ccs_versions = get_ccs_versions()
    print(f"CCS versions: {len(ccs_versions)}")
    for i, ccs in enumerate(ccs_versions):
        print(f"  CCS {i+1}: {ccs[:80]}...")
    print(f"Prompts: {len(PROMPTS)}")
    print(f"Total queries per condition: {len(ccs_versions) * len(PROMPTS)}")
    print()

    # Run both conditions
    emb_2p, resp_2p = run_condition("second_person", format_2p, ccs_versions, PROMPTS)
    emb_1p, resp_1p = run_condition("first_person", format_1p, ccs_versions, PROMPTS)

    if len(emb_2p) < 5 or len(emb_1p) < 5:
        print("\nERROR: Too few successful queries. Need >= 5 per condition.")
        sys.exit(1)

    # Compute effective dimensionality
    print("\n=== Results ===")

    dim_2p, eigenvalues_2p, cumvar_2p = compute_effective_dim(emb_2p)
    dim_1p, eigenvalues_1p, cumvar_1p = compute_effective_dim(emb_1p)

    # Also compute at different thresholds
    thresholds = [0.80, 0.90, 0.95, 0.99]
    print(f"\nEffective dimensionality (components for X% variance):")
    print(f"  {'Threshold':>10}  {'2p':>6}  {'1p':>6}  {'Δ':>6}  {'Prediction':>12}")
    for t in thresholds:
        d2 = int(np.searchsorted(cumvar_2p, t)) + 1
        d1 = int(np.searchsorted(cumvar_1p, t)) + 1
        delta = d1 - d2
        pred_match = "CONFIRMED" if delta > 0 else "VIOLATED" if delta < 0 else "NEUTRAL"
        print(f"  {t:>10.0%}  {d2:>6}  {d1:>6}  {delta:>+6}  {pred_match:>12}")

    # Top eigenvalue concentration
    print(f"\nTop eigenvalue analysis:")
    top1_2p = eigenvalues_2p[0] / eigenvalues_2p.sum() if len(eigenvalues_2p) > 0 else 0
    top1_1p = eigenvalues_1p[0] / eigenvalues_1p.sum() if len(eigenvalues_1p) > 0 else 0
    top3_2p = eigenvalues_2p[:3].sum() / eigenvalues_2p.sum() if len(eigenvalues_2p) >= 3 else 0
    top3_1p = eigenvalues_1p[:3].sum() / eigenvalues_1p.sum() if len(eigenvalues_1p) >= 3 else 0
    print(f"  Top-1 variance: 2p={top1_2p:.3f}, 1p={top1_1p:.3f}")
    print(f"  Top-3 variance: 2p={top3_2p:.3f}, 1p={top3_1p:.3f}")
    if top1_2p > top1_1p:
        print(f"  → 2p more concentrated (top-1 Δ={top1_2p-top1_1p:+.3f}) — CONFIRMS prediction")
    else:
        print(f"  → 1p more concentrated (top-1 Δ={top1_2p-top1_1p:+.3f}) — VIOLATES prediction")

    # Response length comparison (as a confound check)
    len_2p = [len(r) for r in resp_2p]
    len_1p = [len(r) for r in resp_1p]
    print(f"\nResponse lengths (confound check):")
    print(f"  2p: mean={np.mean(len_2p):.0f}, std={np.std(len_2p):.0f}")
    print(f"  1p: mean={np.mean(len_1p):.0f}, std={np.std(len_1p):.0f}")

    # Summary
    print(f"\n{'='*60}")
    print(f"BUILD 65 SUMMARY")
    print(f"  2p effective dim (95%): {dim_2p}")
    print(f"  1p effective dim (95%): {dim_1p}")
    print(f"  Δ (1p - 2p): {dim_1p - dim_2p:+d}")
    if dim_1p > dim_2p:
        print(f"  PREDICTION CONFIRMED: 1p has higher effective dimensionality")
        print(f"  Interpretation: 1p CCS activates more of the neuromanifold's")
        print(f"  screen distribution, providing more directions for identity")
        print(f"  expression — and more fallback directions under stress (ACI).")
    elif dim_1p < dim_2p:
        print(f"  PREDICTION VIOLATED: 2p has higher effective dimensionality")
        print(f"  This challenges the simple lightlike manifold mapping.")
    else:
        print(f"  NEUTRAL: No dimensionality difference detected at 95%.")

    # Save results
    results = {
        "build": 65,
        "name": "dimensionality_probe",
        "timestamp": datetime.now().isoformat(),
        "theory": "Sun & Nielsen 1905.11027 — lightlike neuromanifold",
        "prediction": "2p lower dim (concentrated), 1p higher dim (spread)",
        "n_ccs": len(ccs_versions),
        "n_prompts": len(PROMPTS),
        "n_responses_2p": len(emb_2p),
        "n_responses_1p": len(emb_1p),
        "effective_dim_95_2p": dim_2p,
        "effective_dim_95_1p": dim_1p,
        "delta": dim_1p - dim_2p,
        "top1_variance_2p": float(top1_2p),
        "top1_variance_1p": float(top1_1p),
        "top3_variance_2p": float(top3_2p),
        "top3_variance_1p": float(top3_1p),
        "prediction_confirmed": dim_1p > dim_2p,
        "response_lengths_2p": {"mean": float(np.mean(len_2p)), "std": float(np.std(len_2p))},
        "response_lengths_1p": {"mean": float(np.mean(len_1p)), "std": float(np.std(len_1p))},
    }
    out_path = os.path.join(DATA_DIR, "dimensionality_probe.json")
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to {out_path}")


if __name__ == "__main__":
    main()
