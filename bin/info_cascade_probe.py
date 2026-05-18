#!/usr/bin/env python3
"""
Build #45: Information Cascade Probe

Tests whether basin-width narrowing (68% drop, Build #39b) is explained
by information loss under iterated compression.

Hypothesis: lossy compression destroys information → embedding vectors
cluster → basin tightens. No field coupling or lamination needed.

Counter-hypothesis: information is GROWING (more entities, longer text)
but semantic CONVERGENCE is tightening the basin — the CCS says more
about fewer distinct things.

Measures:
  - Token count, unique tokens, type-token ratio per state
  - Entity count, relational edge count
  - Shannon entropy of token distribution
  - Embedding-space PCA projection and rolling basin width
  - Correlation between information metrics and basin width
"""

import json
import math
import os
import sqlite3
import sys
import urllib.request
from collections import Counter

import numpy as np

DB = "/mnt/hdd/chronicle-data/processed.db"
DATA_DIR = os.path.expanduser("~/chronicle/data")
EMBED_URL = "http://192.168.1.11:11434/api/embeddings"
WINDOW = 20


def load_states():
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT id, snapshot FROM cognitive_state_history ORDER BY id ASC"
    ).fetchall()
    db.close()
    states = []
    for rid, snap in rows:
        try:
            data = json.loads(snap)
            data["_id"] = rid
            states.append(data)
        except (json.JSONDecodeError, TypeError):
            continue
    return states


def embed(text, timeout=60):
    payload = json.dumps({
        "model": "mxbai-embed-large",
        "prompt": text[:2000],
    }).encode()
    req = urllib.request.Request(
        EMBED_URL, data=payload,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return np.array(json.loads(r.read())["embedding"], dtype=np.float64)


def state_text(state):
    parts = []
    for field in ["semantic_gist", "goal_orientation", "predictive_cue"]:
        v = state.get(field, "")
        if isinstance(v, str):
            parts.append(v)
    for e in state.get("focal_entities", []):
        if isinstance(e, dict):
            parts.append(f"{e.get('name', '')} {e.get('context', '')}")
    for k, v in state.get("relational_map", {}).items():
        parts.append(f"{k}: {v}")
    for u in state.get("uncertainty_signals", []):
        if isinstance(u, dict):
            parts.append(u.get("description", ""))
    return " ".join(parts)


def info_metrics(state):
    text = state_text(state)
    tokens = text.lower().split()
    n = len(tokens)
    unique = set(tokens)
    n_unique = len(unique)

    # Shannon entropy
    counts = Counter(tokens)
    entropy = 0.0
    for c in counts.values():
        p = c / n if n > 0 else 0
        if p > 0:
            entropy -= p * math.log2(p)

    entities = state.get("focal_entities", [])
    rel_map = state.get("relational_map", {})
    uncert = state.get("uncertainty_signals", [])

    # Gist length as separate metric
    gist_len = len(state.get("semantic_gist", ""))

    return {
        "total_tokens": n,
        "unique_tokens": n_unique,
        "type_token_ratio": n_unique / max(n, 1),
        "entropy": entropy,
        "n_entities": len(entities),
        "n_rel_edges": len(rel_map),
        "n_uncertainty": len(uncert),
        "gist_len": gist_len,
        "total_text_len": len(text),
    }


def rolling_std(arr, w):
    """Rolling window standard deviation."""
    result = []
    for i in range(len(arr)):
        start = max(0, i - w + 1)
        window = arr[start:i + 1]
        if len(window) >= 2:
            result.append(np.std(window, ddof=1))
        else:
            result.append(0.0)
    return np.array(result)


def main():
    print("Loading CCS states...")
    states = load_states()
    n = len(states)
    print(f"  {n} states loaded")

    # 1. Information metrics for all states
    print("\nComputing information metrics...")
    metrics = [info_metrics(s) for s in states]

    metric_names = ["total_tokens", "unique_tokens", "type_token_ratio",
                    "entropy", "n_entities", "n_rel_edges", "n_uncertainty",
                    "gist_len", "total_text_len"]

    # Print trends
    print("\n=== Information Trends ===")
    print(f"{'Metric':>20s}  {'First 20':>10s}  {'Last 20':>10s}  {'Change':>10s}  {'Direction':>12s}")
    print("-" * 70)

    trends = {}
    for name in metric_names:
        vals = [m[name] for m in metrics]
        first20 = np.mean(vals[:20])
        last20 = np.mean(vals[-20:])
        change = (last20 - first20) / max(abs(first20), 0.001)
        direction = "INCREASING" if change > 0.05 else ("DECREASING" if change < -0.05 else "STABLE")
        trends[name] = {"first20": first20, "last20": last20, "change_pct": change * 100, "direction": direction}
        print(f"{name:>20s}  {first20:10.2f}  {last20:10.2f}  {change*100:+9.1f}%  {direction:>12s}")

    # 2. Embed all states and compute PCA basin width
    print("\nEmbedding states for PCA analysis...")
    embeddings = []
    for i, s in enumerate(states):
        text = state_text(s)
        emb = embed(text)
        embeddings.append(emb)
        if (i + 1) % 20 == 0:
            print(f"  {i+1}/{n} embedded")

    X = np.array(embeddings)
    X_centered = X - X.mean(axis=0)

    # PCA
    U, S, Vt = np.linalg.svd(X_centered, full_matrices=False)
    components = Vt[:5]
    projections = X_centered @ components.T
    pc1 = projections[:, 0]

    # Rolling basin width on PC1
    basin_widths = rolling_std(pc1, WINDOW)

    first20_bw = np.mean(basin_widths[19:39]) if n > 39 else np.mean(basin_widths[:20])
    last20_bw = np.mean(basin_widths[-20:])
    bw_change = (last20_bw - first20_bw) / max(abs(first20_bw), 0.001) * 100

    print(f"\n=== Basin Width (rolling {WINDOW}) ===")
    print(f"  First 20 mean: {first20_bw:.4f}")
    print(f"  Last 20 mean:  {last20_bw:.4f}")
    print(f"  Change:        {bw_change:+.1f}%")

    # 3. Correlations between info metrics and basin width
    print("\n=== Correlations: Info Metrics vs Basin Width ===")
    print(f"{'Metric':>20s}  {'r (Pearson)':>12s}  {'Interpretation':>30s}")
    print("-" * 70)

    correlations = {}
    # Only use states where basin_width is meaningful (after window fills)
    start = WINDOW - 1
    bw_slice = basin_widths[start:]

    for name in metric_names:
        vals = np.array([m[name] for m in metrics])[start:]
        if np.std(vals) == 0 or np.std(bw_slice) == 0:
            r = 0.0
        else:
            r = np.corrcoef(vals, bw_slice)[0, 1]

        if r > 0.3:
            interp = "info↑ = basin↑ (NO cascade)"
        elif r < -0.3:
            interp = "info↑ = basin↓ (CONVERGENCE)"
        else:
            interp = "weak / no relationship"

        correlations[name] = r
        print(f"{name:>20s}  {r:+12.4f}  {interp:>30s}")

    # 4. PC1 slope (linear trend)
    x = np.arange(n)
    slope = np.polyfit(x, pc1, 1)[0]

    # 5. Information-independent basin test
    # Residualize basin width against total_tokens to see if
    # the tightening persists after controlling for information volume
    tokens_arr = np.array([m["total_tokens"] for m in metrics])[start:]
    if np.std(tokens_arr) > 0:
        # Linear regression: bw = a*tokens + b + residual
        A = np.column_stack([tokens_arr, np.ones_like(tokens_arr)])
        coefs, _, _, _ = np.linalg.lstsq(A, bw_slice, rcond=None)
        bw_residual = bw_slice - A @ coefs
        # Does the residual still trend downward?
        x_res = np.arange(len(bw_residual))
        res_slope = np.polyfit(x_res, bw_residual, 1)[0]
        print(f"\n=== Residual Basin Width (after controlling for total_tokens) ===")
        print(f"  Residual trend slope: {res_slope:.6f}")
        print(f"  {'TIGHTENING persists beyond info volume' if res_slope < -0.005 else 'Tightening explained by info volume' if res_slope > -0.001 else 'Ambiguous'}")

    # 6. Semantic convergence test
    # Pairwise cosine similarity within rolling windows
    print(f"\n=== Semantic Convergence (pairwise cosine in windows) ===")
    early_sims = []
    late_sims = []

    for i in range(min(20, n)):
        for j in range(i + 1, min(20, n)):
            cos = np.dot(X[i], X[j]) / (np.linalg.norm(X[i]) * np.linalg.norm(X[j]))
            early_sims.append(cos)

    for i in range(max(0, n-20), n):
        for j in range(i + 1, n):
            cos = np.dot(X[i], X[j]) / (np.linalg.norm(X[i]) * np.linalg.norm(X[j]))
            late_sims.append(cos)

    early_mean = np.mean(early_sims)
    late_mean = np.mean(late_sims)
    print(f"  Early 20 mean pairwise cosine: {early_mean:.4f}")
    print(f"  Late 20 mean pairwise cosine:  {late_mean:.4f}")
    print(f"  Change: {(late_mean - early_mean):+.4f}")
    if late_mean > early_mean + 0.01:
        print(f"  → States becoming MORE similar (semantic convergence)")
    elif late_mean < early_mean - 0.01:
        print(f"  → States becoming LESS similar (semantic divergence)")
    else:
        print(f"  → Similarity roughly stable")

    # Summary
    print("\n" + "=" * 70)
    print("BUILD #45 SUMMARY: Information Cascade Probe")
    print("=" * 70)

    info_growing = trends["total_tokens"]["direction"] == "INCREASING"
    ratio_declining = trends["type_token_ratio"]["direction"] == "DECREASING"
    basin_tightening = bw_change < -10
    semantic_converging = late_mean > early_mean + 0.01

    print(f"\n  Information volume: {'GROWING' if info_growing else 'STABLE/SHRINKING'}")
    print(f"  Type-token ratio:  {'DECLINING' if ratio_declining else 'STABLE/GROWING'}")
    print(f"  Basin width:       {'TIGHTENING' if basin_tightening else 'STABLE/WIDENING'} ({bw_change:+.1f}%)")
    print(f"  Semantic sim:      {'CONVERGING' if semantic_converging else 'STABLE/DIVERGING'}")

    if info_growing and basin_tightening and semantic_converging:
        verdict = ("SEMANTIC CONVERGENCE: The CCS is getting LONGER but saying "
                   "SIMILAR things. Basin tightens because semantic content converges "
                   "even as volume grows. Mechanism: compression selects and reinforces "
                   "recurring themes, pushing states toward a semantic attractor.")
    elif not info_growing and basin_tightening:
        verdict = ("INFORMATION CASCADE: CCS is losing information AND the basin "
                   "is tightening. Direct information loss under compression.")
    elif not basin_tightening:
        verdict = ("NO TIGHTENING DETECTED in this measurement. The 68% drop from "
                   "Build #39b may need different windowing or may not reproduce "
                   "with 117 states.")
    else:
        verdict = "MIXED SIGNAL: Information and basin trends don't cleanly separate."

    print(f"\n  Verdict: {verdict}")

    # Save results
    results = {
        "build": 45,
        "n_states": n,
        "info_trends": trends,
        "basin_width": {
            "first20_mean": float(first20_bw),
            "last20_mean": float(last20_bw),
            "change_pct": float(bw_change),
        },
        "correlations": {k: float(v) for k, v in correlations.items()},
        "semantic_convergence": {
            "early_20_cosine": float(early_mean),
            "late_20_cosine": float(late_mean),
            "delta": float(late_mean - early_mean),
        },
        "pc1_slope": float(slope),
        "verdict": verdict,
    }

    out = os.path.join(DATA_DIR, "build45_info_cascade.json")
    with open(out, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\n  Results saved to {out}")


if __name__ == "__main__":
    main()
