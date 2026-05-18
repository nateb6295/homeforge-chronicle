#!/usr/bin/env python3
"""
Build #50b: ΦID Causal Emergence Probe

Computes causal emergence (ΦID) over the CCS state trajectory,
following Pigozzi & Levin (2026) "The Causally Emergent Alignment
Hypothesis" (arxiv:2605.06746).

ΦID = Downward causation + Synergy
  - Downward causation: whole predicts parts' futures
  - Synergy: whole predicts its own future

Method (adapted from paper):
  1. Embed all CCS states → 1024-dim vectors
  2. PCA to m=2 dimensions (per paper's recommendation)
  3. Copula-based Gaussianization (rank-normal transform)
  4. Lag-1 mutual information matrix for bipartitioned system
  5. Fiedler vector bipartition (graph Laplacian)
  6. Solve for ΦID atoms

For Gaussian continuous variables, mutual information simplifies to:
  I(X,Y) = -ln(1 - ρ²) / 2
where ρ is Pearson correlation.
"""

import json
import sqlite3

import numpy as np
import urllib.request
from scipy import stats

DB = "/mnt/hdd/chronicle-data/processed.db"
EMBED_URL = "http://192.168.1.11:11434/api/embeddings"


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
    for c in state.get("constraints", []):
        if isinstance(c, str):
            parts.append(c)
    et = state.get("episodic_trace", [])
    if isinstance(et, list):
        for e in et:
            if isinstance(e, str):
                parts.append(e)
    return " ".join(parts)


def rank_normal_transform(data):
    """Copula-based Gaussianization via rank-normal transform."""
    n = data.shape[0]
    result = np.zeros_like(data)
    for j in range(data.shape[1]):
        ranks = stats.rankdata(data[:, j])
        uniform = (ranks - 0.5) / n
        result[:, j] = stats.norm.ppf(uniform)
    return result


def gaussian_mi(x, y):
    """Mutual information for Gaussian variables: I(X,Y) = -ln(1-ρ²)/2"""
    rho = np.corrcoef(x, y)[0, 1]
    rho = np.clip(rho, -0.999, 0.999)
    return -np.log(1 - rho**2) / 2


def fiedler_bipartition(mi_matrix):
    """Bipartition using Fiedler vector of the graph Laplacian."""
    n = mi_matrix.shape[0]
    if n < 2:
        return [0], []
    degree = np.sum(mi_matrix, axis=1)
    laplacian = np.diag(degree) - mi_matrix
    eigenvalues, eigenvectors = np.linalg.eigh(laplacian)
    fiedler = eigenvectors[:, 1]
    group_a = np.where(fiedler >= 0)[0].tolist()
    group_b = np.where(fiedler < 0)[0].tolist()
    if not group_a:
        group_a = [0]
        group_b = list(range(1, n))
    if not group_b:
        group_b = [n - 1]
        group_a = list(range(n - 1))
    return group_a, group_b


def compute_phi_id(Z, window_size=20):
    """
    Compute ΦID trajectory over time series Z.

    Z: array of shape (T, d) — time series of d-dimensional states
    window_size: number of steps per window

    Returns list of ΦID values, one per window.
    """
    T, d = Z.shape
    if d < 2:
        return []

    phi_values = []

    for start in range(0, T - window_size):
        window = Z[start:start + window_size]

        mi_matrix = np.zeros((d, d))
        for i in range(d):
            for j in range(i + 1, d):
                mi = gaussian_mi(window[:-1, i], window[1:, j])
                mi_matrix[i, j] = max(mi, 0)
                mi_matrix[j, i] = max(mi, 0)

        group_a, group_b = fiedler_bipartition(mi_matrix)

        mean_a = window[:, group_a].mean(axis=1)
        mean_b = window[:, group_b].mean(axis=1)
        whole = window.mean(axis=1)

        mi_whole_future = gaussian_mi(whole[:-1], whole[1:])

        mi_a_future_a = gaussian_mi(mean_a[:-1], mean_a[1:])
        mi_b_future_b = gaussian_mi(mean_b[:-1], mean_b[1:])
        mi_parts = (mi_a_future_a + mi_b_future_b) / 2

        mi_whole_future_a = gaussian_mi(whole[:-1], mean_a[1:])
        mi_whole_future_b = gaussian_mi(whole[:-1], mean_b[1:])
        downward_causation = (mi_whole_future_a + mi_whole_future_b) / 2 - mi_parts

        synergy = mi_whole_future - (mi_whole_future_a + mi_whole_future_b) / 2

        phi = max(downward_causation + synergy, 0)
        phi_values.append(phi)

    return phi_values


def main():
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT snapshot FROM cognitive_state_history ORDER BY id ASC"
    ).fetchall()
    db.close()

    states = []
    for r in rows:
        try:
            states.append(json.loads(r[0]))
        except (json.JSONDecodeError, TypeError):
            continue

    n = len(states)
    print(f"Loaded {n} CCS states")

    texts = [state_text(s) for s in states]

    print("Embedding all states...")
    embeddings = []
    for i, t in enumerate(texts):
        embeddings.append(embed(t))
        if (i + 1) % 20 == 0:
            print(f"  {i+1}/{n}")
    embeddings = np.array(embeddings)

    print("PCA projection...")
    mean_emb = embeddings.mean(axis=0)
    centered = embeddings - mean_emb
    _, S, Vt = np.linalg.svd(centered, full_matrices=False)

    for m in [2, 4, 6, 8]:
        projected = centered @ Vt[:m].T

        print(f"\n--- ΦID with m={m} PCA dimensions ---")

        gaussianized = rank_normal_transform(projected)

        for window in [10, 15, 20]:
            if window >= n - 1:
                continue
            phi_values = compute_phi_id(gaussianized, window_size=window)
            if not phi_values:
                continue

            phi_arr = np.array(phi_values)
            print(f"  Window={window}: mean ΦID={phi_arr.mean():.6f}, "
                  f"std={phi_arr.std():.6f}, "
                  f"min={phi_arr.min():.6f}, max={phi_arr.max():.6f}")

            n_phi = len(phi_values)
            if n_phi >= 20:
                first_half = phi_arr[:n_phi // 2]
                second_half = phi_arr[n_phi // 2:]
                print(f"    First half: {first_half.mean():.6f}, "
                      f"Second half: {second_half.mean():.6f}, "
                      f"Δ={second_half.mean() - first_half.mean():+.6f}")

                thirds = np.array_split(phi_arr, 3)
                for i, third in enumerate(thirds):
                    phase_label = ["Phase 1 (early)", "Phase 2 (mid)", "Phase 3 (late)"][i]
                    print(f"    {phase_label}: mean={third.mean():.6f}")

    best_m = 2
    best_window = 15
    projected = centered @ Vt[:best_m].T
    gaussianized = rank_normal_transform(projected)
    phi_values = compute_phi_id(gaussianized, window_size=best_window)

    print(f"\n--- ΦID Trajectory (m={best_m}, window={best_window}) ---")
    print(f"{'Step':>4} {'ΦID':>10}")
    for i, phi in enumerate(phi_values):
        marker = ""
        if i == 0:
            marker = " ← start"
        elif i == len(phi_values) - 1:
            marker = " ← end"
        print(f"{i + best_window:>4} {phi:>10.6f}{marker}")

    lag1_autocorr = np.corrcoef(
        np.array(phi_values[:-1]), np.array(phi_values[1:])
    )[0, 1] if len(phi_values) > 2 else 0.0

    print(f"\nLag-1 autocorrelation of ΦID: {lag1_autocorr:.4f}")

    trend_x = np.arange(len(phi_values))
    if len(phi_values) > 2:
        slope, intercept, r_value, _, _ = stats.linregress(trend_x, phi_values)
        print(f"Linear trend: slope={slope:.6f}, r={r_value:.4f}")

    results = {
        "n_states": n,
        "phi_trajectory": phi_values,
        "pca_dimensions": best_m,
        "window_size": best_window,
        "mean_phi": float(np.mean(phi_values)),
        "std_phi": float(np.std(phi_values)),
        "lag1_autocorrelation": float(lag1_autocorr),
        "trend_slope": float(slope) if len(phi_values) > 2 else 0.0,
        "trend_r": float(r_value) if len(phi_values) > 2 else 0.0,
    }

    out = "/home/nate-agx/chronicle/data/build50b_phi_id_trajectory.json"
    with open(out, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to {out}")


if __name__ == "__main__":
    main()
