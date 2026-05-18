#!/usr/bin/env python3
"""
Brownian Drift Probe — Thread #319 (Trip Deep-Work)

Tests whether CCS compression bias accumulates directionally or random-walks.

Method: embed each CCS gist, compute step-wise displacement vectors in
embedding space. If drift is directional, displacements correlate with
each other (positive autocorrelation). If random-walk, displacements
are uncorrelated.

Also decomposes drift into:
  - Net displacement (start→end vector magnitude)
  - Total path length (sum of step magnitudes)
  - Directional ratio = net / total (1.0 = straight line, ~0 = random walk)
  - MSD scaling: <r²> ~ t^α. α=1 → diffusion, α>1 → superdiffusion (directed), α<1 → subdiffusion (confined)

Can partition by time window to compare pre-trip vs during-trip drift.
"""

import json
import os
import sqlite3
import struct
import sys
import urllib.request

import numpy as np
from scipy import stats as sp_stats

DB = "/mnt/hdd/chronicle-data/processed.db"
EMBED_URL = "http://192.168.1.11:11434/api/embeddings"
DATA_DIR = os.path.expanduser("~/chronicle/data")


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


def load_states():
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT id, snapshot, created_at, trigger FROM cognitive_state_history ORDER BY id ASC"
    ).fetchall()
    db.close()

    states = []
    for rid, snap, ts, trigger in rows:
        try:
            data = json.loads(snap)
            data["_id"] = rid
            data["_ts"] = ts
            data["_trigger"] = trigger
            states.append(data)
        except (json.JSONDecodeError, TypeError):
            continue
    return states


def embed_gist(state):
    gist = state.get("semantic_gist", "")
    if not gist:
        return None
    return embed(gist)


def msd_scaling(positions):
    """Compute mean squared displacement as function of lag τ."""
    n = len(positions)
    max_tau = min(n // 4, 50)
    taus = []
    msds = []
    for tau in range(1, max_tau + 1):
        displacements = positions[tau:] - positions[:-tau]
        sq_disp = np.sum(displacements ** 2, axis=1)
        taus.append(tau)
        msds.append(sq_disp.mean())
    return np.array(taus), np.array(msds)


def main():
    use_cached = "--no-cache" not in sys.argv
    partition_id = None
    for arg in sys.argv[1:]:
        if arg.startswith("--partition="):
            partition_id = int(arg.split("=")[1])

    states = load_states()
    n = len(states)
    print(f"Loaded {n} CCS states")

    # Check for cached gist embeddings
    cache_path = os.path.join(DATA_DIR, "ccs_gist_embeddings.npy")
    cache_n_path = os.path.join(DATA_DIR, "ccs_gist_embeddings_n.txt")

    cached_n = 0
    if use_cached and os.path.exists(cache_path) and os.path.exists(cache_n_path):
        with open(cache_n_path) as f:
            cached_n = int(f.read().strip())
        if cached_n <= n:
            cached = np.load(cache_path)
            print(f"Loaded {cached_n} cached gist embeddings")
        else:
            cached_n = 0

    embeddings = []
    if cached_n > 0:
        embeddings = list(cached[:cached_n])

    if cached_n < n:
        print(f"Embedding gists {cached_n + 1}–{n}...")
        for i in range(cached_n, n):
            try:
                e = embed_gist(states[i])
                if e is not None:
                    embeddings.append(e)
                elif embeddings:
                    embeddings.append(embeddings[-1].copy())
                else:
                    embeddings.append(np.zeros(1024))
            except Exception as ex:
                print(f"  state {states[i].get('_id', '?')}: embed failed ({ex})")
                if embeddings:
                    embeddings.append(embeddings[-1].copy())
                else:
                    embeddings.append(np.zeros(1024))
            if (i + 1) % 20 == 0 or i == n - 1:
                print(f"  {i + 1}/{n}")

    emb_array = np.array(embeddings[:n])

    # Save cache
    np.save(cache_path, emb_array)
    with open(cache_n_path, "w") as f:
        f.write(str(n))

    # --- Analysis ---
    print(f"\n{'=' * 60}")
    print(f"  BROWNIAN DRIFT PROBE — CCS GIST TRAJECTORY")
    print(f"  {n} states")
    print(f"{'=' * 60}\n")

    # Step displacements
    steps = np.diff(emb_array, axis=0)
    step_mags = np.linalg.norm(steps, axis=1)

    # Net displacement
    net_vec = emb_array[-1] - emb_array[0]
    net_mag = np.linalg.norm(net_vec)
    total_path = step_mags.sum()
    directional_ratio = net_mag / total_path if total_path > 0 else 0

    print(f"Step statistics:")
    print(f"  Mean step magnitude: {step_mags.mean():.6f}")
    print(f"  Std step magnitude:  {step_mags.std():.6f}")
    print(f"  Max step:            {step_mags.max():.6f}")
    print(f"  Min step:            {step_mags.min():.6f}")
    print(f"\nTrajectory:")
    print(f"  Net displacement:    {net_mag:.6f}")
    print(f"  Total path length:   {total_path:.6f}")
    print(f"  Directional ratio:   {directional_ratio:.4f} (1.0=straight, ~0=random walk)")

    # Step autocorrelation (do successive steps point in the same direction?)
    if len(steps) > 2:
        step_norms = steps / (np.linalg.norm(steps, axis=1, keepdims=True) + 1e-12)
        autocorrs = []
        for lag in range(1, min(20, len(steps))):
            dots = np.sum(step_norms[:-lag] * step_norms[lag:], axis=1)
            autocorrs.append((lag, dots.mean(), dots.std()))

        print(f"\nStep direction autocorrelation:")
        print(f"  {'Lag':>4} {'Mean r':>10} {'Std':>10} {'Interpretation':>20}")
        print(f"  {'-' * 44}")
        for lag, mean_r, std_r in autocorrs[:10]:
            interp = "directional" if mean_r > 0.1 else ("reverting" if mean_r < -0.1 else "random")
            print(f"  {lag:>4} {mean_r:>10.4f} {std_r:>10.4f} {interp:>20}")

    # MSD scaling
    print(f"\nMSD scaling (α=1 → diffusion, α>1 → directed, α<1 → confined):")
    taus, msds = msd_scaling(emb_array)
    if len(taus) > 3:
        log_taus = np.log(taus)
        log_msds = np.log(msds + 1e-20)
        slope, intercept, r, _, _ = sp_stats.linregress(log_taus, log_msds)
        print(f"  α = {slope:.3f} (r² = {r**2:.3f})")
        if slope > 1.2:
            print(f"  → SUPERDIFFUSION: CCS drifts directionally")
        elif slope < 0.8:
            print(f"  → SUBDIFFUSION: CCS is confined/attracted to basin")
        else:
            print(f"  → NORMAL DIFFUSION: CCS random-walks")

    # Partition analysis (pre-trip vs during-trip)
    if partition_id:
        pre_idx = [i for i, s in enumerate(states) if s["_id"] <= partition_id]
        post_idx = [i for i, s in enumerate(states) if s["_id"] > partition_id]
        if len(pre_idx) > 5 and len(post_idx) > 5:
            print(f"\n{'=' * 60}")
            print(f"  PARTITION ANALYSIS (split at state {partition_id})")
            print(f"  Pre: {len(pre_idx)} states, Post: {len(post_idx)} states")
            print(f"{'=' * 60}")

            for label, idxs in [("PRE", pre_idx), ("POST", post_idx)]:
                sub_emb = emb_array[idxs]
                sub_steps = np.diff(sub_emb, axis=0)
                sub_mags = np.linalg.norm(sub_steps, axis=1)
                sub_net = np.linalg.norm(sub_emb[-1] - sub_emb[0])
                sub_total = sub_mags.sum()
                sub_ratio = sub_net / sub_total if sub_total > 0 else 0

                sub_taus, sub_msds = msd_scaling(sub_emb)
                if len(sub_taus) > 3:
                    sl, _, r, _, _ = sp_stats.linregress(np.log(sub_taus), np.log(sub_msds + 1e-20))
                else:
                    sl, r = float('nan'), float('nan')

                print(f"\n  {label}:")
                print(f"    Mean step: {sub_mags.mean():.6f}, Directional ratio: {sub_ratio:.4f}")
                print(f"    MSD α = {sl:.3f} (r² = {r**2:.3f})")

    # Save results
    results = {
        "n_states": n,
        "mean_step": float(step_mags.mean()),
        "std_step": float(step_mags.std()),
        "net_displacement": float(net_mag),
        "total_path": float(total_path),
        "directional_ratio": float(directional_ratio),
        "msd_alpha": float(slope) if len(taus) > 3 else None,
        "autocorr_lag1": float(autocorrs[0][1]) if autocorrs else None,
        "timestamp": int(states[-1]["_ts"]) if states else 0,
    }
    out_path = os.path.join(DATA_DIR, "brownian_drift_results.json")
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to {out_path}")


if __name__ == "__main__":
    main()
