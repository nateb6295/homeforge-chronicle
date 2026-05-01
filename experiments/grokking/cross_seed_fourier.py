#!/usr/bin/env python3
"""
Cross-seed Fourier alignment test.

Claim from tonight's thread: Y-independence is empirically earned iff the direction
of sensitivity concentration aligns across seeds even when internal representations
differ. For the mod-97 transformer, Y = mod-97 arithmetic. The "direction" is the
set of Fourier frequencies that carry function-sensitivity.

For each seed (0, 1, 2):
  - Load final token embedding (97 tokens x 128 dims)
  - For each embedding dim, take DFT across the 97 token axis
  - Sum power across dims to get per-frequency energy profile (length 97)
  - Normalize to a distribution over frequencies

Test: are the three per-frequency energy profiles aligned at the same peaks?

If YES -> Y-independence empirically earned. Same arithmetic structure forces
convergence on the same Fourier subspace regardless of random init.
If NO  -> the "Y" is not actually independent; what the models learn is seed-
dependent; claim falls.
"""

import json
from pathlib import Path

import numpy as np
import torch


RUNS_ROOT = Path("/home/nate-agx/chronicle/experiments/grokking/runs")
SEEDS = {
    "seed0": "v2",
    "seed1": "v2_seed1",
    "seed2": "v2_seed2",
}
# Step well after grokking to get a stable post-grok snapshot.
# Pick the latest step < 50k that's a stable (val_acc >= 0.99) snapshot.
TARGET_STEP = 49000


def load_embedding(run_tag: str, step: int) -> np.ndarray:
    """Return the token embedding matrix of shape (P, d_model). P=97 (excludes '=' token)."""
    path = RUNS_ROOT / run_tag / "snapshots" / f"step_{step:06d}.pt"
    sd = torch.load(path, map_location="cpu", weights_only=True)
    # tok_emb.weight: shape (P+1, d_model); we want rows 0..P-1 (the number tokens)
    emb = sd["tok_emb.weight"][:97].cpu().numpy()
    return emb


def fourier_profile(emb: np.ndarray) -> np.ndarray:
    """DFT across the 97-token axis for each of d_model embedding dims.
    Returns per-frequency energy of shape (P,) normalized to sum 1."""
    # FFT along axis 0 (tokens); shape (P, d_model) -> (P, d_model)
    f = np.fft.fft(emb, axis=0)
    # power per (freq, dim), sum across dims
    power = np.abs(f) ** 2
    per_freq = power.sum(axis=1)
    # fold negative frequencies onto positive (freq k and P-k are complex conj)
    P = len(per_freq)
    folded = np.zeros(P // 2 + 1)
    folded[0] = per_freq[0]
    for k in range(1, P // 2 + 1):
        if k == P - k:
            folded[k] = per_freq[k]
        else:
            folded[k] = per_freq[k] + per_freq[P - k]
    # normalize (exclude DC for the alignment test since DC is always big and carries no Y structure)
    non_dc = folded[1:].sum()
    if non_dc > 0:
        folded[1:] = folded[1:] / non_dc
    return folded


def top_k_freqs(profile: np.ndarray, k: int = 8) -> list[tuple[int, float]]:
    """Return top-k (freq, normalized_power) pairs, excluding DC."""
    ac = profile[1:].copy()
    idx = np.argsort(ac)[::-1][:k]
    return [(int(i + 1), float(ac[i])) for i in idx]


def cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    """Cosine similarity between two 1D vectors, ignoring DC component."""
    x, y = a[1:], b[1:]
    nx, ny = np.linalg.norm(x), np.linalg.norm(y)
    if nx == 0 or ny == 0:
        return 0.0
    return float(np.dot(x, y) / (nx * ny))


def main():
    print(f"Cross-seed Fourier alignment test @ step {TARGET_STEP}")
    print(f"Seeds: {list(SEEDS.keys())}\n")

    profiles = {}
    for seed_name, tag in SEEDS.items():
        try:
            emb = load_embedding(tag, TARGET_STEP)
        except FileNotFoundError as e:
            print(f"  [{seed_name}] MISSING snapshot — {e}")
            return
        prof = fourier_profile(emb)
        profiles[seed_name] = prof
        top = top_k_freqs(prof, k=8)
        top_str = ", ".join(f"k={f}:{p:.3f}" for f, p in top)
        print(f"  [{seed_name}] top-8 frequencies:  {top_str}")

    # Pairwise cosine similarity of frequency profiles
    print("\nPairwise cosine similarity of frequency-energy profiles (excluding DC):")
    names = list(profiles.keys())
    for i in range(len(names)):
        for j in range(i + 1, len(names)):
            sim = cosine_similarity(profiles[names[i]], profiles[names[j]])
            print(f"  {names[i]} vs {names[j]}: {sim:.4f}")

    # Identify top frequencies common across seeds
    top_sets = [set(f for f, _ in top_k_freqs(p, k=8)) for p in profiles.values()]
    common = set.intersection(*top_sets) if top_sets else set()
    print(f"\nFrequencies in top-8 of ALL {len(SEEDS)} seeds: {sorted(common)}")

    # Verdict
    print("\n=== VERDICT ===")
    mean_sim = np.mean([
        cosine_similarity(profiles[names[i]], profiles[names[j]])
        for i in range(len(names))
        for j in range(i + 1, len(names))
    ])
    print(f"mean pairwise cosine sim: {mean_sim:.4f}")
    print(f"frequencies shared across all seeds: {len(common)}/8")
    if mean_sim > 0.7 and len(common) >= 4:
        print("RESULT: Strong cross-seed alignment. Y-independence empirically earned.")
    elif mean_sim > 0.4:
        print("RESULT: Partial alignment. Y has some independent structure but seeds "
              "diverge on details.")
    else:
        print("RESULT: Poor alignment. Seeds found different solutions. "
              "Claim of Y-independence fails this test.")


if __name__ == "__main__":
    main()
