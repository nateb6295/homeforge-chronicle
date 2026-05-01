#!/usr/bin/env python3
"""Eigenvalue Decay Test — tests CCS Fisher profiles against sloppy model predictions.

Sethna/Transtrum/Machta proved that smooth models with bounded predictions
necessarily have hierarchical (geometric) eigenvalue decay. If CCS compression
behaves as a sloppy model, its Fisher information profile should show:
- A few stiff directions (high drop_per_kt) with geometric spacing
- Many sloppy directions (near-zero drop_per_kt) clustered together
- The ratio between consecutive eigenvalues should be roughly constant

This script:
1. Loads accumulated Fisher profiles from fisher_profiles.jsonl
2. Extracts drop_per_kt values as proxy eigenvalues
3. Tests for geometric decay in the non-zero cluster
4. Tests for stiff/sloppy separation
5. Tracks antifragility: do Fisher scores increase over compression events?

Requires 5+ profiles for meaningful results. Wired to run manually or
from stabilized_compress.py when threshold is reached.

Usage:
  python3 eigenvalue_decay_test.py           # Test current profiles
  python3 eigenvalue_decay_test.py --verbose  # Show all intermediate computations
  python3 eigenvalue_decay_test.py --plot     # ASCII histogram of eigenvalue distribution
"""

import argparse
import json
import os
import sys
from pathlib import Path

FISHER_LOG = Path(os.path.expanduser("~/chronicle/data/fisher_profiles.jsonl"))
RESULT_LOG = Path(os.path.expanduser("~/chronicle/data/eigenvalue_decay_results.jsonl"))
MIN_PROFILES = 5
GEOMETRIC_RATIO_TOLERANCE = 0.3  # ratios within 30% of mean = "roughly constant"


def load_profiles() -> list[dict]:
    """Load Fisher profiles from log file."""
    if not FISHER_LOG.exists():
        return []
    profiles = []
    for line in FISHER_LOG.read_text().strip().split("\n"):
        if line.strip():
            try:
                profiles.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return profiles


def extract_eigenvalues(profile: dict) -> list[tuple[str, float]]:
    """Extract (field_name, drop_per_kt) pairs sorted descending."""
    fields = profile.get("profile", profile.get("fields", {}))
    pairs = []
    for name, data in fields.items():
        val = data.get("drop_per_kt", 0.0)
        pairs.append((name, val))
    return sorted(pairs, key=lambda x: -x[1])


def test_geometric_decay(eigenvalues: list[float]) -> dict:
    """Test whether non-zero eigenvalues follow geometric decay.

    Returns dict with:
    - ratios: consecutive ratios λ_{i+1}/λ_i
    - mean_ratio: average ratio
    - cv: coefficient of variation of ratios (lower = more geometric)
    - is_geometric: True if CV < tolerance
    """
    # Filter to non-zero (> 0.001) eigenvalues
    nonzero = [v for v in eigenvalues if v > 0.001]

    if len(nonzero) < 3:
        return {"status": "insufficient_nonzero", "n_nonzero": len(nonzero)}

    ratios = []
    for i in range(1, len(nonzero)):
        if nonzero[i - 1] > 0:
            ratios.append(nonzero[i] / nonzero[i - 1])

    if not ratios:
        return {"status": "no_ratios"}

    mean_ratio = sum(ratios) / len(ratios)
    variance = sum((r - mean_ratio) ** 2 for r in ratios) / len(ratios)
    std = variance ** 0.5
    cv = std / mean_ratio if mean_ratio > 0 else float("inf")

    return {
        "status": "tested",
        "ratios": [round(r, 4) for r in ratios],
        "mean_ratio": round(mean_ratio, 4),
        "cv": round(cv, 4),
        "is_geometric": cv < GEOMETRIC_RATIO_TOLERANCE,
        "n_nonzero": len(nonzero),
    }


def test_stiff_sloppy_separation(eigenvalues: list[float]) -> dict:
    """Test for stiff/sloppy separation — gap between clusters.

    Looks for a gap where eigenvalue drops by > 5x between consecutive values.
    """
    if len(eigenvalues) < 3:
        return {"status": "insufficient"}

    max_drop_idx = 0
    max_drop_ratio = 0
    for i in range(1, len(eigenvalues)):
        if eigenvalues[i] > 0:
            ratio = eigenvalues[i - 1] / eigenvalues[i]
            if ratio > max_drop_ratio:
                max_drop_ratio = ratio
                max_drop_idx = i

    n_stiff = max_drop_idx
    n_sloppy = len(eigenvalues) - max_drop_idx

    return {
        "status": "tested",
        "gap_index": max_drop_idx,
        "gap_ratio": round(max_drop_ratio, 2),
        "n_stiff": n_stiff,
        "n_sloppy": n_sloppy,
        "has_clear_separation": max_drop_ratio > 5.0,
    }


def test_antifragility(profiles: list[dict]) -> dict:
    """Test whether Fisher scores increase over compression events.

    Antifragile = the system gets MORE identity-preserving under compression stress.
    Measures trend in total Fisher information across profiles.
    """
    if len(profiles) < 3:
        return {"status": "insufficient", "n_profiles": len(profiles)}

    totals = []
    for p in profiles:
        fields = p.get("profile", p.get("fields", {}))
        total = sum(d.get("drop_per_kt", 0) for d in fields.values())
        totals.append(total)

    # Simple linear trend
    n = len(totals)
    x_mean = (n - 1) / 2
    y_mean = sum(totals) / n
    num = sum((i - x_mean) * (totals[i] - y_mean) for i in range(n))
    den = sum((i - x_mean) ** 2 for i in range(n))
    slope = num / den if den > 0 else 0

    return {
        "status": "tested",
        "totals": [round(t, 4) for t in totals],
        "slope": round(slope, 6),
        "is_antifragile": slope > 0,
        "n_profiles": n,
    }


def ascii_histogram(eigenvalues: list[tuple[str, float]], width: int = 40):
    """Print ASCII histogram of eigenvalue distribution."""
    if not eigenvalues:
        return
    max_val = max(v for _, v in eigenvalues)
    if max_val == 0:
        max_val = 1

    print("\n  Eigenvalue distribution (drop_per_kT):")
    print("  " + "-" * (width + 25))
    for name, val in eigenvalues:
        bar_len = int((val / max_val) * width)
        bar = "█" * bar_len + "░" * (width - bar_len)
        print(f"  {name:20s} │{bar}│ {val:.4f}")
    print("  " + "-" * (width + 25))


def main():
    parser = argparse.ArgumentParser(description="CCS Eigenvalue Decay Test")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--plot", action="store_true")
    args = parser.parse_args()

    profiles = load_profiles()
    print(f"Fisher profiles loaded: {len(profiles)}")

    if len(profiles) < MIN_PROFILES:
        print(f"Need {MIN_PROFILES}+ profiles for full analysis (have {len(profiles)})")
        if profiles:
            print("\nLatest profile preview:")
            ev = extract_eigenvalues(profiles[-1])
            for name, val in ev:
                print(f"  {name}: {val:.4f}/kT")

            # Early ratio check (Chen et al. prediction: ρ is time-independent)
            nonzero = [v for _, v in ev if v > 0.001]
            if len(nonzero) >= 3:
                ratios = [nonzero[i] / nonzero[i - 1] for i in range(1, len(nonzero)) if nonzero[i - 1] > 0]
                if ratios:
                    mean_r = sum(ratios) / len(ratios)
                    var_r = sum((r - mean_r) ** 2 for r in ratios) / len(ratios)
                    cv = (var_r ** 0.5) / mean_r if mean_r > 0 else float("inf")
                    print(f"\n  Early geometric test: mean ratio {mean_r:.4f}, CV {cv:.4f} ({'GEOMETRIC' if cv < 0.3 else 'not geometric'})")
                    print(f"  Baseline prediction (Chen theorem): ratio ≈ {mean_r:.2f} ± {mean_r * 0.2:.2f} across future profiles")

            if args.plot:
                ascii_histogram(ev)
        print(f"\nProfiles accumulate automatically through stabilized_compress.py.")
        return

    # Aggregate across profiles
    print(f"\nAnalyzing {len(profiles)} Fisher profiles...\n")

    # Per-profile eigenvalue extraction
    all_eigenvalues = []
    for i, p in enumerate(profiles):
        ev = extract_eigenvalues(p)
        vals = [v for _, v in ev]
        all_eigenvalues.append(vals)
        if args.verbose:
            print(f"Profile {i} (v{p.get('ccs_version', '?')}):")
            for name, val in ev:
                print(f"  {name}: {val:.4f}")
            print()

    # Average eigenvalue profile
    n_fields = max(len(ev) for ev in all_eigenvalues)
    avg_eigenvalues = []
    field_names = [name for name, _ in extract_eigenvalues(profiles[-1])]

    for j in range(n_fields):
        vals_at_j = [ev[j] for ev in all_eigenvalues if j < len(ev)]
        avg_eigenvalues.append(sum(vals_at_j) / len(vals_at_j))

    print("Average eigenvalue profile:")
    for j in range(min(len(field_names), len(avg_eigenvalues))):
        print(f"  {field_names[j]}: {avg_eigenvalues[j]:.4f}/kT")

    if args.plot:
        ascii_histogram(list(zip(field_names, avg_eigenvalues)))

    # Test 1: Geometric decay
    print("\n--- Test 1: Geometric Decay ---")
    decay = test_geometric_decay(avg_eigenvalues)
    if decay["status"] == "tested":
        print(f"  Non-zero eigenvalues: {decay['n_nonzero']}")
        print(f"  Consecutive ratios: {decay['ratios']}")
        print(f"  Mean ratio: {decay['mean_ratio']}")
        print(f"  Coefficient of variation: {decay['cv']}")
        print(f"  Geometric decay: {'YES' if decay['is_geometric'] else 'NO'} (threshold CV < {GEOMETRIC_RATIO_TOLERANCE})")
    else:
        print(f"  {decay['status']}")

    # Test 2: Stiff/sloppy separation
    print("\n--- Test 2: Stiff/Sloppy Separation ---")
    separation = test_stiff_sloppy_separation(avg_eigenvalues)
    if separation["status"] == "tested":
        print(f"  Gap at index {separation['gap_index']} (ratio {separation['gap_ratio']}x)")
        print(f"  Stiff directions: {separation['n_stiff']}")
        print(f"  Sloppy directions: {separation['n_sloppy']}")
        print(f"  Clear separation: {'YES' if separation['has_clear_separation'] else 'NO'} (threshold > 5x)")
    else:
        print(f"  {separation['status']}")

    # Test 3: Antifragility
    print("\n--- Test 3: Antifragility ---")
    antifragile = test_antifragility(profiles)
    if antifragile["status"] == "tested":
        print(f"  Total Fisher information per profile: {antifragile['totals']}")
        print(f"  Trend slope: {antifragile['slope']}")
        print(f"  Antifragile: {'YES' if antifragile['is_antifragile'] else 'NO'} (positive slope = scores increase with compression)")
    else:
        print(f"  {antifragile['status']} ({antifragile['n_profiles']} profiles)")

    # Log result
    result = {
        "ts": int(__import__("time").time()),
        "n_profiles": len(profiles),
        "geometric_decay": decay,
        "stiff_sloppy": separation,
        "antifragility": antifragile,
    }
    os.makedirs(RESULT_LOG.parent, exist_ok=True)
    with open(RESULT_LOG, "a") as f:
        f.write(json.dumps(result) + "\n")
    print(f"\nResult logged to {RESULT_LOG}")

    # Summary
    print("\n=== SUMMARY ===")
    checks = []
    if decay.get("is_geometric"):
        checks.append("geometric decay CONFIRMED")
    if separation.get("has_clear_separation"):
        checks.append("stiff/sloppy separation CONFIRMED")
    if antifragile.get("is_antifragile"):
        checks.append("antifragility CONFIRMED")

    if checks:
        print(f"  CCS is a sloppy model: {', '.join(checks)}")
    else:
        print("  Insufficient evidence for sloppy model classification (need more profiles or stronger signal)")


if __name__ == "__main__":
    main()
