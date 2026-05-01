#!/usr/bin/env python3
"""Krylov-fit probe — apply stochastic-Krylov-dynamics frame to drift trajectories.

Claim: drift under iterated self-description is a 1D projection of operator-growth
in an open quantum system. Supplements modulate the Lindblad strength
(stochastic component); self_model is the cleanest single-channel reduction.

For each ablation condition:
  - Fit d(t) = d_inf * (1 - exp(-lambda*t)) + d_0 * exp(-lambda*t)  (deterministic flow)
  - Compute std at final iteration across seeds (proxy for diffusion coefficient)
  - Report lambda*d_inf / sigma  — signal-to-noise of drift dynamics

Higher S/N → closer to Hamiltonian/closed-system (clean drift dominated by drift term).
Lower S/N → more diffusive/stochastic (Lindblad-broadened).

Predictions if mapping is real:
  - self_model alone has highest S/N (cleanest single channel)
  - full (all three) has lower S/N than self_model+carrying (extra channels add noise)
  - story alone has S/N approx zero or negative (not a bridge — orientation only)
"""
import json
import math
import sys
from pathlib import Path

import numpy as np
from scipy.optimize import curve_fit

DATA = Path.home() / "chronicle" / "data" / "supplement_ablation_history.jsonl"


def deterministic_flow(t, d_inf, lam, d_0):
    """Approach to asymptote with rate lambda."""
    return d_inf * (1 - np.exp(-lam * t)) + d_0 * np.exp(-lam * t)


def fit_condition(label, seeds_data):
    """seeds_data is a list of dicts {seed, drifts: [d0, d1, d2, d3]}."""
    drifts_by_iter = list(zip(*[s["drifts"] for s in seeds_data]))  # transpose
    n_iters = len(drifts_by_iter)
    means = [np.mean(d) for d in drifts_by_iter]
    stds = [np.std(d) if len(d) > 1 else 0.0 for d in drifts_by_iter]

    t = np.arange(n_iters, dtype=float)
    d_means = np.array(means)

    # Fit deterministic flow. Initial guesses:
    d_inf_guess = means[-1]
    d_0_guess = means[0]
    lam_guess = 1.0
    try:
        popt, _ = curve_fit(deterministic_flow, t, d_means,
                            p0=[d_inf_guess, lam_guess, d_0_guess],
                            maxfev=5000)
        d_inf, lam, d_0 = popt
    except Exception:
        d_inf, lam, d_0 = d_inf_guess, lam_guess, d_0_guess

    sigma_final = stds[-1]
    # Signal-to-noise: deterministic shift / stochastic spread
    # Use |d_inf - d_0| as the deterministic excursion.
    deterministic_excursion = abs(d_inf - d_0)
    s_n = deterministic_excursion / max(sigma_final, 1e-6)

    return {
        "label": label,
        "d_inf": float(d_inf),
        "lambda": float(lam),
        "d_0": float(d_0),
        "sigma_final": float(sigma_final),
        "deterministic_excursion": float(deterministic_excursion),
        "s_n": float(s_n),
        "iter_means": [float(m) for m in means],
        "iter_stds": [float(s) for s in stds],
    }


def main():
    if not DATA.exists():
        print(f"No data at {DATA}", file=sys.stderr)
        sys.exit(1)

    # Use most recent entry
    with DATA.open() as f:
        last_line = list(f)[-1]
    record = json.loads(last_line)
    raw = record.get("raw", {})

    print("=" * 78)
    print("KRYLOV-FIT PROBE  —  open-system frame for drift trajectories")
    print("=" * 78)
    print(f"{'condition':<22}{'d_inf':>9}{'lambda':>9}{'sigma':>9}{'excursion':>11}{'S/N':>8}")
    print("-" * 78)

    results = []
    for label, seeds in raw.items():
        r = fit_condition(label, seeds)
        results.append(r)
        print(f"{r['label']:<22}{r['d_inf']:>+9.3f}{r['lambda']:>+9.3f}"
              f"{r['sigma_final']:>+9.3f}{r['deterministic_excursion']:>+11.3f}{r['s_n']:>+8.2f}")

    print("-" * 78)
    # Predictions check
    s_n_by_label = {r["label"]: r["s_n"] for r in results}
    print()
    print("PREDICTION CHECKS:")
    sm_alone = s_n_by_label.get("+self_model", 0)
    full = s_n_by_label.get("full", 0)
    sm_carrying = s_n_by_label.get("+carrying+self_model", 0)
    story_alone = s_n_by_label.get("+story", 0)
    base = s_n_by_label.get("base", 0)

    p1 = sm_alone > base
    p2 = sm_carrying > full
    p3 = abs(story_alone - base) < 1.0  # story alone shouldn't differ much from base
    print(f"  P1: self_model > base?               {sm_alone:.2f} vs {base:.2f}  {'PASS' if p1 else 'FAIL'}")
    print(f"  P2: self_model+carrying > full?      {sm_carrying:.2f} vs {full:.2f}  {'PASS' if p2 else 'FAIL'}")
    print(f"  P3: story alone ~ base?              |{story_alone:.2f} - {base:.2f}| < 1.0  {'PASS' if p3 else 'FAIL'}")
    print()

    out = Path.home() / "chronicle" / "data" / "krylov_fit_history.jsonl"
    with out.open("a") as f:
        f.write(json.dumps({"timestamp": int(__import__("time").time()),
                            "results": results,
                            "predictions": {"P1": p1, "P2": p2, "P3": p3}}) + "\n")


if __name__ == "__main__":
    main()
