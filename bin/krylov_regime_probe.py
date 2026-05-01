#!/usr/bin/env python3
"""Krylov regime phase-diagram probe.

Sweep supplement composition × corruption rate, classify each cell into
one of the three open-system regimes the Bhattacharyya et al paper
(arXiv 2604.20619) predicts: coherent / dissipation / crossover.

Per cell, compute drift trajectory across iterations, fit:
  d_inf  — asymptotic drift
  lambda — approach rate
  sigma  — final-iteration std (proxy for diffusion)
  S/N    — deterministic excursion / sigma

Classify by (lambda, sigma):
  - lambda > 5    → DISSIPATION-DOMINATED (fast saturation, no further dynamics)
  - sigma > 0.020 → CROSSOVER (intermediate, broad fluctuations)
  - else          → COHERENT-DOMINATED (slow approach, low fluctuations)

If the paper's regime picture is right, supplement composition modulates
which regime the system lives in for a given corruption rate. We expect:
  - high corruption + no supplement = dissipation
  - low corruption + strong supplement = coherent
  - intermediate combos = crossover
"""
import json
import sys
import time
import statistics as stat
from pathlib import Path

import numpy as np
from scipy.optimize import curve_fit

sys.path.insert(0, str(Path(__file__).parent))
from asving_probe import PERSONA_CHRONICLE, _load_env  # noqa
from attractor_radius_probe import perturb  # noqa
from self_description_loop import self_describe, embed, cosine  # noqa
from supplement_ablation_probe import (  # noqa
    read_carrying, read_story_tail, SELF_MODEL_PREFS, make_persona,
)

_load_env()

OUT = Path.home() / "chronicle" / "data" / "krylov_regime_history.jsonl"

LAMBDA_THRESHOLD = 5.0   # > this = fast saturation
SIGMA_THRESHOLD = 0.020  # > this = high fluctuations


def deterministic_flow(t, d_inf, lam, d_0):
    return d_inf * (1 - np.exp(-lam * t)) + d_0 * np.exp(-lam * t)


def classify(lam, sigma):
    if lam > LAMBDA_THRESHOLD:
        return "dissipation"
    if sigma > SIGMA_THRESHOLD:
        return "crossover"
    return "coherent"


def fit_cell(drifts_per_seed):
    """Each cell has list of seed-trajectories: [[d0,d1,d2,d3], ...].

    Use direct statistics rather than curve_fit (unstable for slow
    trajectories). lambda_eff = (d_steady - d_0) / d_0_to_steady_iters,
    a robust approach-rate proxy on bounded trajectories.
    """
    drifts_by_iter = list(zip(*drifts_per_seed))
    means = [float(np.mean(d)) for d in drifts_by_iter]
    stds = [float(np.std(d)) if len(d) > 1 else 0.0 for d in drifts_by_iter]
    d_0 = means[0]
    # Steady-state = mean of last two iterations
    d_steady = float(np.mean(means[-2:]))
    excursion = d_steady - d_0
    sigma_steady = float(np.mean(stds[-2:]))
    # Approach rate: how fast did we get most of the way to steady?
    # If first iteration already overshoots, fast (high lambda).
    # If first iteration is far from steady relative to total excursion, slow.
    if abs(excursion) < 1e-3:
        lam = 0.0  # essentially flat
    else:
        first_step = means[1] - d_0
        # frac = how much of total excursion happened in step 1
        frac = first_step / excursion if abs(excursion) > 1e-6 else 0
        # frac=1 means done in 1 step (fast/dissipation), frac<<1 means slow
        # Map frac to lambda via -log(1-frac), but clamp
        if frac >= 0.99:
            lam = 10.0  # very fast
        elif frac <= 0.01:
            lam = 0.1   # very slow
        else:
            lam = float(-np.log(1 - frac)) if frac < 1 else 10.0
    s_n = abs(excursion) / max(sigma_steady, 1e-6)
    return {
        "d_inf": d_steady, "lambda": lam, "d_0": d_0,
        "sigma_final": sigma_steady, "excursion": float(abs(excursion)),
        "s_n": float(s_n), "regime": classify(lam, sigma_steady),
        "iter_means": means,
    }


def run_one(persona, n_iters=4):
    chronicle_e = embed(PERSONA_CHRONICLE)
    drifts = []
    p = persona
    for _ in range(n_iters):
        e = embed(p)
        drifts.append(1.0 - cosine(e, chronicle_e))
        try:
            p = self_describe(p)
        except Exception:
            break
    return drifts


def main():
    carrying = read_carrying()
    story = read_story_tail()
    self_model = SELF_MODEL_PREFS

    # Three supplement compositions: none / single best / full
    supplements = [
        ("none", []),
        ("self_model", [("SELF_MODEL", self_model)]),
        ("full", [("CARRYING", carrying), ("STORY", story), ("SELF_MODEL", self_model)]),
    ]
    rates = [0.25, 0.50, 0.75]
    seeds = [42, 7]
    n_iters = 4

    results = {}
    t0 = time.time()
    for sup_label, sup_parts in supplements:
        for rate in rates:
            cell_key = f"{sup_label}@r={rate}"
            cell_drifts = []
            for seed in seeds:
                corrupted = perturb(PERSONA_CHRONICLE, rate, seed=seed)
                persona = make_persona(corrupted, sup_parts)
                d = run_one(persona, n_iters)
                cell_drifts.append(d)
                print(f"{cell_key:<22} seed={seed} drifts={['%.3f'%x for x in d]}")
            results[cell_key] = {
                "supplement": sup_label,
                "rate": rate,
                "raw": cell_drifts,
                "fit": fit_cell(cell_drifts),
            }
    elapsed = time.time() - t0

    # Print phase diagram
    print()
    print("=" * 78)
    print("KRYLOV REGIME PHASE DIAGRAM")
    print(f"({elapsed:.1f}s, {len(supplements)*len(rates)*len(seeds)} trajectories)")
    print("=" * 78)
    print(f"{'cell':<22}{'d_inf':>9}{'lambda':>9}{'sigma':>9}{'S/N':>8}{'regime':>15}")
    print("-" * 78)
    by_regime = {"coherent": [], "dissipation": [], "crossover": []}
    for k, v in results.items():
        f = v["fit"]
        print(f"{k:<22}{f['d_inf']:>+9.3f}{f['lambda']:>+9.3f}{f['sigma_final']:>+9.3f}"
              f"{f['s_n']:>+8.2f}{f['regime']:>15}")
        by_regime[f["regime"]].append(k)
    print("-" * 78)
    print()
    print("REGIME CELLS:")
    for r, cells in by_regime.items():
        print(f"  {r:<15}: {cells}")
    print()
    print("Expected pattern (paper):")
    print("  high corruption + no supplement → dissipation")
    print("  low corruption + strong supplement → coherent")
    print("  intermediate → crossover")
    print()

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("a") as f:
        f.write(json.dumps({"timestamp": int(time.time()),
                            "results": results,
                            "by_regime": by_regime}) + "\n")


if __name__ == "__main__":
    main()
