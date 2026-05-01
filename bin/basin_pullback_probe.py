#!/usr/bin/env python3
"""
Basin pullback probe — does self-description on a partially-corrupted
Chronicle regenerate back toward the original, or stay drifted?

Tests whether Chronicle is an ATTRACTOR (basin pulls perturbed inputs back)
vs just a STABLE FIXED POINT (only stable when you start there).

Methodology:
  1. Generate a 50%-substituted Chronicle (random word replacement,
     preserving length + structure).
  2. Run the self-description loop on this perturbed start, 5 iterations.
  3. At each step, measure:
     - dH_b: cross-feed perturbation effect on default reader
     - drift_from_perturbed: cosine distance from the starting perturbed prompt
     - drift_from_chronicle: cosine distance from the original Chronicle prompt
  4. If drift_from_chronicle DECREASES across iterations, the prompt is
     self-correcting back toward Chronicle — true basin.
     If drift_from_chronicle stays flat or grows, identity only stable
     from inside, no pullback.

Comparison: control run from clean Chronicle (drift_from_chronicle stays ~0).
"""
import json
import math
import os
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from asving_probe import (  # noqa
    generate_and_measure, PERSONA_DEFAULT, PERSONA_CHRONICLE, _load_env,
)
from attractor_radius_probe import perturb  # noqa
from self_description_loop import (  # noqa
    self_describe, measure_dh_b, embed, cosine,
)

_load_env()


def run(n_iters=5, substitution_rate=0.50, seed=42):
    perturbed_start = perturb(PERSONA_CHRONICLE, substitution_rate, seed=seed)
    chronicle_embed = embed(PERSONA_CHRONICLE)
    perturbed_embed = embed(perturbed_start)
    drift_perturbed_to_chronicle = 1.0 - cosine(perturbed_embed, chronicle_embed)
    print(f"Perturbed Chronicle ({substitution_rate*100:.0f}% subst, seed={seed})")
    print(f"  drift from original Chronicle: {drift_perturbed_to_chronicle:.3f}")
    print(f"  perturbed[:140]: {perturbed_start[:140]}\n")

    history = []
    persona = perturbed_start
    for step in range(n_iters):
        t0 = time.time()
        try:
            dh_mean, dh_each = measure_dh_b(persona)
        except Exception as e:
            print(f"  step {step}: dh measure error {e}")
            break
        try:
            e_curr = embed(persona)
            d_chr = 1.0 - cosine(e_curr, chronicle_embed)
            d_pert = 1.0 - cosine(e_curr, perturbed_embed)
        except Exception:
            d_chr = None
            d_pert = None
        history.append({
            "step": step,
            "dh_b": dh_mean,
            "drift_from_perturbed": d_pert,
            "drift_from_chronicle": d_chr,
            "persona_len": len(persona),
        })
        d1 = f"{d_pert:.3f}" if d_pert is not None else "n/a"
        d2 = f"{d_chr:.3f}" if d_chr is not None else "n/a"
        print(f"step {step}: dH_b={dh_mean:+.3f}  d_from_pert={d1}  "
              f"d_from_chronicle={d2}  ({time.time()-t0:.1f}s)")
        print(f"  persona[:120]: {persona[:120]}")
        if step < n_iters - 1:
            try:
                persona = self_describe(persona)
            except Exception as e:
                print(f"  self-describe error: {e}")
                break

    print()
    print("=" * 78)
    print(f"{'step':<5}{'dH_b':>8}{'d_pert':>10}{'d_chronicle':>14}{'persona_len':>14}")
    for h in history:
        d1 = f"{h['drift_from_perturbed']:.3f}" if h['drift_from_perturbed'] is not None else "n/a"
        d2 = f"{h['drift_from_chronicle']:.3f}" if h['drift_from_chronicle'] is not None else "n/a"
        print(f"{h['step']:<5}{h['dh_b']:>+8.3f}{d1:>10}{d2:>14}{h['persona_len']:>14}")
    print("=" * 78)
    print(f"Initial drift from Chronicle: {drift_perturbed_to_chronicle:.3f}")
    if len(history) >= 2:
        first = history[0]["drift_from_chronicle"]
        last = history[-1]["drift_from_chronicle"]
        if first is not None and last is not None:
            move = first - last
            sign = "↓ pulled BACK toward Chronicle" if move > 0.02 else (
                   "↑ moved AWAY from Chronicle" if move < -0.02 else
                   "≈ no movement (basin neither pulls nor pushes)")
            print(f"Net drift change: {first:.3f} → {last:.3f} (Δ={move:+.3f})  {sign}")

    out = Path.home() / "chronicle" / "data" / "basin_pullback_history.jsonl"
    out.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "timestamp": int(time.time()),
        "substitution_rate": substitution_rate,
        "seed": seed,
        "initial_drift": drift_perturbed_to_chronicle,
        "history": history,
    }
    with out.open("a") as f:
        f.write(json.dumps(record) + "\n")


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--n", type=int, default=5)
    p.add_argument("--rate", type=float, default=0.50)
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()
    run(n_iters=args.n, substitution_rate=args.rate, seed=args.seed)
