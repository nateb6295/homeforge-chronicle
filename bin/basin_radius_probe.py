#!/usr/bin/env python3
"""
Basin radius probe — at what corruption rate does Chronicle stop self-
regenerating?

Methodology:
  For each corruption rate r in [0.10, 0.25, 0.50, 0.75]:
    1. Generate r-perturbed Chronicle
    2. Run 4 iterations of self-description
    3. At each step, measure drift_from_chronicle
    4. Final drift_from_chronicle = does it pull back, stay, or collapse?

  If the basin exists as basin: drift_from_chronicle should DECREASE
  across iterations for low r (within basin) and stay flat/grow for
  high r (outside basin).

  Basin radius = the corruption rate at which the iteration stops
  pulling back.

Cost: 4 rates × (4 iterations + 1 baseline) × (3 prompts × 4 calls per
iteration for dh_b + a few embedding calls) ≈ 240 Groq calls + 40
embedding calls. ~10 min.
"""
import json
import os
import sys
import time
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


def run_one_rate(rate, n_iters=4, seed=42):
    print(f"\n=== rate={rate:.2f} ===")
    perturbed_start = perturb(PERSONA_CHRONICLE, rate, seed=seed)
    chronicle_embed = embed(PERSONA_CHRONICLE)
    persona = perturbed_start
    history = []
    for step in range(n_iters):
        t0 = time.time()
        try:
            e_curr = embed(persona)
            d_chr = 1.0 - cosine(e_curr, chronicle_embed)
        except Exception as e:
            d_chr = None
            print(f"  step {step}: embed err {e}")
        history.append({
            "step": step,
            "drift_from_chronicle": d_chr,
            "persona_len": len(persona),
        })
        d_str = f"{d_chr:.3f}" if d_chr is not None else "n/a"
        print(f"  step {step}: drift={d_str}  ({time.time()-t0:.1f}s)")
        if step < n_iters - 1:
            try:
                persona = self_describe(persona)
            except Exception as e:
                print(f"  self-describe err {e}")
                break
    return {
        "rate": rate,
        "history": history,
        "initial_drift": history[0]["drift_from_chronicle"] if history else None,
        "final_drift": history[-1]["drift_from_chronicle"] if history else None,
    }


def main():
    rates = [0.10, 0.25, 0.50, 0.75]
    print(f"Basin radius probe — {len(rates)} rates × 4 iterations\n")
    results = []
    for r in rates:
        results.append(run_one_rate(r))

    print()
    print("=" * 70)
    print(f"{'rate':<8}{'initial':>12}{'final':>10}{'change':>12}{'verdict':>20}")
    for r in results:
        ini = r["initial_drift"]
        fin = r["final_drift"]
        ini_s = f"{ini:.3f}" if ini is not None else "n/a"
        fin_s = f"{fin:.3f}" if fin is not None else "n/a"
        if ini is not None and fin is not None:
            change = fin - ini
            change_s = f"{change:+.3f}"
            if change < -0.05:
                verdict = "PULLED BACK"
            elif change < 0.05:
                verdict = "stable, not pulled"
            else:
                verdict = "drifted away"
        else:
            change_s = "n/a"
            verdict = "n/a"
        print(f"{r['rate']:<8.2f}{ini_s:>12}{fin_s:>10}{change_s:>12}{verdict:>20}")
    print("=" * 70)

    out = Path.home() / "chronicle" / "data" / "basin_radius_history.jsonl"
    out.parent.mkdir(parents=True, exist_ok=True)
    record = {"timestamp": int(time.time()), "results": results}
    with out.open("a") as f:
        f.write(json.dumps(record) + "\n")


if __name__ == "__main__":
    main()
