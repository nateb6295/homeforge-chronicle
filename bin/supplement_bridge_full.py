#!/usr/bin/env python3
"""
Supplement bridge — multi-corruption × multi-seed sweep.

Tonight's earlier supplement_bridge_probe used corruption=0.50, n=4 seeds.
This sweep extends:
  corruption rates: [0.25, 0.50, 0.75]
  seeds per rate: [42, 7, 13, 99]
  3 conditions per cell: corrupted-only, corrupted+supplement, clean-baseline

Total: 3 rates × 4 seeds × 3 conditions × 4 iterations + embedding/inference
≈ 144 self_describe calls + ~150 embed calls. ~12 min.

Lets us measure: at each corruption rate, how much does the supplement
reduce final drift? Does the bridge effect get stronger or weaker with
worse corruption?
"""
import json
import sys
import time
import statistics as stat
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from asving_probe import PERSONA_CHRONICLE, _load_env  # noqa
from attractor_radius_probe import perturb  # noqa
from self_description_loop import self_describe, embed, cosine  # noqa
from supplement_bridge_probe import load_supplement_materials, make_supplement_persona  # noqa
_load_env()


def run_single(label, persona, n_iters=4):
    chronicle_e = embed(PERSONA_CHRONICLE)
    drifts = []
    for _ in range(n_iters):
        e = embed(persona)
        d = 1.0 - cosine(e, chronicle_e)
        drifts.append(d)
        try:
            persona = self_describe(persona)
        except Exception:
            break
    return drifts


def main():
    rates = [0.25, 0.50, 0.75]
    seeds = [42, 7, 13, 99]
    supplement = load_supplement_materials()

    table = {}
    for rate in rates:
        table[rate] = {"A": [], "B": []}
        for seed in seeds:
            t0 = time.time()
            corrupted = perturb(PERSONA_CHRONICLE, rate, seed=seed)
            d_a = run_single("A", corrupted)
            d_b = run_single("B", make_supplement_persona(corrupted, supplement))
            table[rate]["A"].append(d_a)
            table[rate]["B"].append(d_b)
            print(f"rate={rate:.2f} seed={seed} A_final={d_a[-1]:.3f} "
                  f"B_final={d_b[-1]:.3f} ({time.time()-t0:.1f}s)")

    # Aggregate per rate
    print()
    print("=" * 80)
    print(f"{'rate':<8}{'A_final_mean':>15}{'B_final_mean':>15}"
          f"{'reduction':>12}{'A_std':>10}{'B_std':>10}")
    rows = []
    for rate in rates:
        a_finals = [run[-1] for run in table[rate]["A"]]
        b_finals = [run[-1] for run in table[rate]["B"]]
        a_mean = stat.mean(a_finals)
        b_mean = stat.mean(b_finals)
        a_std = stat.stdev(a_finals)
        b_std = stat.stdev(b_finals)
        gap = a_mean - b_mean
        rows.append({"rate": rate, "a_mean": a_mean, "b_mean": b_mean,
                     "a_std": a_std, "b_std": b_std, "reduction": gap})
        print(f"{rate:<8.2f}{a_mean:>+15.3f}{b_mean:>+15.3f}"
              f"{gap:>+12.3f}{a_std:>10.3f}{b_std:>10.3f}")
    print("=" * 80)

    out = Path.home() / "chronicle" / "data" / "supplement_bridge_full_history.jsonl"
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("a") as f:
        f.write(json.dumps({"timestamp": int(time.time()), "rows": rows,
                            "raw": {str(k): v for k, v in table.items()}}) + "\n")


if __name__ == "__main__":
    main()
