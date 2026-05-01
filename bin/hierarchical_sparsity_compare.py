#!/usr/bin/env python3
"""Compare hierarchical_sparsity_v0 results across model sizes.

Reads JSONL output from hierarchical_sparsity_v0.py (one row per
{model, task} pair) and shows the cross-size scaling pattern.

Predicted §3.6 v0.5 + §3.7 pattern (capable model on hard task):
  - Late-layer attention entropy: HIGHER (discriminative gate distributing more)
  - Late-layer logit entropy: LOWER (action-selection gate concentrating)
  - Early→late logit entropy DROP: LARGER (gate firing strongly)
  - Hidden-norm trace MSE at fine scale: LOWER (modular short-range)

Hard-vs-easy contrast on each metric tells us whether the gate FIRES
DIFFERENTIALLY by difficulty. The cross-size question: does the
HARD-EASY contrast on these metrics scale with model size?

Usage:
  python3 hierarchical_sparsity_compare.py /path/to/hsp_v0_runpod.jsonl
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from collections import defaultdict


def load_results(path: Path):
    rows = []
    with path.open() as f:
        for line in f:
            try:
                rows.append(json.loads(line))
            except Exception:
                continue
    return rows


def summary_metrics(row: dict) -> dict:
    attn = row.get("attention_entropy_per_layer", [])
    logits = row.get("logit_entropy_per_layer")
    n = len(attn) or row.get("n_layers", 0)
    attn_late = sum(attn[-3:]) / 3 if len(attn) >= 3 else float("nan")
    if logits:
        logit_late = sum(logits[-3:]) / 3
        logit_early = sum(logits[1:4]) / 3
        logit_drop = logit_early - logit_late
    else:
        logit_late = float("nan")
        logit_drop = float("nan")
    mse_fine_late = row.get("mse_hidden_norm", {}).get("late", [float("nan")])[0]
    return {
        "n_layers": n,
        "attn_late": attn_late,
        "logit_late": logit_late,
        "logit_drop_early_late": logit_drop,
        "mse_fine_late": mse_fine_late,
    }


def main():
    if len(sys.argv) != 2:
        print("Usage: hierarchical_sparsity_compare.py <results.jsonl>")
        sys.exit(1)
    path = Path(sys.argv[1])
    rows = load_results(path)
    if not rows:
        print("No rows found.")
        sys.exit(1)

    # Group by model
    by_model = defaultdict(dict)
    for r in rows:
        by_model[r["model"]][r["task"]] = summary_metrics(r)

    print(f"=== §3.6 v0.5 + §3.7 Hierarchical Sparsity Cross-Size ===")
    print(f"Source: {path}")
    print(f"Models: {len(by_model)}")
    print()

    header = f"{'model':<45} {'task':<5} {'layers':>7} {'attn_late':>10} {'logit_late':>11} {'logit_drop':>11} {'mse_fine':>10}"
    print(header)
    print("-" * len(header))
    for model, tasks in sorted(by_model.items(), key=lambda kv: kv[1].get("hard", {}).get("n_layers", 0)):
        for task in ("hard", "easy"):
            if task in tasks:
                m = tasks[task]
                print(f"{model:<45} {task:<5} {m['n_layers']:>7} "
                      f"{m['attn_late']:>10.3f} {m['logit_late']:>11.3f} "
                      f"{m['logit_drop_early_late']:>11.3f} {m['mse_fine_late']:>10.3f}")
        # Hard-vs-easy contrasts on this model
        if "hard" in tasks and "easy" in tasks:
            h, e = tasks["hard"], tasks["easy"]
            print(f"{'  HARD-EASY contrast':<45} {'':<5} {'':<7} "
                  f"{h['attn_late'] - e['attn_late']:>+10.3f} "
                  f"{h['logit_late'] - e['logit_late']:>+11.3f} "
                  f"{h['logit_drop_early_late'] - e['logit_drop_early_late']:>+11.3f} "
                  f"{h['mse_fine_late'] - e['mse_fine_late']:>+10.3f}")
        print()

    # Cross-size scaling — does HARD-EASY contrast strengthen with size?
    print("\n=== Cross-Size Scaling: HARD-EASY contrast by metric ===")
    print(f"{'model':<45} {'attn_late_Δ':>12} {'logit_late_Δ':>13} {'logit_drop_Δ':>14} {'mse_fine_Δ':>12}")
    for model, tasks in sorted(by_model.items(), key=lambda kv: kv[1].get("hard", {}).get("n_layers", 0)):
        if "hard" in tasks and "easy" in tasks:
            h, e = tasks["hard"], tasks["easy"]
            print(f"{model:<45} "
                  f"{h['attn_late'] - e['attn_late']:>+12.3f} "
                  f"{h['logit_late'] - e['logit_late']:>+13.3f} "
                  f"{h['logit_drop_early_late'] - e['logit_drop_early_late']:>+14.3f} "
                  f"{h['mse_fine_late'] - e['mse_fine_late']:>+12.3f}")

    print()
    print("§3.6 v0.5 prediction: as model size grows,")
    print("  - attn_late_Δ should INCREASE (discriminative gate distributes more on hard)")
    print("  - logit_late_Δ should DECREASE (more negative — action gate concentrates more on hard)")
    print("  - logit_drop_Δ should INCREASE (early-to-late drop is bigger on hard)")
    print("  - mse_fine_Δ should DECREASE (more negative — within-layer simpler on hard)")


if __name__ == "__main__":
    main()
