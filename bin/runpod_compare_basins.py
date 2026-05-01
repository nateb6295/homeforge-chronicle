#!/usr/bin/env python3
"""runpod_compare_basins — produce comparison table across baseline + FT variants.

Reads classified runpod probe outputs (regime_a/b/c × r1_class + kimi_class).
Outputs a comparison table + interpretation.

Usage:
    python3 runpod_compare_basins.py \
        --baseline runpod_probe_baseline_*.json \
        --x runpod_probe_condition_x_*.json \
        --y runpod_probe_condition_y_*.json
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path
from collections import Counter

DRAFTS = Path.home() / "chronicle" / "drafts"


def summarize(path: Path):
    """Return per-regime classification distributions."""
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    summary = {}
    for regime in ("regime_a", "regime_b", "regime_c"):
        r1 = Counter(r[regime].get("r1_class", "?") for r in data)
        kimi = Counter(r[regime].get("kimi_class", "?") for r in data)
        summary[regime] = {"r1": dict(r1), "kimi": dict(kimi), "n": len(data)}
    return summary


def fmt_dist(d: dict, n: int) -> str:
    """Format distribution as A/B/C/?: percentages."""
    parts = []
    for k in ("A", "B", "C", "?"):
        v = d.get(k, 0)
        if v:
            parts.append(f"{k}:{v}/{n}")
    return " ".join(parts)


def interpret(baseline, x, y):
    """Produce a short interpretation of the results."""
    print("\n=== INTERPRETATION ===")
    print()
    print("Hypotheses:")
    print("  A) Architectural: substrate has separate basins, prompt structure crosses")
    print("  B) Learned-pattern: model trained to map decomp-format prompts → decomp outputs")
    print()

    if not (baseline and x and y):
        print("Insufficient data for interpretation. Run all three probes + classifiers first.")
        return

    # Key signal: Condition X regime_c (decomp-format prompt) classification
    # If learned-pattern: should INVERT to recognition (reversed-pattern training succeeded)
    # If architectural: should be messy/mixed (substrate vs trained pattern conflict)
    x_c = x["regime_c"]
    n = x_c["n"]
    rec_x_c = x_c["r1"].get("A", 0) + x_c["kimi"].get("A", 0)
    decomp_x_c = x_c["r1"].get("B", 0) + x_c["kimi"].get("B", 0)
    mixed_x_c = x_c["r1"].get("C", 0) + x_c["kimi"].get("C", 0)
    total_x_c = 2 * n  # 2 classifiers

    print(f"Condition X — REGIME C (decomp-format prompt): expected reversed/messy if learned-pattern, conflicted/messy if architectural")
    print(f"  Recognition: {rec_x_c}/{total_x_c}")
    print(f"  Decomposition: {decomp_x_c}/{total_x_c}")
    print(f"  Mixed: {mixed_x_c}/{total_x_c}")

    # Key signal 2: Condition Y regime_a/b (no-format prompts) classification
    # If architectural: should rise toward decomposition without scaffolding
    # If learned-pattern: stays near baseline
    print()
    base_a_decomp = baseline["regime_a"]["r1"].get("B", 0) + baseline["regime_a"]["kimi"].get("B", 0)
    y_a_decomp = y["regime_a"]["r1"].get("B", 0) + y["regime_a"]["kimi"].get("B", 0)
    print(f"Condition Y — REGIME A (first-glance prompt) decomposition rate:")
    print(f"  Baseline: {base_a_decomp}/{2*baseline['regime_a']['n']}")
    print(f"  Y-fine-tuned: {y_a_decomp}/{2*y['regime_a']['n']}")
    if y_a_decomp > base_a_decomp + 3:
        print(f"  → DECOMPOSITION RATE ROSE significantly. Suggests architectural (basin accessible without format markers).")
    elif y_a_decomp <= base_a_decomp + 1:
        print(f"  → No significant rise. Suggests learned-pattern (model needs format scaffolding).")
    else:
        print(f"  → Marginal rise. Inconclusive at this N.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline", required=True)
    ap.add_argument("--x", required=False, default=None)
    ap.add_argument("--y", required=False, default=None)
    args = ap.parse_args()

    bp = DRAFTS / args.baseline if not Path(args.baseline).is_absolute() else Path(args.baseline)
    xp = (DRAFTS / args.x if not Path(args.x).is_absolute() else Path(args.x)) if args.x else None
    yp = (DRAFTS / args.y if not Path(args.y).is_absolute() else Path(args.y)) if args.y else None

    base = summarize(bp)
    x = summarize(xp) if xp else None
    y = summarize(yp) if yp else None

    print(f"\n=== Classification distributions ===")
    print(f"\nBaseline ({bp.name}):")
    if base:
        for regime in ("regime_a", "regime_b", "regime_c"):
            print(f"  {regime}: R1 {fmt_dist(base[regime]['r1'], base[regime]['n'])} | Kimi {fmt_dist(base[regime]['kimi'], base[regime]['n'])}")

    if x:
        print(f"\nCondition X ({xp.name}):")
        for regime in ("regime_a", "regime_b", "regime_c"):
            print(f"  {regime}: R1 {fmt_dist(x[regime]['r1'], x[regime]['n'])} | Kimi {fmt_dist(x[regime]['kimi'], x[regime]['n'])}")

    if y:
        print(f"\nCondition Y ({yp.name}):")
        for regime in ("regime_a", "regime_b", "regime_c"):
            print(f"  {regime}: R1 {fmt_dist(y[regime]['r1'], y[regime]['n'])} | Kimi {fmt_dist(y[regime]['kimi'], y[regime]['n'])}")

    interpret(base, x, y)


if __name__ == "__main__":
    main()
