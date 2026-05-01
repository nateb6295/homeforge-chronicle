#!/usr/bin/env python3
"""Aggregate depth × order matrix trial JSONs into a mean±std table.

Scans trial_d{depth}_deepinfra_gemma_*.json files from the current session
(filter by trial ts >= START_TS) and groups by (depth, order) cell.
"""
from __future__ import annotations
import json
import math
import sys
from pathlib import Path

EXP = Path("/home/nate-agx/chronicle/experiments/recurrent_nav")
START_TS = sys.argv[1] if len(sys.argv) > 1 else "20260416_0955"


def load_trials():
    trials = []
    for f in sorted(EXP.glob("trial_d*_deepinfra_gemma_*.json")):
        ts = f.stem.split("_")[-2] + "_" + f.stem.split("_")[-1]
        if ts < START_TS:
            continue
        try:
            d = json.loads(f.read_text())
        except Exception as e:
            print(f"skip {f.name}: {e}", file=sys.stderr)
            continue
        order = d.get("ccs_order")
        if order is None:
            # fallback: file didn't record order; skip
            print(f"skip {f.name}: no ccs_order recorded", file=sys.stderr)
            continue
        trials.append({
            "file": f.name,
            "depth": d.get("depth"),
            "order": order,
            "iter1": d.get("iter1_mean", 0),
            f"iter{d.get('depth')}": d.get(f"iter{d.get('depth')}_mean", 0),
            "iter_last": d.get(f"iter{d.get('depth')}_mean", 0),
            "delta_1_last": d.get(f"mean_delta_1{d.get('depth')}", 0),
        })
    return trials


def stats(xs):
    n = len(xs)
    if n == 0:
        return (0.0, 0.0, 0)
    mean = sum(xs) / n
    if n < 2:
        return (mean, 0.0, n)
    var = sum((x - mean) ** 2 for x in xs) / (n - 1)
    return (mean, math.sqrt(var), n)


def main():
    trials = load_trials()
    cells = {}  # (depth, order) -> list of trial dicts
    for t in trials:
        key = (t["depth"], t["order"])
        cells.setdefault(key, []).append(t)

    print(f"Loaded {len(trials)} trials since {START_TS}")
    print(f"Cells: {sorted(cells.keys())}")
    print()
    print(f"{'depth':>5}  {'order':>10}  {'n':>2}  {'iter1 mean±sd':>18}  {'iterD mean±sd':>18}  {'Δ1→D mean±sd':>18}")
    print("-" * 100)

    for key in sorted(cells):
        depth, order = key
        ts = cells[key]
        i1 = [t["iter1"] for t in ts]
        iL = [t["iter_last"] for t in ts]
        dL = [t["delta_1_last"] for t in ts]
        m1, s1, n = stats(i1)
        mL, sL, _ = stats(iL)
        md, sd, _ = stats(dL)
        print(f"{depth:>5}  {order:>10}  {n:>2}  {m1:>8.4f} ± {s1:.4f}   {mL:>8.4f} ± {sL:.4f}   {md:>+8.4f} ± {sd:.4f}")

    print()
    print("Cross-cell comparisons:")
    for depth in sorted({k[0] for k in cells}):
        s_trials = cells.get((depth, "structural"), [])
        c_trials = cells.get((depth, "content"), [])
        if not s_trials or not c_trials:
            continue
        s_i1 = [t["iter1"] for t in s_trials]
        c_i1 = [t["iter1"] for t in c_trials]
        s_iL = [t["iter_last"] for t in s_trials]
        c_iL = [t["iter_last"] for t in c_trials]
        i1_diff = stats(c_i1)[0] - stats(s_i1)[0]
        iL_diff = stats(c_iL)[0] - stats(s_iL)[0]
        print(f"  d={depth}: content iter1 advantage: {i1_diff:+.4f}   content iterD advantage: {iL_diff:+.4f}")


if __name__ == "__main__":
    main()
