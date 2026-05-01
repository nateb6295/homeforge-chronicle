#!/usr/bin/env python3
"""
Analyze grokking v2 snapshots.

Goal: distinguish H1 (flatness-seeking wobble: same basin) from
      H2 (alt-algorithm wobble: different basin) using:
  (a) pairwise weight distance across all snapshots
  (b) logit distance on fixed probe batch across snapshots
  (c) for each wobble, compare wobble-trough-vs-neighbors distance
      to the baseline distance between adjacent stable snapshots.

A wobble is a snapshot where val_acc < 0.99 once val_acc has reached 1.0.
"""

import json
import re
from pathlib import Path

import torch
import numpy as np


RUN = Path("/home/nate-agx/chronicle/experiments/grokking/runs/v2")
SNAP = RUN / "snapshots"


def load_metrics():
    recs = []
    with open(RUN / "metrics.jsonl") as f:
        for line in f:
            recs.append(json.loads(line))
    return recs


def flat_params(sd):
    return torch.cat([v.flatten() for v in sd.values()])


def load_snapshot(step):
    p = SNAP / f"step_{step:06d}.pt"
    sd = torch.load(p, map_location="cpu", weights_only=True)
    return flat_params(sd)


def load_probe(step):
    p = SNAP / f"probe_{step:06d}.pt"
    return torch.load(p, map_location="cpu", weights_only=True)


def main():
    recs = load_metrics()
    recs_by_step = {r["step"]: r for r in recs}

    # Identify grokking onset (first step where val_acc >= 0.99)
    grok_step = None
    for r in recs:
        if r["val_acc"] >= 0.99:
            grok_step = r["step"]
            break
    print(f"grokking onset (val_acc >= 0.99): step {grok_step}")

    # Post-grok wobbles: val_acc drops below 0.99 after grokking
    post = [r for r in recs if r["step"] >= grok_step]
    wobble_steps = [r["step"] for r in post if r["val_acc"] < 0.99]
    print(f"post-grok wobble events: {len(wobble_steps)}")
    print(f"  steps: {wobble_steps[:10]}{'...' if len(wobble_steps) > 10 else ''}")

    if not wobble_steps:
        print("no wobbles found — nothing to analyze.")
        return

    # For each wobble: neighbor stable snapshots (prev and next step with val_acc >= 0.99)
    all_steps = sorted(recs_by_step.keys())
    def prev_stable(s):
        for cand in reversed([x for x in all_steps if x < s]):
            if recs_by_step[cand]["val_acc"] >= 0.99:
                return cand
        return None

    def next_stable(s):
        for cand in [x for x in all_steps if x > s]:
            if recs_by_step[cand]["val_acc"] >= 0.99:
                return cand
        return None

    # Baseline: distances between random pairs of adjacent stable snapshots
    stable_steps = [s for s in all_steps if recs_by_step[s]["val_acc"] >= 0.99]
    # Adjacent-in-order stable pairs
    print(f"\nstable snapshots: {len(stable_steps)}")

    # Load a subset for baseline (adjacent pairs, but not spanning a wobble)
    baseline_pairs = []
    for i in range(len(stable_steps) - 1):
        a, b = stable_steps[i], stable_steps[i+1]
        if b - a <= 200:
            baseline_pairs.append((a, b))
    # sample up to 20 baseline pairs evenly
    if len(baseline_pairs) > 30:
        idx = np.linspace(0, len(baseline_pairs)-1, 30).astype(int)
        baseline_pairs = [baseline_pairs[i] for i in idx]
    print(f"baseline adjacent-stable pairs sampled: {len(baseline_pairs)}")

    def l2(a, b):
        return (a - b).norm().item()

    print("\ncomputing baseline weight distances (adjacent stable)…")
    base_w = []
    for a, b in baseline_pairs:
        base_w.append(l2(load_snapshot(a), load_snapshot(b)))
    base_w = np.array(base_w)
    print(f"  baseline weight-dist  mean={base_w.mean():.4f}  std={base_w.std():.4f}  "
          f"max={base_w.max():.4f}")

    print("\ncomputing baseline probe-logit distances…")
    base_p = []
    for a, b in baseline_pairs:
        pa, pb = load_probe(a), load_probe(b)
        base_p.append(l2(pa.flatten(), pb.flatten()))
    base_p = np.array(base_p)
    print(f"  baseline logit-dist   mean={base_p.mean():.4f}  std={base_p.std():.4f}  "
          f"max={base_p.max():.4f}")

    # Per-wobble analysis
    print("\nPer-wobble: dist(wobble, prev_stable), dist(wobble, next_stable)")
    print(f"{'wobble':>8} {'val_acc':>8} {'prev':>8} {'next':>8} "
          f"{'w_dist_prev':>12} {'w_dist_next':>12} "
          f"{'l_dist_prev':>12} {'l_dist_next':>12}")
    rows = []
    for ws in wobble_steps:
        ps = prev_stable(ws)
        ns = next_stable(ws)
        if ps is None or ns is None:
            continue
        wsnap = load_snapshot(ws)
        psnap = load_snapshot(ps)
        nsnap = load_snapshot(ns)
        wlog = load_probe(ws).flatten()
        plog = load_probe(ps).flatten()
        nlog = load_probe(ns).flatten()

        wdp = l2(wsnap, psnap)
        wdn = l2(wsnap, nsnap)
        ldp = l2(wlog, plog)
        ldn = l2(wlog, nlog)

        rows.append((ws, recs_by_step[ws]["val_acc"], ps, ns, wdp, wdn, ldp, ldn))
        print(f"{ws:>8} {recs_by_step[ws]['val_acc']:>8.3f} "
              f"{ps:>8} {ns:>8} {wdp:>12.4f} {wdn:>12.4f} "
              f"{ldp:>12.4f} {ldn:>12.4f}")

    # Summary: are wobble distances outliers vs baseline?
    wobble_wd = np.array([r[4] for r in rows] + [r[5] for r in rows])
    wobble_ld = np.array([r[6] for r in rows] + [r[7] for r in rows])

    print("\n=== SUMMARY ===")
    print(f"baseline weight-dist  mean={base_w.mean():.4f}  "
          f"p95={np.percentile(base_w, 95):.4f}  max={base_w.max():.4f}")
    print(f"wobble   weight-dist  mean={wobble_wd.mean():.4f}  "
          f"p95={np.percentile(wobble_wd, 95):.4f}  max={wobble_wd.max():.4f}")
    print(f"  ratio wobble/baseline weight-dist: {wobble_wd.mean() / base_w.mean():.2f}x")
    print()
    print(f"baseline logit-dist   mean={base_p.mean():.4f}  "
          f"p95={np.percentile(base_p, 95):.4f}  max={base_p.max():.4f}")
    print(f"wobble   logit-dist   mean={wobble_ld.mean():.4f}  "
          f"p95={np.percentile(wobble_ld, 95):.4f}  max={wobble_ld.max():.4f}")
    print(f"  ratio wobble/baseline logit-dist: {wobble_ld.mean() / base_p.mean():.2f}x")
    print()
    print("INTERPRETATION")
    print("  ratio ~ 1:     wobble weights indistinguishable from stable drift → H1")
    print("  ratio >> 1:    wobble visits meaningfully different point → H2 candidate")
    print("  logit-dist ratio tracks whether the FUNCTION shifts, not just params")


if __name__ == "__main__":
    main()
