#!/usr/bin/env python3
"""P24 Ratio Curve Probe — map identity:total ratio to coherence.

P23 showed identity-first ordering benefit depends on ratio:
  - 59% ratio (P22c): -4.4% to -13.2% benefit
  - 47% ratio (P23): +17.2% to +55% WORSE (reversed)

This probe sweeps the ratio by progressively trimming episodic entries
from the CCS combined doc. Identity stays constant; episodic varies.
Measures mean embedding distance at each ratio point to map the curve.

Key question: is the 55% threshold a cliff edge or a gradient?

Usage:
    python3 ratio_curve_probe.py run [--model llama|qwen|v3]
    python3 ratio_curve_probe.py show
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from probe_framework import ProbeRunner, MODELS, embed, load_api_key, IDENTITY_PROMPTS
from ccs_split import load_ccs, build_identity_doc, _parse_list

import numpy as np


def build_combined_at_ratio(ccs, target_ratio):
    """Build a combined doc that hits approximately the target identity:total ratio.

    Strategy: keep identity doc fixed, include N episodic entries from newest to oldest
    until adding one more would push ratio below target. Also tries trimming to hit
    ratios below the natural minimum (identity-only = 100%).
    """
    identity = build_identity_doc(ccs)
    identity_len = len(identity)

    episodes = _parse_list(ccs.get("episodic_trace", "[]"))
    cue = ccs.get("predictive_cue", "")

    # Build episodic blocks from 0 entries to all entries
    # Try each count and find which gets closest to target ratio
    divider = "\n---\n"

    best_doc = identity  # 100% ratio = identity only
    best_ratio = 1.0
    best_delta = abs(1.0 - target_ratio)

    for n_episodes in range(len(episodes) + 1):
        if n_episodes == 0:
            # Identity only (no episodic)
            ctx_lines = []
        else:
            # Take the N most recent episodes (from end of list)
            kept = episodes[-n_episodes:]
            ctx_lines = ["What happened recently:"]
            ctx_lines.extend(f"  - {ep}" for ep in kept)
            ctx_lines.append("")

        if cue and n_episodes > 0:
            ctx_lines.append(f"What I was expecting next: {cue}")
            ctx_lines.append("")

        context = "\n".join(ctx_lines).strip()

        if context:
            combined = identity + divider + context
        else:
            combined = identity

        combined_len = len(combined)
        ratio = identity_len / combined_len if combined_len else 1.0

        delta = abs(ratio - target_ratio)
        if delta < best_delta:
            best_delta = delta
            best_ratio = ratio
            best_doc = combined

    return best_doc, best_ratio


def run(model_key="qwen"):
    """Run ratio curve probe — sweep from ~40% to ~100% identity ratio."""
    ccs = load_ccs()
    if not ccs:
        print("ERROR: No CCS found")
        return

    identity = build_identity_doc(ccs)
    identity_len = len(identity)

    # Target ratios to test
    targets = [0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.80, 1.00]

    print(f"Identity doc: {identity_len} chars")
    print(f"Model: {MODELS[model_key]['label']}")
    print(f"\nBuilding ratio conditions...")

    # Build conditions at each ratio point
    ratio_docs = {}
    for target in targets:
        doc, actual_ratio = build_combined_at_ratio(ccs, target)
        ratio_docs[target] = (doc, actual_ratio)
        print(f"  Target {target:.0%} → actual {actual_ratio:.1%} ({len(doc)} chars)")

    # Use a subset of prompts for speed (5 instead of 10)
    test_prompts = IDENTITY_PROMPTS[:5]

    # Run probe at each ratio point
    runner = ProbeRunner(f"P24_ratio_curve", prompts=test_prompts)
    runner.set_centroid_text(identity)

    for target in targets:
        doc, actual_ratio = ratio_docs[target]
        label = f"ratio_{int(actual_ratio * 100)}"
        runner.add_condition(label, doc)

    print(f"\nRunning {len(targets)} conditions × {len(test_prompts)} prompts on {MODELS[model_key]['label']}...")
    results = runner.run(model=model_key)
    runner.store()

    # Print the curve
    print(f"\n{'='*60}")
    print(f"P24 RATIO CURVE — {MODELS[model_key]['label']}")
    print(f"{'='*60}")
    print(f"{'Ratio':>8s}  {'Mean Dist':>10s}  {'Std':>8s}  {'vs 100%':>8s}")
    print(f"{'-'*8}  {'-'*10}  {'-'*8}  {'-'*8}")

    # Get 100% (identity-only) as baseline
    baseline_key = "ratio_100"
    baseline_dist = results.get(baseline_key, {}).get("mean_ccs_distance", 0)

    for target in targets:
        doc, actual_ratio = ratio_docs[target]
        label = f"ratio_{int(actual_ratio * 100)}"
        r = results.get(label, {})
        mean_d = r.get("mean_ccs_distance", 0)
        std_d = r.get("std_ccs_distance", 0)
        delta_pct = ((mean_d - baseline_dist) / baseline_dist * 100) if baseline_dist else 0
        marker = " ◄" if 0.54 <= actual_ratio <= 0.56 else ""
        print(f"  {actual_ratio:>5.1%}   {mean_d:>9.4f}   {std_d:>7.4f}   {delta_pct:>+7.1f}%{marker}")

    # Identify the optimal ratio (minimum distance)
    best_ratio = None
    best_dist = float('inf')
    for target in targets:
        doc, actual_ratio = ratio_docs[target]
        label = f"ratio_{int(actual_ratio * 100)}"
        r = results.get(label, {})
        mean_d = r.get("mean_ccs_distance", 0)
        if mean_d > 0 and mean_d < best_dist:
            best_dist = mean_d
            best_ratio = actual_ratio

    if best_ratio:
        print(f"\n  Optimal ratio: {best_ratio:.0%} (dist={best_dist:.4f})")
        if best_ratio >= 0.55:
            print(f"  → Confirms: healthy ratio produces best coherence")
        elif best_ratio < 0.50:
            print(f"  → Surprising: model prefers MORE episodic content")
        else:
            print(f"  → Boundary zone: near the 55% threshold")

    # Check for cliff vs gradient
    dists = []
    for target in targets:
        doc, actual_ratio = ratio_docs[target]
        label = f"ratio_{int(actual_ratio * 100)}"
        r = results.get(label, {})
        mean_d = r.get("mean_ccs_distance", 0)
        if mean_d > 0:
            dists.append((actual_ratio, mean_d))

    if len(dists) >= 3:
        ratios_arr = np.array([d[0] for d in dists])
        dists_arr = np.array([d[1] for d in dists])
        # Check max jump between adjacent points
        jumps = np.abs(np.diff(dists_arr))
        max_jump_idx = np.argmax(jumps)
        max_jump = jumps[max_jump_idx]
        mean_jump = np.mean(jumps)

        if max_jump > 3 * mean_jump:
            cliff_at = (ratios_arr[max_jump_idx] + ratios_arr[max_jump_idx + 1]) / 2
            print(f"\n  → CLIFF detected near {cliff_at:.0%} ratio (jump {max_jump:.4f} vs mean {mean_jump:.4f})")
        else:
            print(f"\n  → GRADIENT (no single cliff; max jump {max_jump:.4f}, mean {mean_jump:.4f})")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    model = "qwen"
    for i, arg in enumerate(sys.argv):
        if arg == "--model" and i + 1 < len(sys.argv):
            model = sys.argv[i + 1]

    if cmd == "run":
        run(model)
    elif cmd == "show":
        ProbeRunner.compare_all("P24_")
    else:
        print(f"Usage: {sys.argv[0]} [run|show] [--model llama|qwen|v3]")
