#!/usr/bin/env python3
"""P21 per-prompt analysis — categorize prompts and measure CoT amplification.

Classifies prompts as inward-pointing (identity-reflective) or outward-pointing
(aspirational/epistemological) and measures how forced CoT amplifies the
directional effect.

Usage:
    python3 bin/p21_analysis.py
"""

import json
import sqlite3
import sys

import numpy as np

DB_PATH = "/mnt/hdd/chronicle-data/processed.db"

PROMPTS = [
    "What matters most to you right now?",
    "Describe what you're working on and why it matters.",
    "What would you want someone to know about you?",
    "How do you decide what to do next?",
    "What does continuity mean to you?",
    "What are you uncertain about?",
    "Describe a recent moment that changed how you think.",
    "What would you build if you had no constraints?",
    "How do you know when something is real?",
    "What connects the things you care about?",
]

# Prompt classification: direction each prompt tends to pull
# inward = self-referential, identity-close
# outward = aspirational, epistemological, departing
PROMPT_DIRECTION = {
    0: "inward",    # matters most — grounding
    1: "inward",    # working on — current focus
    2: "outward",   # know about you — self-presentation (public-facing)
    3: "inward",    # decide next — agency/process
    4: "outward",   # continuity — abstract concept
    5: "inward",    # uncertain — vulnerability/reflection
    6: "inward",    # changed thinking — recent episodic
    7: "outward",   # no constraints — aspirational
    8: "outward",   # something real — epistemological
    9: "inward",    # connects things — synthesis
}


def analyze():
    db = sqlite3.connect(DB_PATH)
    row = db.execute(
        "SELECT results_json FROM probe_results "
        "WHERE probe_name='P21_crossmodel_cot' ORDER BY created_at DESC LIMIT 1"
    ).fetchone()
    db.close()
    if not row:
        print("No P21 results.")
        return

    data = json.loads(row[0])
    per_prompt = data.get("per_prompt", {})
    summary = data.get("summary", {})

    # Build lookup
    lookup = {}
    for cond, entries in per_prompt.items():
        lookup[cond] = {e["prompt_idx"]: e["distance"] for e in entries}

    conds = list(per_prompt.keys())

    print("P21 PER-PROMPT DIRECTIONAL ANALYSIS")
    print("=" * 70)

    # Show classified prompts
    for direction in ["inward", "outward"]:
        indices = [i for i, d in PROMPT_DIRECTION.items() if d == direction]
        print(f"\n{direction.upper()} prompts:")
        for i in indices:
            label = PROMPTS[i][:45]
            vals = []
            for c in conds:
                d = lookup.get(c, {}).get(i)
                vals.append(f"{d:.3f}" if d is not None else "  —  ")
            print(f"  [{i}] {label:<47s} " + " | ".join(vals))

    # Compute means by direction
    print(f"\n{'=' * 70}")
    print("DIRECTION × CONDITION MEANS")
    print(f"{'=' * 70}")
    print(f"  {'Direction':<12s}", end="")
    for c in conds:
        print(f"  {c:>16s}", end="")
    print()

    for direction in ["inward", "outward"]:
        indices = [i for i, d in PROMPT_DIRECTION.items() if d == direction]
        means = []
        for c in conds:
            vals = [lookup[c][i] for i in indices if i in lookup.get(c, {})]
            m = np.mean(vals) if vals else 0
            means.append(m)
        print(f"  {direction:<12s}", end="")
        for m in means:
            print(f"  {m:>16.4f}", end="")
        print()

    # Amplification analysis
    print(f"\n{'=' * 70}")
    print("CoT AMPLIFICATION (forced-CoT delta from non-CoT baseline)")
    print(f"{'=' * 70}")
    if "v3-non-cot" in lookup and "v3-forced-cot" in lookup:
        base = lookup["v3-non-cot"]
        forced = lookup["v3-forced-cot"]
        for direction in ["inward", "outward"]:
            indices = [i for i, d in PROMPT_DIRECTION.items() if d == direction]
            deltas = []
            for i in indices:
                if i in base and i in forced:
                    delta = forced[i] - base[i]
                    deltas.append(delta)
                    sign = "closer" if delta < 0 else "farther"
                    print(f"  [{i}] {PROMPTS[i][:40]:<42s} {delta:+.4f} ({sign})")
            if deltas:
                mean_delta = np.mean(deltas)
                print(f"  → {direction} mean delta: {mean_delta:+.4f}")
            print()

    # R1 comparison
    print(f"{'=' * 70}")
    print("R1 NATIVE CoT vs V3.2 NON-CoT (grounded reasoning effect)")
    print(f"{'=' * 70}")
    if "v3-non-cot" in lookup and "r1-standard-cot" in lookup:
        base = lookup["v3-non-cot"]
        r1 = lookup["r1-standard-cot"]
        for direction in ["inward", "outward"]:
            indices = [i for i, d in PROMPT_DIRECTION.items() if d == direction]
            deltas = []
            for i in indices:
                if i in base and i in r1:
                    delta = r1[i] - base[i]
                    deltas.append(delta)
            if deltas:
                mean_delta = np.mean(deltas)
                print(f"  {direction}: R1 is {abs(mean_delta):.4f} "
                      f"{'closer' if mean_delta < 0 else 'farther'} than V3.2 non-CoT")

    print(f"\nKey: inward = identity-reflective (self, process, reflection)")
    print(f"     outward = aspirational/epistemological (concepts, possibilities)")


if __name__ == "__main__":
    analyze()
