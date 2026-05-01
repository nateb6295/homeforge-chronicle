#!/usr/bin/env python3
"""Aggregate overnight experiment results into a morning digest."""
from pathlib import Path

GROK = Path("/home/nate-agx/chronicle/experiments/grokking")
OUT = GROK / "overnight"


def read_or(file, fallback="(not produced)"):
    p = OUT / file
    if p.exists():
        return p.read_text()
    return fallback


def main():
    print("# Overnight digest — grokking experiments\n")
    print("## What ran\n")
    print("1. Mul cross-seed (seeds 0, 1, 2) — anatomy + row-97 ablation")
    print("2. p=113 addition training (60k steps) — scaling probe")
    print("3. p=113 ablation on row 113 (= token at p=113)")
    print()

    print("## Cross-seed mul results\n")
    print("```")
    print(read_or("cross_seed_mul.txt").strip())
    print("```\n")

    print("## p=113 scaling result\n")
    print("Key question: does the equals-token-row finding move with vocab?\n")
    print("If row 113 is load-bearing at p=113 the way row 97 was at p=97,")
    print("the finding is about the equals token role. If row 113 is not")
    print("special, the row-97 finding was a p=97 artifact.\n")
    print("```")
    print(read_or("p113_ablation.txt").strip())
    print("```\n")

    print("## Interpretation guide\n")
    print("Reference numbers from evening (p=97, step 50000):")
    print("- add: row 97 knockout → 0.72 (baseline 1.0)")
    print("- sub: row 97 knockout → 0.51")
    print("- mul: row 97 knockout → 0.95  (NOT load-bearing for mul)")
    print()
    print("Look for at p=113 addition: if row 113 knockout is around 0.72,")
    print("the finding replicates and generalizes to any prime. If it's")
    print("much higher (closer to 1.0), the p=97 finding was fragile.\n")

    print("## For paper\n")
    print("- Distributional concentration signature: reproduces across tasks (established evening)")
    print("- Causal row-97 dependency: tight for add/sub, loose for mul (established evening)")
    print("- Fourier sparsity of row 97: similar across tasks (established evening)")
    print("- Cross-seed mul invariance: [see cross_seed_mul.txt above]")
    print("- p=113 generalization: [see p113_ablation.txt above]")

    marker = OUT / "OVERNIGHT_COMPLETE"
    if marker.exists():
        print(f"\n## Status: complete ({marker.stat().st_mtime})")
    else:
        print("\n## Status: chain incomplete (marker absent)")


if __name__ == "__main__":
    main()
