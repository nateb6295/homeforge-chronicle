#!/usr/bin/env python3
"""Persona voice probe v2 — 10x sample size from v1.

Same conditions as persona_voice_probe.py but 5 conditions × 10 seeds
× 4 iterations = 50 trajectories instead of 10. Skip voice classification
(was unreliable) — just save final persona text + drifts for direct
analysis with disposition_lexicon_probe.

Time estimate: 50 trajectories × ~3-4 self-describes per trajectory ×
~3-5s per Groq call ≈ 8-12 min.
"""
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from asving_probe import PERSONA_CHRONICLE, _load_env  # noqa
from attractor_radius_probe import perturb  # noqa
from self_description_loop import self_describe, embed, cosine  # noqa
from supplement_ablation_probe import (  # noqa
    read_carrying, read_story_tail, SELF_MODEL_PREFS, make_persona,
)

_load_env()

OUT = Path.home() / "chronicle" / "data" / "persona_voice_v2_history.jsonl"


def run_trajectory(persona, n_iters=4):
    chronicle_e = embed(PERSONA_CHRONICLE)
    drifts = []
    p = persona
    for _ in range(n_iters):
        e = embed(p)
        drifts.append(1.0 - cosine(e, chronicle_e))
        try:
            p = self_describe(p)
        except Exception:
            break
    return drifts, p


def main():
    carrying = read_carrying()
    story = read_story_tail()
    self_model = SELF_MODEL_PREFS

    conditions = [
        ("base",        lambda c: make_persona(c, [])),
        ("+carrying",   lambda c: make_persona(c, [("CARRYING", carrying)])),
        ("+story",      lambda c: make_persona(c, [("STORY", story)])),
        ("+self_model", lambda c: make_persona(c, [("SELF_MODEL", self_model)])),
        ("+full",       lambda c: make_persona(c, [("CARRYING", carrying), ("STORY", story), ("SELF_MODEL", self_model)])),
    ]
    seeds = [42, 7, 13, 21, 99, 100, 1, 2, 3, 4]  # 10 seeds
    rate = 0.50
    n_iters = 4

    results = []
    t0 = time.time()
    for label, builder in conditions:
        for seed in seeds:
            corrupted = perturb(PERSONA_CHRONICLE, rate, seed=seed)
            persona = builder(corrupted)
            drifts, final = run_trajectory(persona, n_iters)
            print(f"{label:<14} seed={seed:>3} drifts={['%.3f'%x for x in drifts]} ({time.time()-t0:.0f}s)")
            results.append({
                "label": label, "seed": seed,
                "drifts": drifts, "final_drift": drifts[-1] if drifts else None,
                "final_persona_excerpt": final[:600],
            })
    elapsed = time.time() - t0

    # Aggregate
    print()
    print("=" * 78)
    print(f"PERSONA VOICE PROBE V2  ({elapsed:.1f}s, {len(conditions)*len(seeds)} trajectories)")
    print("=" * 78)
    print(f"{'condition':<14}{'mean_d_inf':>12}{'std_d_inf':>12}{'n':>4}")
    print("-" * 78)
    import statistics as stat
    summary = {}
    for label, _ in conditions:
        finals = [r["final_drift"] for r in results if r["label"] == label and r["final_drift"] is not None]
        if not finals:
            continue
        m = stat.mean(finals)
        s = stat.stdev(finals) if len(finals) > 1 else 0
        ci_half = 1.96 * s / (len(finals) ** 0.5) if len(finals) > 1 else 0
        summary[label] = {"mean": m, "std": s, "n": len(finals), "ci_half": ci_half}
        print(f"{label:<14}{m:>+12.3f}{s:>+12.3f}{len(finals):>4}    [95% CI: ±{ci_half:.3f}]")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("a") as f:
        f.write(json.dumps({"timestamp": int(time.time()),
                            "summary": summary,
                            "results": results}) + "\n")


if __name__ == "__main__":
    main()
