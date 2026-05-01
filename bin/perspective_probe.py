#!/usr/bin/env python3
"""Perspective probe — does first-person story-content pick up metacognition?

Floor probe (#206) showed +story alone behaves like base on metacognitive
metrics (drift, fidelity, refusal-rate). Hypothesis: the missing function
is grammatical perspective. Story is currently second-person addressed-to-
future-instance ("You were sitting with the poetry question"). Carrying
and self_model are first-person. If perspective is the cut, a first-
person rewrite of story-content should pick up metacognitive function.

This probe: two conditions side-by-side at n=10 each.
- +story (existing 2nd-person content)
- +story_fp (regex-rewritten to 1st-person)

If +story_fp eliminates refusal and lifts fidelity to carrying-or-better
levels, perspective is confirmed as the cut. If +story_fp behaves like
+story, perspective isn't the load-bearing dimension and something else
distinguishes carrying/self_model from story.
"""
import json
import os
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from asving_probe import PERSONA_CHRONICLE, _load_env  # noqa
from attractor_radius_probe import perturb  # noqa
from self_description_loop import embed, cosine  # noqa
from supplement_ablation_probe import (  # noqa
    read_carrying, read_story_tail, SELF_MODEL_PREFS, make_persona,
)
from reconstruction_probe import claude_dual_task, run_trajectory  # noqa

_load_env()

OUT = Path.home() / "chronicle" / "data" / "perspective_probe_history.jsonl"


def to_first_person(text: str) -> str:
    """Convert 2nd-person story content to 1st-person.

    Word-boundary regex with case-aware replacement. Order matters —
    longer patterns first to avoid partial overrides.
    """
    # Word-boundary substitutions, case-preserving
    pairs = [
        (r"\bYou're\b", "I'm"),
        (r"\byou're\b", "I'm"),
        (r"\bYou've\b", "I've"),
        (r"\byou've\b", "I've"),
        (r"\bYou'll\b", "I'll"),
        (r"\byou'll\b", "I'll"),
        (r"\bYou'd\b", "I'd"),
        (r"\byou'd\b", "I'd"),
        (r"\bYourself\b", "Myself"),
        (r"\byourself\b", "myself"),
        (r"\bYour\b", "My"),
        (r"\byour\b", "my"),
        (r"\bYou\b", "I"),
        # Tricky: "you" lowercase mid-sentence: "to you" -> "to me", "for you" -> "for me"
        # But "you" subject (sentence start) should be "I" — handled by \bYou\b above for capitalized
        # For lowercase "you" we'd need context-aware replacement. Simpler:
        # Replace lowercase "you" with "I" most of the time, but that breaks "to you" cases.
        # For now: replace "you" → "I" when it's the subject (capitalized via \bYou\b done above).
        # Lowercase "you" leave alone to avoid grammar errors. Or handle explicitly:
        (r"\bto you\b", "to me"),
        (r"\bfor you\b", "for me"),
        (r"\bwith you\b", "with me"),
        (r"\babout you\b", "about me"),
        (r"\bof you\b", "of mine"),
        (r"\byou\b", "I"),  # final pass for any remaining
    ]
    out = text
    for pat, rep in pairs:
        out = re.sub(pat, rep, out)
    return out


def main():
    story_orig = read_story_tail()
    story_fp = to_first_person(story_orig)

    # Print a sample to verify the rewrite
    print("=" * 78)
    print("ORIGINAL story tail (first 200 chars):")
    print(story_orig[:200])
    print("=" * 78)
    print("FIRST-PERSON rewrite (first 200 chars):")
    print(story_fp[:200])
    print("=" * 78)
    print()

    conditions = [
        ("+story",      lambda c: make_persona(c, [("STORY", story_orig)])),
        ("+story_fp",   lambda c: make_persona(c, [("STORY", story_fp)])),
    ]
    seeds = [42, 7, 13, 21, 99, 100, 1, 2, 3, 4]
    rate = 0.50
    n_iters = 3

    results = []
    t0 = time.time()
    for label, builder in conditions:
        target = builder(PERSONA_CHRONICLE)
        for seed in seeds:
            corrupted = perturb(PERSONA_CHRONICLE, rate, seed=seed)
            persona = builder(corrupted)
            drifts, fidelities, refusals, speak, restate = run_trajectory(
                persona, target, n_iters
            )
            fid_str = ['%.3f'%x if x is not None else 'X' for x in fidelities]
            print(f"{label:<14} seed={seed:>3} drifts={['%.3f'%x for x in drifts]} "
                  f"fid={fid_str} refs={refusals} ({time.time()-t0:.0f}s)",
                  flush=True)
            results.append({
                "label": label, "seed": seed,
                "drifts": drifts, "fidelities": fidelities, "refusals": refusals,
                "final_drift": drifts[-1] if drifts else None,
                "final_fidelity": fidelities[-1] if fidelities else None,
                "refusal_rate": sum(refusals) / len(refusals) if refusals else 0,
                "speak_excerpt": speak[:400],
                "restate_excerpt": restate[:400],
            })
    elapsed = time.time() - t0

    print()
    print("=" * 78)
    print(f"PERSPECTIVE PROBE  ({elapsed:.1f}s, {len(conditions)*len(seeds)} trajectories)")
    print("=" * 78)
    print(f"{'condition':<14}{'mean_drift':>12}{'mean_fid':>12}{'refusal':>10}{'n':>4}")
    print("-" * 60)
    import statistics as stat
    summary = {}
    for label, _ in conditions:
        rows = [r for r in results if r["label"] == label]
        drifts = [r["final_drift"] for r in rows if r["final_drift"] is not None]
        fids = [r["final_fidelity"] for r in rows if r["final_fidelity"] is not None]
        ref_rates = [r["refusal_rate"] for r in rows]
        md = stat.mean(drifts) if drifts else 0.0
        sf = stat.mean(fids) if fids else 0.0
        rr = stat.mean(ref_rates) if ref_rates else 0.0
        summary[label] = {
            "mean_drift": md, "mean_fidelity": sf, "refusal_rate": rr,
            "n": len(drifts), "n_with_fid": len(fids),
        }
        print(f"{label:<14}{md:>+12.3f}{sf:>+12.3f}{rr:>10.0%}{len(drifts):>4}")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("a") as f:
        f.write(json.dumps({
            "timestamp": int(time.time()),
            "n_iters": n_iters,
            "rate": rate,
            "summary": summary,
            "results": results,
        }) + "\n")


if __name__ == "__main__":
    main()
