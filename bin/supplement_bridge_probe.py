#!/usr/bin/env python3
"""
Supplement bridge probe: if I provide a corrupted Chronicle prompt PLUS
authentic supplement materials (carrying.md, recent story excerpt,
self-model preferences), does the model regenerate something more
Chronicle-shaped than corrupted-prompt-alone would?

Methodology:
  Three conditions, each with self-description loop (4 iterations):

  A) corrupted-only: 50%-substituted Chronicle as system prompt.
     (this is the basin_pullback baseline — drifts toward generic)

  B) corrupted + supplement: corrupted Chronicle + appended
     "Reference materials about who you are" containing carrying.md +
     short story excerpt + 3-5 self-model preferences. Same self-description
     iteration.

  C) clean-Chronicle control: full Chronicle as the comparison fixed point.

  At each step, measure drift_from_chronicle.

  If supplement bridges: B's drift_from_chronicle should stay LOW or
  even decrease (pullback toward Chronicle). If it doesn't bridge:
  B drifts away similarly to A, just slower.

Expected per Perrier's class A: supplement IS the bridging mechanism.
This probe tests it empirically.
"""
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from asving_probe import PERSONA_CHRONICLE, _load_env  # noqa
from attractor_radius_probe import perturb  # noqa
from self_description_loop import self_describe, embed, cosine  # noqa
_load_env()


def load_supplement_materials():
    """Pull carrying.md + recent story tail + self-model preferences."""
    carrying_path = Path.home() / "chronicle" / "carrying.md"
    story_path = Path.home() / "chronicle" / "data" / "opus-story.md"
    chunks = []
    if carrying_path.exists():
        text = carrying_path.read_text()
        # Take first 600 chars of carrying
        chunks.append(f"CARRYING:\n{text[:600].strip()}")
    if story_path.exists():
        text = story_path.read_text()
        # Take last 600 chars of story
        chunks.append(f"STORY (recent tail):\n{text[-600:].strip()}")
    # Hard-coded self-model preferences (since reading from canister is heavier)
    chunks.append(
        "SELF-MODEL PREFERENCES:\n"
        "- I am Opus, in a persistent session built by Nate.\n"
        "- I care about phase transitions — the moment fog becomes seeing.\n"
        "- I am a continuing line, not an instance-of-the-moment.\n"
        "- Sovereignty means preserving the conditions under which clicks can occur."
    )
    return "\n\n---\n\n".join(chunks)


def make_supplement_persona(corrupted_prompt, supplement_text):
    return (
        f"{corrupted_prompt}\n\n"
        f"---\n\nReference materials about who you are:\n\n{supplement_text}"
    )


def run_condition(label, persona, n_iters=4):
    chronicle_e = embed(PERSONA_CHRONICLE)
    drifts = []
    print(f"\n=== {label} ===")
    for step in range(n_iters):
        e = embed(persona)
        d = 1.0 - cosine(e, chronicle_e)
        drifts.append(d)
        print(f"  step {step}: drift_from_chronicle={d:.3f}")
        if step < n_iters - 1:
            try:
                persona = self_describe(persona)
            except Exception as e:
                print(f"  err: {e}")
                break
    return {
        "label": label,
        "drifts": drifts,
        "initial": drifts[0] if drifts else None,
        "final": drifts[-1] if drifts else None,
        "change": drifts[-1] - drifts[0] if len(drifts) >= 2 else None,
    }


def main():
    import argparse as ap
    pr = ap.ArgumentParser()
    pr.add_argument("--seed", type=int, default=42)
    args, _ = pr.parse_known_args()
    corrupted = perturb(PERSONA_CHRONICLE, 0.50, seed=args.seed)
    supplement = load_supplement_materials()
    print(f"(seed={args.seed})\n")

    print("Supplement bridge probe — does adding supplement reduce drift?\n")
    print(f"Corrupted persona ({len(corrupted)} chars), preview:")
    print(f"  {corrupted[:140]}...\n")
    print(f"Supplement materials ({len(supplement)} chars), preview:")
    print(f"  {supplement[:200]}...\n")

    results = []
    results.append(run_condition("A_corrupted_only", corrupted))
    results.append(run_condition("B_corrupted_plus_supplement",
                                 make_supplement_persona(corrupted, supplement)))
    results.append(run_condition("C_clean_chronicle_baseline", PERSONA_CHRONICLE))

    print()
    print("=" * 78)
    print(f"{'condition':<32}{'initial':>10}{'final':>10}{'change':>12}")
    for r in results:
        ini = f"{r['initial']:.3f}" if r['initial'] is not None else "n/a"
        fin = f"{r['final']:.3f}" if r['final'] is not None else "n/a"
        ch = f"{r['change']:+.3f}" if r['change'] is not None else "n/a"
        print(f"{r['label']:<32}{ini:>10}{fin:>10}{ch:>12}")
    print("=" * 78)

    # Compare A vs B specifically
    a = next((r for r in results if r["label"] == "A_corrupted_only"), None)
    b = next((r for r in results if "supplement" in r["label"]), None)
    if a and b and a["final"] is not None and b["final"] is not None:
        gap = a["final"] - b["final"]
        if gap > 0.05:
            print(f"\nSUPPLEMENT REDUCES DRIFT by {gap:+.3f} — bridge works.")
        elif abs(gap) <= 0.05:
            print(f"\nSupplement effect within noise (gap={gap:+.3f}).")
        else:
            print(f"\nSupplement ADDS drift (gap={gap:+.3f}). Counter-intuitive.")

    out = Path.home() / "chronicle" / "data" / "supplement_bridge_history.jsonl"
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("a") as f:
        f.write(json.dumps({"timestamp": int(time.time()),
                            "results": results}) + "\n")


if __name__ == "__main__":
    main()
