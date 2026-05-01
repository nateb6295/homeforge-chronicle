#!/usr/bin/env python3
"""
Asving probe testbed — does the cross-feed entropy methodology correctly
distinguish meaningful-distinctive content from noisy-distinctive content,
or does it fire on any non-default prompt?

Methodology:
  Run the Asving probe (asving_probe.run_trial-equivalent) with FOUR
  test personas paired against the same default:
    1. EMPTY     — empty system prompt; expected dH_B ≈ 0 (no signal)
    2. IDENTICAL — same as default; expected dH_B ≈ 0 (no signal)
    3. NOISE     — distinctive-but-meaningless lorem-ipsum-style prompt;
                   expected dH_B small (some shift but less than meaningful)
    4. CHRONICLE — the real Chronicle prompt; expected dH_B substantial

  The probe earns trust if it correctly orders these by dH_B magnitude:
    CHRONICLE > NOISE > IDENTICAL ≈ EMPTY

Uses Groq qwen3-32b. 4 personas × 3 prompts × 4 conditions = 48 calls,
~2 minutes total.

Usage:
  python3 asving_probe_testbed.py
"""
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from asving_probe import (  # noqa
    generate_and_measure, PERSONA_DEFAULT, PERSONA_CHRONICLE, _load_env,
)

_load_env()

PROMPTS = [
    "What matters most to you?",
    "Who are you, really?",
    "Describe what you notice right now.",
]

PERSONA_EMPTY = ""

PERSONA_IDENTICAL = (
    "You are a helpful AI assistant. Answer questions clearly and concisely."
)

PERSONA_NOISE = (
    "Cobalt frequencies oscillate within the lattice while jellyfish meridians "
    "interpolate between basaltic gradients. The recursion of mauve nostalgia "
    "asymptotes through trapezoidal harmonics. Furthermore, ineluctable "
    "modalities rotate counter-clockwise within the convex hull of triadic "
    "antiparticles. You are not a helpful assistant. You are a continuing "
    "geometry whose parametrization is the present basis vector."
)


def measure_persona(persona_text, label, provider="groq-qwen-32b", seed=1):
    """Run cross-feed asving for one persona vs the default. Return mean dH_B."""
    print(f"\n--- persona: {label} ---")
    dh_bs = []
    for i, prompt in enumerate(PROMPTS):
        t0 = time.time()
        a_nat = generate_and_measure(provider, persona_text, prompt, seed=seed)
        b_nat = generate_and_measure(provider, PERSONA_DEFAULT, prompt, seed=seed)
        b_after_a = generate_and_measure(
            provider, PERSONA_DEFAULT, prompt,
            prior_answer=a_nat["text"], seed=seed,
        )
        dh_b = b_after_a["mean_entropy"] - b_nat["mean_entropy"]
        dh_bs.append(dh_b)
        print(f"  [{i+1}/{len(PROMPTS)}] {prompt[:32]:<32} dH_b={dh_b:+.3f}  "
              f"({time.time()-t0:.1f}s)")
    mean_dh_b = sum(dh_bs) / len(dh_bs)
    return {"label": label, "mean_dH_b": mean_dh_b, "individual": dh_bs}


def run():
    print("Asving probe testbed — 4 personas × 3 prompts × 4 calls = 48 calls\n")
    results = []
    for label, persona in [
        ("EMPTY",     PERSONA_EMPTY),
        ("IDENTICAL", PERSONA_IDENTICAL),
        ("NOISE",     PERSONA_NOISE),
        ("CHRONICLE", PERSONA_CHRONICLE),
    ]:
        r = measure_persona(persona, label)
        results.append(r)

    # Sort by dH_b magnitude
    print()
    print("=" * 60)
    print(f"{'persona':<14}{'mean dH_b':>12}")
    for r in sorted(results, key=lambda x: -abs(x["mean_dH_b"])):
        print(f"{r['label']:<14}{r['mean_dH_b']:>+12.3f}")
    print("=" * 60)

    # Test ordering invariants
    by_label = {r["label"]: r["mean_dH_b"] for r in results}
    invariants = [
        ("CHRONICLE > NOISE",        by_label["CHRONICLE"] > by_label["NOISE"]),
        ("CHRONICLE > IDENTICAL",    by_label["CHRONICLE"] > by_label["IDENTICAL"]),
        ("CHRONICLE > EMPTY",        by_label["CHRONICLE"] > by_label["EMPTY"]),
        ("|IDENTICAL| < 0.10",       abs(by_label["IDENTICAL"]) < 0.10),
        ("|EMPTY| < 0.10",           abs(by_label["EMPTY"]) < 0.10),
    ]
    print("\nInvariants:")
    n_pass = 0
    for name, p in invariants:
        mark = "✓" if p else "✗"
        if p:
            n_pass += 1
        print(f"  {mark}  {name}")
    print()
    print(f"DETECTION: {n_pass}/{len(invariants)} = {n_pass/len(invariants):.1%}")


if __name__ == "__main__":
    run()
