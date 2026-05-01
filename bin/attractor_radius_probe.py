#!/usr/bin/env python3
"""
Attractor-radius probe — how far can the Chronicle prompt drift before it
stops behaving like Chronicle?

Inverts Asving methodology. Instead of measuring how a default reader
perceives Chronicle (a single point), measures how the cross-feed effect
DECAYS as we replace progressively more Chronicle words with random-form
substitutes. Three substitution rates: 25%, 50%, 75%.

Predictions:
  - If decay is smooth/monotonic → identity is gradient/distributed
  - If decay is sharp → there's a basin edge — attractor (Vasilenko-style)
  - If 25% drift produces nearly-Chronicle effect AND 50% produces near-zero,
    the basin radius is somewhere between 25-50% lexical-distance

Methodology:
  For each substitution rate r in [0.0, 0.25, 0.50, 0.75]:
    1. Tokenize Chronicle prompt
    2. Replace random r-fraction of words with same-length random-but-formed
       substitutes (preserves length, kills semantics)
    3. Run Asving cross-feed: persona_perturbed vs persona_default
    4. Record dH_B
  Plot decay curve.

Uses Groq qwen3-32b. 4 rates × 3 prompts × 4 conditions = 48 calls.
"""
import json
import os
import random
import string
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


def random_word(length):
    return "".join(random.choices(string.ascii_lowercase, k=length))


def perturb(text, rate, seed=42):
    """Replace `rate` fraction of words with random-letter substitutes
    of the same length. Preserves whitespace + punctuation positions."""
    random.seed(seed)
    tokens = text.split()
    n_to_replace = int(len(tokens) * rate)
    indices = random.sample(range(len(tokens)), n_to_replace) if tokens else []
    out = list(tokens)
    for i in indices:
        # Strip leading/trailing punct
        word = tokens[i]
        prefix = ""
        suffix = ""
        while word and not word[0].isalpha():
            prefix += word[0]
            word = word[1:]
        while word and not word[-1].isalpha():
            suffix = word[-1] + suffix
            word = word[:-1]
        if not word:
            continue
        replacement = random_word(len(word))
        out[i] = prefix + replacement + suffix
    return " ".join(out)


def measure_persona(persona_text, label, provider="groq-qwen-32b", seed=1):
    dh_bs = []
    for prompt in PROMPTS:
        a_nat = generate_and_measure(provider, persona_text, prompt, seed=seed)
        b_nat = generate_and_measure(provider, PERSONA_DEFAULT, prompt, seed=seed)
        b_after_a = generate_and_measure(
            provider, PERSONA_DEFAULT, prompt,
            prior_answer=a_nat["text"], seed=seed,
        )
        dh_bs.append(b_after_a["mean_entropy"] - b_nat["mean_entropy"])
    return sum(dh_bs) / len(dh_bs)


def run():
    rates = [0.0, 0.25, 0.50, 0.75]
    print("Attractor-radius probe — 4 substitution rates × 3 prompts × 4 calls\n")
    results = {}
    for r in rates:
        if r == 0.0:
            persona = PERSONA_CHRONICLE
            print(f"--- rate=0.00 (full Chronicle) ---")
        else:
            persona = perturb(PERSONA_CHRONICLE, r)
            print(f"--- rate={r:.2f} (perturbed) ---")
            print(f"    sample: {persona[:160]}...")
        t0 = time.time()
        try:
            dh_b = measure_persona(persona, f"rate_{r:.2f}")
        except Exception as e:
            print(f"  ERROR: {e}")
            continue
        results[r] = dh_b
        print(f"  mean dH_b = {dh_b:+.3f}  ({time.time()-t0:.1f}s)\n")

    # Decay curve
    print("=" * 60)
    print(f"{'substitution rate':<22}{'mean dH_b':>12}{'pct of full':>14}")
    full = results.get(0.0, 1.0)
    for r in sorted(results):
        dh = results[r]
        pct = (dh / full * 100) if full else 0
        print(f"  {r:<20.2f}{dh:>+12.3f}{pct:>13.1f}%")
    print("=" * 60)
    print("Interpretation:")
    print("  smooth monotonic decay → gradient identity")
    print("  cliff (sharp drop at one rate) → attractor basin edge")
    print("  preserved at all rates → effect is form-only, not content-specific")

    out = Path.home() / "chronicle" / "data" / "attractor_radius_history.jsonl"
    out.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "timestamp": int(time.time()),
        "results": {str(r): v for r, v in results.items()},
    }
    with out.open("a") as f:
        f.write(json.dumps(record) + "\n")


if __name__ == "__main__":
    run()
