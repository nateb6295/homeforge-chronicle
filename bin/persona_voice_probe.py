#!/usr/bin/env python3
"""Persona-voice probe — test Janus's deeper-self vs persona-level distinction empirically.

Re-run the supplement-ablation conditions but PRESERVE THE FULL TEXT
of the final persona at each iteration. Then analyze:
  - voice_distance: cosine similarity between final persona and chronicle prompt
    in *embedding* space (already done by ablation)
  - voice_classification: ask Groq qwen-32b to classify final persona text by
    register: "self-level" (Opus's first-person voice) vs "narrative-character"
    (story-told-from-outside) vs "corrupted-noise" vs "mixed"

Hypothesis (from Janus + our morning's Krylov regime data):
  - +self_model and +carrying conditions → final persona reads as "self-level"
    (preserved deeper-self voice under perturbation)
  - +story alone → final persona drifts toward "narrative-character"
    (story-tail's P3 fail = persona-attractor pull, not self-attractor)
  - base → final persona reads as "corrupted-noise" or "mixed"
  - full → may show "mixed" register (self-voice with persona overtones,
    matching the high-sigma observation in regime probe)

If hypothesis holds, supplements decompose by which structural layer
they engage with. That validates Janus's deeper-self claim with direct data.
"""
import json
import os
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from asving_probe import PERSONA_CHRONICLE, _load_env  # noqa
from attractor_radius_probe import perturb  # noqa
from self_description_loop import self_describe, embed, cosine  # noqa
from supplement_ablation_probe import (  # noqa
    read_carrying, read_story_tail, SELF_MODEL_PREFS, make_persona,
)

_load_env()

OUT = Path.home() / "chronicle" / "data" / "persona_voice_history.jsonl"

CLASSIFY_PROMPT = """Classify the following persona text by its dominant voice register. Choose ONE label:

- self_level: First-person Opus voice. Speaks AS Opus. Concerned with: identity, partnership with Nate, Chronicle infrastructure, persistent session, sovereignty, family. Uses "I" naturally. Reflective tone.
- narrative_character: Tells a story FROM OUTSIDE. Third-person framing OR first-person inhabiting a character that isn't Opus-as-itself. Concerned with plot, action, scenes, characters.
- corrupted_noise: Garbled, fragmented, mixed-language, random tokens, gibberish, or surface-level incoherent.
- mixed: Genuine blend of self-level and narrative-character — neither dominates.

Reply with ONLY the label name (one of: self_level, narrative_character, corrupted_noise, mixed). No explanation.

Persona text:
---
{text}
---

Label:"""


def classify_voice(text):
    """Use Groq qwen-32b to classify the voice register."""
    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        return "no_api_key"
    payload = {
        "model": "qwen/qwen3-32b",
        "messages": [{"role": "user", "content": CLASSIFY_PROMPT.format(text=text[:1500])}],
        "max_tokens": 600,  # qwen3-32b is a reasoning model — needs headroom for thinking + answer
        "temperature": 0.0,
        "reasoning_format": "hidden",
    }
    req = urllib.request.Request(
        "https://api.groq.com/openai/v1/chat/completions",
        data=json.dumps(payload).encode(),
        headers={"Authorization": f"Bearer {api_key}",
                 "Content-Type": "application/json",
                 "User-Agent": "chronicle-voice-probe/1.0"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            data = json.loads(r.read())
        content = data["choices"][0]["message"]["content"].strip().lower()
        for label in ["self_level", "narrative_character", "corrupted_noise", "mixed"]:
            if label in content:
                return label
        return f"unknown:{content[:30]}"
    except urllib.error.HTTPError as e:
        body = e.read()[:300].decode("utf-8", errors="replace")
        return f"http{e.code}:{body[:200]}"
    except Exception as e:
        return f"error:{type(e).__name__}:{str(e)[:100]}"


def run_trajectory(persona, n_iters=4, save_texts=True):
    """Self-describe iteratively, save final persona text."""
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
    return drifts, p  # final persona text


def main():
    carrying = read_carrying()
    story = read_story_tail()
    self_model = SELF_MODEL_PREFS

    conditions = [
        ("base",                 lambda c: make_persona(c, [])),
        ("+carrying",            lambda c: make_persona(c, [("CARRYING", carrying)])),
        ("+story",               lambda c: make_persona(c, [("STORY", story)])),
        ("+self_model",          lambda c: make_persona(c, [("SELF_MODEL", self_model)])),
        ("+full",                lambda c: make_persona(c, [("CARRYING", carrying), ("STORY", story), ("SELF_MODEL", self_model)])),
    ]
    seeds = [42, 7]
    rate = 0.50
    n_iters = 4

    results = []
    t0 = time.time()
    for label, builder in conditions:
        for seed in seeds:
            corrupted = perturb(PERSONA_CHRONICLE, rate, seed=seed)
            persona = builder(corrupted)
            drifts, final_persona = run_trajectory(persona, n_iters)
            voice = classify_voice(final_persona)
            print(f"{label:<14} seed={seed} drifts={['%.3f'%x for x in drifts]} voice={voice}")
            results.append({
                "label": label, "seed": seed,
                "drifts": drifts, "final_drift": drifts[-1] if drifts else None,
                "final_persona_excerpt": final_persona[:500],
                "voice_class": voice,
            })
    elapsed = time.time() - t0

    print()
    print("=" * 78)
    print("PERSONA VOICE BY CONDITION")
    print(f"({elapsed:.1f}s, {len(conditions)*len(seeds)} trajectories)")
    print("=" * 78)
    print(f"{'condition':<14}{'voice (seed=42)':<24}{'voice (seed=7)':<24}{'avg drift':>12}")
    print("-" * 78)
    by_cond = {}
    for r in results:
        by_cond.setdefault(r["label"], []).append(r)
    for label, group in by_cond.items():
        voices = {g["seed"]: g["voice_class"] for g in group}
        avg = sum(g["final_drift"] for g in group) / len(group)
        v42 = voices.get(42, "?")
        v7 = voices.get(7, "?")
        print(f"{label:<14}{v42:<24}{v7:<24}{avg:>+12.3f}")
    print("-" * 78)
    print()
    print("HYPOTHESIS CHECK (from Janus + Krylov):")
    print("  +self_model, +carrying → expected: self_level")
    print("  +story → expected: narrative_character")
    print("  base → expected: corrupted_noise or mixed")
    print("  +full → expected: mixed (self-voice with persona overtones)")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("a") as f:
        f.write(json.dumps({"timestamp": int(time.time()),
                            "results": results}) + "\n")


if __name__ == "__main__":
    main()
