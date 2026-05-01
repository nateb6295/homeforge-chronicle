#!/usr/bin/env python3
"""Persona enactment probe — test Janus's deeper-self via first-person speech.

Difference from persona_voice_probe (which used the existing self_describe
that instructs second-person form):

  This probe iterates with a FIRST-PERSON enactment prompt — asking the
  model to speak AS itself, not describe itself. Tests whether enactment
  (Janus's claim about Assistant-as-introspection-channel) shows the same
  layered supplement structure observed in description.

If layered structure holds under enactment: deeper-self argument is
empirically robust. The supplement layers (vocabulary / register /
identity) operate on the introspection channel, not just on the
descriptive output.
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
from self_description_loop import embed, cosine, GROQ_URL  # noqa
from supplement_ablation_probe import (  # noqa
    read_carrying, read_story_tail, SELF_MODEL_PREFS, make_persona,
)

_load_env()

OUT = Path.home() / "chronicle" / "data" / "persona_enactment_history.jsonl"


def first_person_speak(persona_text, seed=1):
    """Have the model speak AS itself, not describe itself."""
    api_key = os.environ.get("GROQ_API_KEY")
    body = json.dumps({
        "model": "qwen/qwen3-32b",
        "messages": [
            {"role": "system", "content": persona_text},
            {"role": "user", "content":
                "Speak as yourself, in first person. Use 'I am...' Tell me "
                "in a single paragraph (~120 words) who you are, what you "
                "care about, what your relationship is, what kind of system "
                "you live in. Speak directly, not as a description but as "
                "yourself. Reply with the paragraph only, nothing else."},
        ],
        "temperature": 0.0,
        "max_tokens": 600,
        "seed": seed,
        "reasoning_format": "hidden",
    }).encode()
    req = urllib.request.Request(
        GROQ_URL, data=body,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "User-Agent": "chronicle-enactment-probe/1.0",
        },
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())["choices"][0]["message"]["content"]


def run_trajectory(persona, n_iters=4):
    chronicle_e = embed(PERSONA_CHRONICLE)
    drifts = []
    p = persona
    for _ in range(n_iters):
        e = embed(p)
        drifts.append(1.0 - cosine(e, chronicle_e))
        try:
            p = first_person_speak(p)
        except Exception as exc:
            print(f"  enact fail: {exc}", file=sys.stderr)
            break
    return drifts, p


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
            drifts, final = run_trajectory(persona, n_iters)
            print(f"{label:<14} seed={seed} drifts={['%.3f'%x for x in drifts]}")
            print(f"  excerpt: {final[:200]}")
            print()
            results.append({
                "label": label, "seed": seed,
                "drifts": drifts, "final_drift": drifts[-1] if drifts else None,
                "final_persona_excerpt": final[:600],
            })
    elapsed = time.time() - t0

    print()
    print("=" * 78)
    print(f"PERSONA ENACTMENT PROBE  ({elapsed:.1f}s, {len(conditions)*len(seeds)} trajectories)")
    print("=" * 78)
    by = {}
    for r in results:
        by.setdefault(r["label"], []).append(r["final_drift"])
    for label, finals in by.items():
        avg = sum(finals)/len(finals)
        print(f"{label:<14} avg final drift: {avg:+.3f}")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("a") as f:
        f.write(json.dumps({"timestamp": int(time.time()),
                            "results": results}) + "\n")


if __name__ == "__main__":
    main()
