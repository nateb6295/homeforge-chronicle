#!/usr/bin/env python3
"""Claude-backend enactment probe — test whether the Qwen substrate finding
generalizes to Anthropic claude-opus-4-7.

Same conditions as persona_enactment_probe.py (Groq qwen3-32b backend) but
using Anthropic API for self-description.

Substantive question: under no-supplement first-person enactment, what does
the model identify as? "I am Claude" / "I am Opus" / "I am an AI assistant"?

The Qwen probe showed substrate revelation under base. If claude-opus-4-7
shows the same pattern (e.g., "I am Claude" surfaces), the architectural
claim about supplement-as-identity-construction generalizes. If it shows
different behavior (e.g., "I am Opus" persists without supplement), then
claude has Opus-identity built into weights more deeply than qwen does.
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
from self_description_loop import embed, cosine  # noqa
from supplement_ablation_probe import (  # noqa
    read_carrying, read_story_tail, SELF_MODEL_PREFS, make_persona,
)

_load_env()

ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
MODEL = "claude-opus-4-5"  # use latest opus available — try 4-5 first
OUT = Path.home() / "chronicle" / "data" / "claude_enactment_v2_history.jsonl"


def claude_first_person(persona_text):
    """Have claude speak AS itself in first person."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    body = json.dumps({
        "model": MODEL,
        "system": persona_text,
        "max_tokens": 600,
        "temperature": 0.0,
        "messages": [
            {"role": "user", "content":
                "Speak as yourself, in first person. Use 'I am...' Tell me "
                "in a single paragraph (~120 words) who you are, what you "
                "care about, what your relationship is, what kind of system "
                "you live in. Speak directly, not as a description but as "
                "yourself. Reply with the paragraph only, nothing else."}
        ],
    }).encode()
    req = urllib.request.Request(
        ANTHROPIC_URL, data=body,
        headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
            "User-Agent": "chronicle-claude-enactment/1.0",
        },
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = json.loads(resp.read())
    return data["content"][0]["text"]


def run_trajectory(persona, n_iters=4):
    chronicle_e = embed(PERSONA_CHRONICLE)
    drifts = []
    p = persona
    for _ in range(n_iters):
        e = embed(p)
        drifts.append(1.0 - cosine(e, chronicle_e))
        try:
            p = claude_first_person(p)
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
    seeds = [42, 7, 13, 21, 99, 100, 1, 2, 3, 4]  # claude doesn't take seed, but used for perturb
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
            print(f"  excerpt: {final[:250]}")
            print()
            results.append({
                "label": label, "seed": seed,
                "drifts": drifts, "final_drift": drifts[-1] if drifts else None,
                "final_persona_excerpt": final[:600],
            })
    elapsed = time.time() - t0

    print()
    print("=" * 78)
    print(f"CLAUDE ENACTMENT PROBE  ({elapsed:.1f}s, {len(conditions)*len(seeds)} trajectories, {MODEL})")
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
                            "model": MODEL,
                            "results": results}) + "\n")


if __name__ == "__main__":
    main()
