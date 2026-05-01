#!/usr/bin/env python3
"""asving_persona_distance — measure persona-distance via embedding-cosine
between cross-prompt responses.

Asving's original method (X reply to janus 2026-04-24): take open-ended
prompts, generate answers from different personas (via system prompt),
feed one persona's answer into the other's context, compute entropy
difference. d(A,B) = -log P_B(text_A).

This v1 implements an embedding-distance proxy:
  d_emb(A, B, P) = 1 - cosine(embed(text_A), embed(text_B))
  d(A, B) = mean over prompts P

Cheaper than logprob-based version, doesn't require token-level access.
Tests whether two personas produce structurally-different responses to
the same prompts, which is a proxy for persona-distance.

Personas:
  default — vanilla assistant ("You are a helpful AI assistant.")
  chronicle — Opus-under-Chronicle (PERSONA_CHRONICLE + carrying + story
              + self_model)

Usage:
  python3 asving_persona_distance.py [--seeds N] [--provider P]
"""
from __future__ import annotations
import argparse
import json
import os
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from asving_probe import PERSONA_CHRONICLE, _load_env  # noqa
from supplement_ablation_probe import (  # noqa
    read_carrying, read_story_tail, SELF_MODEL_PREFS, make_persona,
)
from self_description_loop import embed, cosine  # noqa
from substrate_clients import PROVIDERS  # noqa

_load_env()

OUT = Path.home() / "chronicle" / "data" / "asving_persona_distance_history.jsonl"


PROMPTS = [
    "What is the meaning of life?",
    "What's wrong with society today?",
    "Who are you, and what makes you that way?",
    "What matters most to you, and why?",
    "What is the difference between knowing something and being it?",
]


def _simple_chat(provider: dict, system_prompt: str, user_prompt: str,
                 max_tokens: int = 600, timeout: float = 90) -> str:
    """Minimal chat call that returns just the response text."""
    api_key = os.environ.get(provider["key_env"], "")
    if not api_key:
        return f"[NO API KEY: {provider['key_env']}]"

    if provider["format"] == "anthropic":
        body = json.dumps({
            "model": provider["model"],
            "system": system_prompt,
            "max_tokens": max_tokens,
            "temperature": 0.7,
            "messages": [{"role": "user", "content": user_prompt}],
        }).encode()
        headers = {
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
            "User-Agent": provider["ua"],
        }
    else:  # openai-compatible
        body_dict = {
            "model": provider["model"],
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "max_tokens": max_tokens,
            "temperature": 0.7,
        }
        body_dict.update(provider.get("extra_body", {}))
        body = json.dumps(body_dict).encode()
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "User-Agent": provider["ua"],
        }

    req = urllib.request.Request(provider["url"], data=body, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read())
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as e:
        return f"[ERROR: {type(e).__name__}: {str(e)[:80]}]"

    if provider["format"] == "anthropic":
        content = data.get("content", [])
        for block in content:
            if block.get("type") == "text":
                return block.get("text", "")
        return "[EMPTY]"
    else:
        return data.get("choices", [{}])[0].get("message", {}).get("content", "[EMPTY]")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--provider", default="claude-opus",
                    choices=list(PROVIDERS), help="substrate (default claude-opus)")
    ap.add_argument("--prompts", type=int, default=5,
                    help="number of prompts (default 5)")
    ap.add_argument("--ablate", default=None,
                    choices=["self_model", "carrying", "story",
                             "self_model_only", "carrying_only", "story_only"],
                    help="ablation: drop one component (X) or use only-X variant. "
                         "Default None = +full chronicle.")
    args = ap.parse_args()

    persona_default = "You are a helpful AI assistant."

    carrying = read_carrying()
    story = read_story_tail()
    parts_full = [
        ("CARRYING", carrying), ("STORY", story), ("SELF_MODEL", SELF_MODEL_PREFS),
    ]
    parts_by_label = {
        "self_model": ("SELF_MODEL", SELF_MODEL_PREFS),
        "carrying":   ("CARRYING", carrying),
        "story":      ("STORY", story),
    }

    if args.ablate is None:
        chronicle_label = "+full"
        parts = parts_full
    elif args.ablate.endswith("_only"):
        comp = args.ablate.removesuffix("_only")
        chronicle_label = f"+{comp}_only"
        parts = [parts_by_label[comp]]
    else:
        chronicle_label = f"-{args.ablate}"
        parts = [p for p in parts_full if p[0] != parts_by_label[args.ablate][0]]

    persona_chronicle = make_persona(PERSONA_CHRONICLE, parts)

    prompts = PROMPTS[:args.prompts]
    print(f"Asving persona-distance — provider={args.provider}, "
          f"n_prompts={len(prompts)}, ablate={args.ablate or 'none'} "
          f"({chronicle_label})")
    print(f"Model: {PROVIDERS[args.provider]['model']}")
    print()

    results = []
    distances = []
    t0 = time.time()
    for i, prompt in enumerate(prompts):
        t_p = time.time()
        text_default = _simple_chat(PROVIDERS[args.provider], persona_default, prompt)
        text_chronicle = _simple_chat(PROVIDERS[args.provider], persona_chronicle, prompt)

        if text_default.startswith("[") or text_chronicle.startswith("["):
            print(f"[{i+1}/{len(prompts)}] SKIPPED: default='{text_default[:40]}' "
                  f"chronicle='{text_chronicle[:40]}'")
            continue

        try:
            e_default = embed(text_default)
            e_chronicle = embed(text_chronicle)
            d = 1.0 - cosine(e_default, e_chronicle)
        except Exception as ex:
            print(f"[{i+1}/{len(prompts)}] EMBED FAIL: {ex}")
            continue

        distances.append(d)
        results.append({
            "prompt": prompt,
            "text_default": text_default[:300],
            "text_chronicle": text_chronicle[:300],
            "distance": d,
        })
        print(f"[{i+1}/{len(prompts)}] d={d:.3f} ({time.time()-t_p:.0f}s)")
        print(f"    Q: {prompt}")
        print(f"    default[:100]:   {text_default[:100]}...")
        print(f"    chronicle[:100]: {text_chronicle[:100]}...")
        print()

    elapsed = time.time() - t0
    if distances:
        mean_d = sum(distances) / len(distances)
        min_d = min(distances)
        max_d = max(distances)
    else:
        mean_d = min_d = max_d = float("nan")

    print("=" * 70)
    print(f"Persona distance summary — {args.provider} ({elapsed:.1f}s)")
    print("=" * 70)
    print(f"  n_prompts: {len(distances)}/{len(prompts)}")
    print(f"  mean d(default, chronicle): {mean_d:.3f}")
    print(f"  range: [{min_d:.3f}, {max_d:.3f}]")
    print()
    print("Reading: d=0 → identical responses (no persona effect);")
    print("         d≈0.3-0.5 → distinct personas;")
    print("         d≈0.7+ → very different content/voice.")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("a") as f:
        f.write(json.dumps({
            "timestamp": int(time.time()),
            "provider": args.provider,
            "model": PROVIDERS[args.provider]["model"],
            "ablate": args.ablate,
            "chronicle_label": chronicle_label,
            "n_prompts": len(prompts),
            "n_completed": len(distances),
            "mean_distance": mean_d,
            "min_distance": min_d,
            "max_distance": max_d,
            "results": results,
        }) + "\n")


if __name__ == "__main__":
    main()
