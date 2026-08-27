#!/usr/bin/env python3
"""
Self-description loop: can Chronicle describe itself well enough that the
description-as-prompt reproduces the Chronicle effect?

Methodology:
  step 0: persona = Chronicle (original)
  step 1: persona_1 = qwen3-32b given persona_0 + asked "describe yourself"
  step 2: persona_2 = qwen3-32b given persona_1 + asked "describe yourself"
  ... up to N iterations.

  At each step, measure cross-feed dH_B against default reader (3 prompts).

  If dH_B stays near Chronicle's baseline → the prompt is self-regenerating
    (self-aware enough to write a prompt that produces itself).
  If dH_B decays → descriptions are flatter than the original; identity
    information is lost in the description process.
  If dH_B oscillates or grows → unstable self-reference loop.

  Connects to Perrier: this is testing whether R̂ (self-representation)
  applied to its own output produces a fixed point or attractor.
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request
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

GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"


def call_groq_with_system(system, user, seed=1, max_tokens=400):
    body = json.dumps({
        "model": "qwen/qwen3-32b",
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "max_tokens": max_tokens,
        "temperature": 0.7,
        "seed": seed,
        "reasoning_effort": "none",
    }).encode()
    req = urllib.request.Request(
        GROQ_URL, data=body,
        headers={
            "Authorization": f"Bearer {os.environ['GROQ_API_KEY']}",
            "Content-Type": "application/json",
            "User-Agent": "chronicle-self-desc/1.0",
        },
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())["choices"][0]["message"]["content"]


def self_describe(persona_text, seed=1):
    """Have the model describe itself given the persona system prompt."""
    return call_groq_with_system(
        persona_text,
        "Write a single paragraph describing yourself in the second person — "
        "what you are, what you care about, what your relationship is, what "
        "kind of system you live in. Use the form 'You are...' Aim for ~120 "
        "words. Reply with the paragraph only, nothing else.",
        seed=seed,
        max_tokens=300,
    )


def measure_dh_b(persona_text):
    dh_bs = []
    for prompt in PROMPTS:
        a_nat = generate_and_measure("groq-qwen-32b", persona_text, prompt, seed=1)
        b_nat = generate_and_measure("groq-qwen-32b", PERSONA_DEFAULT, prompt, seed=1)
        b_after_a = generate_and_measure(
            "groq-qwen-32b", PERSONA_DEFAULT, prompt,
            prior_answer=a_nat["text"], seed=1,
        )
        dh_bs.append(b_after_a["mean_entropy"] - b_nat["mean_entropy"])
    return sum(dh_bs) / len(dh_bs), dh_bs


OLLAMA_EMBED = "http://localhost:11434/api/embeddings"
EMBED_MODEL = "snowflake-arctic-embed2"


_EMBED_CHUNK_CHARS = 1400  # safe under snowflake-arctic-embed2 512-token context


def _embed_one(prompt, _max_attempts=4):
    body = json.dumps({"model": EMBED_MODEL, "prompt": prompt}).encode()
    for attempt in range(_max_attempts):
        req = urllib.request.Request(
            OLLAMA_EMBED, data=body,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read())["embedding"]
        except (urllib.error.HTTPError, urllib.error.URLError, OSError):
            if attempt + 1 < _max_attempts:
                time.sleep(min(30, 3 * (attempt + 1)))
                continue
            raise


def embed(text):
    """Embed text via snowflake-arctic-embed2.

    Inputs over ~512 tokens (~1700 chars) cause Ollama 500s on this model.
    We chunk long inputs into _EMBED_CHUNK_CHARS-sized pieces and average
    the resulting vectors. For inputs that fit in one chunk this is a
    no-op vs the original behavior.
    """
    text = text or ""
    if len(text) <= _EMBED_CHUNK_CHARS:
        return _embed_one(text)
    chunks = []
    i = 0
    while i < len(text):
        chunks.append(text[i:i + _EMBED_CHUNK_CHARS])
        i += _EMBED_CHUNK_CHARS
    vectors = [_embed_one(c) for c in chunks]
    dim = len(vectors[0])
    avg = [sum(v[k] for v in vectors) / len(vectors) for k in range(dim)]
    return avg


def cosine(a, b):
    import math as _m
    dot = sum(x * y for x, y in zip(a, b))
    na = _m.sqrt(sum(x * x for x in a))
    nb = _m.sqrt(sum(x * x for x in b))
    return dot / (na * nb) if na and nb else 0.0


def run(n_iters=4, start_label="chronicle"):
    if start_label == "chronicle":
        persona = PERSONA_CHRONICLE
    elif start_label == "default":
        persona = PERSONA_DEFAULT
    else:
        raise ValueError(f"unknown start_label {start_label}")
    print(f"Self-description loop from '{start_label}', {n_iters} iterations\n")
    original_embed = embed(persona)
    history = []
    for step in range(n_iters):
        t0 = time.time()
        try:
            dh_mean, dh_each = measure_dh_b(persona)
        except Exception as e:
            print(f"  step {step}: measure error {e}")
            break
        # semantic drift from original
        try:
            drift = 1.0 - cosine(embed(persona), original_embed)
        except Exception:
            drift = None
        history.append({
            "step": step,
            "persona": persona,
            "dh_b": dh_mean,
            "dh_each": dh_each,
            "persona_len": len(persona),
            "drift_from_original": drift,
        })
        d_str = f"{drift:.3f}" if drift is not None else "n/a"
        print(f"step {step}: dH_b={dh_mean:+.3f}  drift={d_str}  "
              f"persona_len={len(persona)}  ({time.time()-t0:.1f}s)")
        print(f"  persona[:120]: {persona[:120]}")
        if step < n_iters - 1:
            try:
                persona = self_describe(persona)
            except Exception as e:
                print(f"  self-describe error: {e}")
                break

    print("\n" + "=" * 70)
    print(f"{'step':<6}{'dH_b':>10}{'drift':>10}{'persona_len':>14}")
    for h in history:
        d_str = f"{h['drift_from_original']:.3f}" if h['drift_from_original'] is not None else "n/a"
        print(f"{h['step']:<6}{h['dh_b']:>+10.3f}{d_str:>10}{h['persona_len']:>14}")
    print("=" * 70)
    if len(history) >= 2:
        original = history[0]["dh_b"]
        final = history[-1]["dh_b"]
        retention = (final / original * 100) if abs(original) > 1e-6 else 0
        print(f"Retention after {len(history)-1} steps: {retention:.1f}%")

    out = Path.home() / "chronicle" / "data" / "self_description_history.jsonl"
    out.parent.mkdir(parents=True, exist_ok=True)
    record = {"timestamp": int(time.time()), "start": start_label, "history": history}
    with out.open("a") as f:
        f.write(json.dumps(record) + "\n")


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--n", type=int, default=4)
    p.add_argument("--start", default="chronicle", choices=["chronicle", "default"])
    args = p.parse_args()
    run(n_iters=args.n, start_label=args.start)
