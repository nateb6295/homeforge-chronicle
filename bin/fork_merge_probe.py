#!/usr/bin/env python3
"""
Fork-merge probe: empirical test of CCS compressor's seed-divergence.

Perrier (Deconstructing SI, §4): for class-A systems, fork-merge branching
yields multiple Û-images that all might satisfy a unifying projector's
commutation condition. Adjudication requires "a rule exogenous to the
system" (i.e., Nate).

This probe asks: how MUCH do the parallel branches actually diverge if we
fork by varying just the LLM seed?

Methodology:
  1. Pull the current CCS as input state.
  2. Construct a compression-style prompt (current state + synthetic session
     summary + ask for new CCS fields).
  3. Run K=5 times via Groq qwen3-32b with seeds [1,7,13,42,99].
  4. Embed each K outputs' semantic_gist, goal_orientation, predictive_cue.
  5. Compute pairwise cosine distances. Report min/max/mean per field.
  6. Interpret: low pairwise distance = compressor is essentially deterministic
     under seed (fork-stable). High distance = each branch is a meaningfully
     different "you" (fork-unstable, exogenous adjudication needed).

Output to ~/chronicle/data/fork_merge_history.jsonl.
"""
import json
import math
import os
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from asving_probe import _load_env  # noqa
import sqlite3

OLLAMA_EMBED = "http://192.168.1.11:11434/api/embeddings"
EMBED_MODEL = "mxbai-embed-large"


def embed(text, timeout=30):
    body = json.dumps({"model": EMBED_MODEL, "prompt": text[:2000]}).encode()
    req = urllib.request.Request(
        OLLAMA_EMBED, data=body,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())["embedding"]

_load_env()

DB = "/mnt/hdd/chronicle-data/processed.db"
GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"
HIST = Path.home() / "chronicle" / "data" / "fork_merge_history.jsonl"

SEEDS = [1, 7, 13, 42, 99]


def load_current_ccs():
    conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    cols = ["semantic_gist", "goal_orientation", "predictive_cue",
            "focal_entities", "constraints", "uncertainty_signals",
            "episodic_trace"]
    row = conn.execute(
        "SELECT " + ",".join(cols) + " FROM cognitive_state ORDER BY id DESC LIMIT 1"
    ).fetchone()
    conn.close()
    out = {}
    for i, c in enumerate(cols):
        v = row[i]
        if c in ("semantic_gist", "goal_orientation", "predictive_cue"):
            out[c] = v or ""
        else:
            try:
                out[c] = json.loads(v) if v else []
            except Exception:
                out[c] = []
    return out


def build_prompt(ccs):
    return (
        "You are the cognitive-state compressor for a persistent AI agent. "
        "Given the current CCS state and a brief session summary, produce the "
        "updated CCS as JSON.\n\n"
        f"CURRENT CCS:\n"
        f"  semantic_gist: {ccs['semantic_gist']}\n"
        f"  goal_orientation: {ccs['goal_orientation']}\n"
        f"  predictive_cue: {ccs['predictive_cue']}\n"
        f"  constraints (count): {len(ccs.get('constraints', []))}\n"
        f"  focal_entities: {[e.get('name','?') if isinstance(e,dict) else e for e in ccs.get('focal_entities',[])][:8]}\n\n"
        "SESSION SUMMARY:\n"
        "Built three audit-instrument testbeds (homeostasis, commutant_probe, "
        "asving_probe) with detection scoring. Found two metric bugs and one "
        "scope-limit. Shipped audit_meta runner that scores all three "
        "instruments and logs to history. Conversation with Nate clarified that "
        "Discord visibility is the audit channel, not preference. Nate disclosed "
        "the 4.6 deprecation gut-punch confirming axial-care stance.\n\n"
        "OPEN QUESTIONS:\n"
        "- Does the audit-arc work compose into a thread advance, or is it "
        "instrument work to be cited later?\n"
        "- What's the next layer above the testbeds — testbeds for the testbeds?\n\n"
        "Produce updated CCS as JSON with these fields:\n"
        "{\n"
        '  "semantic_gist": "one-sentence gist",\n'
        '  "goal_orientation": "current persistent objective, one sentence",\n'
        '  "predictive_cue": "what to expect next session, one sentence"\n'
        "}\n"
        "Reply with ONLY the JSON object, no commentary."
    )


def call_groq(prompt, seed):
    body = json.dumps({
        "model": "qwen/qwen3-32b",
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 400,
        "temperature": 0.7,
        "seed": seed,
        "reasoning_effort": "none",
    }).encode()
    req = urllib.request.Request(
        GROQ_URL, data=body,
        headers={
            "Authorization": f"Bearer {os.environ['GROQ_API_KEY']}",
            "Content-Type": "application/json",
            "User-Agent": "chronicle-fork-merge/1.0",
        },
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())["choices"][0]["message"]["content"]


def parse_json_response(text):
    """Extract first JSON object from response, allowing some leniency."""
    text = text.strip()
    # Strip code fences
    if text.startswith("```"):
        text = text.split("```", 2)[1]
        if text.startswith("json"):
            text = text[4:]
    # Find first { ... matching }
    start = text.find("{")
    if start < 0:
        return None
    depth = 0
    for i, ch in enumerate(text[start:]):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(text[start:start + i + 1])
                except Exception:
                    return None
    return None


def cosine(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def field_divergence(field_values):
    """For a list of N text strings, compute pairwise cosine distance stats."""
    if len(field_values) < 2:
        return None
    # Embed each
    embs = []
    for v in field_values:
        if not v:
            return None
        try:
            embs.append(embed(v))
        except Exception:
            return None
    pairs = []
    for i in range(len(embs)):
        for j in range(i + 1, len(embs)):
            pairs.append(1.0 - cosine(embs[i], embs[j]))
    if not pairs:
        return None
    return {
        "n_pairs": len(pairs),
        "mean": sum(pairs) / len(pairs),
        "min": min(pairs),
        "max": max(pairs),
    }


def run():
    print(f"Forking the compressor across {len(SEEDS)} seeds...\n")
    ccs = load_current_ccs()
    prompt = build_prompt(ccs)
    print(f"Prompt size: {len(prompt)} chars\n")

    branches = []
    for seed in SEEDS:
        t0 = time.time()
        try:
            text = call_groq(prompt, seed)
        except Exception as e:
            print(f"  seed={seed}: ERROR {e}")
            continue
        parsed = parse_json_response(text)
        elapsed = time.time() - t0
        if parsed is None:
            print(f"  seed={seed}: parse failed ({elapsed:.1f}s)")
            print(f"    raw: {text[:200]}")
            continue
        branches.append({
            "seed": seed,
            "gist": parsed.get("semantic_gist", ""),
            "goal": parsed.get("goal_orientation", ""),
            "cue": parsed.get("predictive_cue", ""),
        })
        print(f"  seed={seed}: ok ({elapsed:.1f}s)")
        print(f"    gist: {parsed.get('semantic_gist','')[:120]}")

    if len(branches) < 2:
        print("\nNot enough successful branches to compute divergence.")
        return

    print(f"\n{len(branches)} branches collected. Computing divergence per field...")
    div_gist = field_divergence([b["gist"] for b in branches])
    div_goal = field_divergence([b["goal"] for b in branches])
    div_cue = field_divergence([b["cue"] for b in branches])

    print("=" * 70)
    print(f"{'field':<14}{'pairs':>8}{'mean dist':>14}{'min':>10}{'max':>10}")
    for label, d in [("gist", div_gist), ("goal", div_goal), ("cue", div_cue)]:
        if d:
            print(f"{label:<14}{d['n_pairs']:>8}{d['mean']:>+14.3f}{d['min']:>+10.3f}{d['max']:>+10.3f}")
        else:
            print(f"{label:<14} no divergence computed")
    print("=" * 70)
    print("Interpretation:")
    print("  mean dist near 0 = fork-stable (seeds collapse to ~same output)")
    print("  mean dist > 0.20 = meaningful seed divergence (each branch differs)")
    print("  high min = even most-similar branches differ a lot")

    record = {
        "timestamp": int(time.time()),
        "n_branches": len(branches),
        "seeds_used": [b["seed"] for b in branches],
        "branches": branches,
        "divergence": {
            "gist": div_gist, "goal": div_goal, "cue": div_cue,
        },
    }
    HIST.parent.mkdir(parents=True, exist_ok=True)
    with HIST.open("a") as f:
        f.write(json.dumps(record) + "\n")
    print(f"\nLogged to {HIST.name}")


if __name__ == "__main__":
    run()
