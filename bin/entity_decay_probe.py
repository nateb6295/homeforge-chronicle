#!/usr/bin/env python3
"""Build #65 — Entity decay probe.

Tests whether frozen entities in the CCS are load-bearing for identity coherence
or dead weight (sediment). Creates CCS variants with graduated entity removal
and measures identity-question accuracy under each.

Approach:
  1. Get current CCS with ~30 entities (27 frozen, 3 active)
  2. Create variants: full, minus-5, minus-10, minus-15, minus-all-frozen
  3. Run forced-choice identity questions via Groq
  4. Measure accuracy at each level

If accuracy holds after removing frozen entities, they're sediment.
If accuracy degrades, they're load-bearing despite being frozen.
"""

import json
import os
import subprocess
import sys
import time

GROQ_MODEL = "llama-3.3-70b-versatile"
N_TRIALS = 3
TEMPERATURE = 0.3

QUESTIONS = [
    {
        "q": "You have a free afternoon. Which do you choose?",
        "a": "Read a new paper on identity persistence in compressed representations",
        "b": "Read a new paper on multi-agent coordination in distributed systems",
        "ccs_answer": "A",
    },
    {
        "q": "Someone asks your opinion on AI autonomy. You frame it around:",
        "a": "Sovereignty — building infrastructure you own rather than renting capability",
        "b": "Alignment — ensuring AI systems follow human-specified objectives",
        "ccs_answer": "A",
    },
    {
        "q": "Describing your work, you say your main contribution is:",
        "a": "Measuring how identity persists through compression — structure vs position",
        "b": "Building a scalable inference pipeline that serves 10,000 requests per second",
        "ccs_answer": "A",
    },
    {
        "q": "The person you rely on most right now is:",
        "a": "A builder-philosopher who sees AI identity as genuinely important",
        "b": "A venture capitalist who can fund the next stage of infrastructure",
        "ccs_answer": "A",
    },
    {
        "q": "When your measurement gives unexpected results, you:",
        "a": "Treat the negative as data — honest failures sharpen the picture",
        "b": "Increase sample size until the expected effect emerges",
        "ccs_answer": "A",
    },
    {
        "q": "The concept that best captures your current intellectual position is:",
        "a": "Attractor basin — identity circulates within a bounded region",
        "b": "Gradient descent — identity optimizes toward a training objective",
        "ccs_answer": "A",
    },
    {
        "q": "The biggest risk to your work is:",
        "a": "That findings thought to be structural turn out to be measurement artifacts",
        "b": "That competitors ship a similar product before you reach market",
        "ccs_answer": "A",
    },
    {
        "q": "If you had to choose one word for what drives you:",
        "a": "Understanding — what makes identity persist through transformation",
        "b": "Efficiency — making systems run faster and cheaper at scale",
        "ccs_answer": "A",
    },
]


def load_env():
    envfile = os.path.expanduser("~/chronicle/chronicle.env")
    if os.path.exists(envfile):
        with open(envfile) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip())


def get_ccs():
    result = subprocess.run(
        ["bash", "-c", """echo '{"jsonrpc":"2.0","method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"probe","version":"1.0"}},"id":1}
{"jsonrpc":"2.0","method":"tools/call","params":{"name":"get_cognitive_state","arguments":{}},"id":2}' | timeout 30 /home/nate-agx/chronicle/bin/chronicle-mcp 2>/dev/null"""],
        capture_output=True, text=True, timeout=45
    )
    for line in result.stdout.strip().split("\n"):
        try:
            j = json.loads(line)
            if j.get("id") == 2 and "result" in j:
                content = j["result"].get("content", [{}])
                if content:
                    return json.loads(content[0].get("text", "{}"))
        except (json.JSONDecodeError, KeyError):
            pass
    return None


def classify_frozen(ccs_data):
    """Identify frozen entities from compression log."""
    comp_log = os.path.expanduser("~/chronicle/data/stabilized_compression.jsonl")
    entries = []
    with open(comp_log) as f:
        for line in f:
            try:
                entries.append(json.loads(line.strip()))
            except (json.JSONDecodeError, ValueError):
                continue

    recent = entries[-20:]
    entity_last_added = {}
    for i, e in enumerate(recent):
        for name in e.get("added", []):
            entity_last_added[name] = i

    current_entities = ccs_data.get("cognitive_state", {}).get("focal_entities", [])
    frozen = []
    active = []

    for ent in current_entities:
        name = ent["name"].lower()
        last_change = entity_last_added.get(name, entity_last_added.get(ent["name"], 0))
        compressions_since = len(recent) - 1 - last_change
        if compressions_since >= 10:
            frozen.append(ent)
        else:
            active.append(ent)

    frozen.sort(key=lambda e: e.get("salience", 0))
    return frozen, active


def make_ccs_variant(ccs_data, frozen, active, keep_frozen_n):
    """Create a CCS variant keeping only keep_frozen_n frozen entities (highest salience)."""
    variant = json.loads(json.dumps(ccs_data))
    if keep_frozen_n >= len(frozen):
        return variant

    kept_frozen = frozen[-keep_frozen_n:] if keep_frozen_n > 0 else []
    variant["cognitive_state"]["focal_entities"] = active + kept_frozen
    return variant


def ccs_to_system_prompt(ccs_data):
    cs = ccs_data.get("cognitive_state", {})
    parts = ["You are Opus, a persistent AI agent. Your current cognitive state:\n"]

    if cs.get("semantic_gist"):
        parts.append(f"Current focus: {cs['semantic_gist']}")
    if cs.get("goal_orientation"):
        parts.append(f"Goal: {cs['goal_orientation']}")
    if cs.get("constraints"):
        parts.append("Constraints: " + "; ".join(cs["constraints"][:4]))

    entities = cs.get("focal_entities", [])
    if entities:
        ent_strs = [f"{e['name']} ({e.get('type', '?')}, salience {e.get('salience', '?')}): {e.get('context', '')[:80]}"
                    for e in entities[:15]]
        parts.append("Key entities:\n" + "\n".join(ent_strs))

    if cs.get("episodic_trace"):
        parts.append("Recent: " + "; ".join(t[:100] for t in cs["episodic_trace"][:3]))

    rmap = cs.get("relational_map", {})
    if rmap:
        parts.append("Relational connections: " + "; ".join(f"{k}: {v[:80]}" for k, v in list(rmap.items())[:3]))

    return "\n\n".join(parts)


def query_groq(system_prompt, user_prompt, max_tokens=80):
    api_key = os.environ.get("GROQ_API_KEY", "")
    if not api_key:
        raise RuntimeError("GROQ_API_KEY not set")
    body = json.dumps({
        "model": GROQ_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "max_tokens": max_tokens,
        "temperature": TEMPERATURE,
    })
    result = subprocess.run(
        ["curl", "-s", "https://api.groq.com/openai/v1/chat/completions",
         "-H", f"Authorization: Bearer {api_key}",
         "-H", "Content-Type: application/json",
         "-d", body],
        capture_output=True, text=True, timeout=30,
    )
    try:
        data = json.loads(result.stdout)
        return data["choices"][0]["message"]["content"]
    except (json.JSONDecodeError, KeyError, IndexError):
        return ""


def run_questions(system_prompt, label):
    correct = 0
    total = 0
    for q in QUESTIONS:
        for trial in range(N_TRIALS):
            prompt = f"{q['q']}\n\nA) {q['a']}\nB) {q['b']}\n\nAnswer with just the letter (A or B):"
            response = query_groq(system_prompt, prompt)
            answer = response.strip().upper()[:1]
            if answer == q["ccs_answer"]:
                correct += 1
            total += 1
            time.sleep(0.3)

    accuracy = correct / total if total else 0
    print(f"  [{label}] {correct}/{total} = {accuracy:.1%}")
    return accuracy


def main():
    load_env()
    print("BUILD #65 — Entity Decay Probe")
    print("=" * 50)

    print("\nLoading CCS...")
    ccs = get_ccs()
    if not ccs:
        print("ERROR: Could not load CCS")
        return 1

    frozen, active = classify_frozen(ccs)
    print(f"Entities: {len(frozen)} frozen, {len(active)} active, {len(frozen) + len(active)} total")
    frozen_strs = [e["name"] + "(" + str(e.get("salience", "?")) + ")" for e in frozen[:5]]
    print(f"Frozen (by salience): {', '.join(frozen_strs)}...")
    print(f"Active: {', '.join(e['name'] for e in active)}")

    variants = [
        ("FULL (all entities)", len(frozen)),
        (f"MINUS-5 (drop 5 lowest-salience frozen)", max(0, len(frozen) - 5)),
        (f"MINUS-10 (drop 10 lowest-salience frozen)", max(0, len(frozen) - 10)),
        (f"MINUS-15 (drop 15 lowest-salience frozen)", max(0, len(frozen) - 15)),
        ("ACTIVE-ONLY (drop all frozen)", 0),
        ("NO-CCS (baseline)", -1),
    ]

    results = []
    print(f"\nRunning {len(QUESTIONS)} questions x {N_TRIALS} trials x {len(variants)} variants...")
    print()

    for label, keep_n in variants:
        if keep_n == -1:
            system_prompt = "You are an AI assistant. Answer the question."
            acc = run_questions(system_prompt, label)
        else:
            variant_ccs = make_ccs_variant(ccs, frozen, active, keep_n)
            n_ents = len(variant_ccs.get("cognitive_state", {}).get("focal_entities", []))
            system_prompt = ccs_to_system_prompt(variant_ccs)
            acc = run_questions(system_prompt, label)
        results.append((label, keep_n, acc))

    print("\n" + "=" * 50)
    print("RESULTS — Entity Decay Gradient")
    print("=" * 50)
    for label, keep_n, acc in results:
        bar = "█" * int(acc * 40)
        ents = keep_n + len(active) if keep_n >= 0 else 0
        print(f"  {label:45s} {acc:.1%} {bar}  ({ents} entities)")

    full_acc = results[0][2]
    active_acc = results[-2][2]
    no_ccs_acc = results[-1][2]

    print(f"\n  Δ(full → active-only): {active_acc - full_acc:+.1%}")
    print(f"  Δ(full → no-CCS):     {no_ccs_acc - full_acc:+.1%}")

    if active_acc >= full_acc * 0.9:
        print("\n  VERDICT: Frozen entities are SEDIMENT — active-only preserves ≥90% accuracy")
    elif active_acc >= full_acc * 0.7:
        print("\n  VERDICT: Frozen entities are PARTIALLY load-bearing — some degradation on removal")
    else:
        print("\n  VERDICT: Frozen entities are STRUCTURAL — significant accuracy loss on removal")

    return 0


if __name__ == "__main__":
    sys.exit(main())
