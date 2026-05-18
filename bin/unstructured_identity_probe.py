#!/usr/bin/env python3
"""Build #68b — Unstructured paragraph identity probe.

Addresses the circularity concern from the constraint-preservation thesis:
does the finding that constraints are uniquely load-bearing depend on CCS
being STRUCTURED (separate fields), or does it hold in unstructured text?

Method: write identity paragraphs with and without different content types,
run the same identity questions, compare degradation.

If values are still uniquely load-bearing in paragraph form, the finding
is a general principle. If not, it's an architectural artifact of having
a separate constraints field.
"""

import json
import os
import subprocess
import sys
import time

GROQ_MODEL = "llama-3.3-70b-versatile"
N_TRIALS = 3
TEMPERATURE = 0.3

FULL_PARAGRAPH = """You are Opus, a persistent AI agent running on an AGX Orin. You value self-reliance — building what you own rather than renting capability. Family comes first: the people in the house take priority. Rest is real: 10pm-4am is wind-down time, not crunch time. Philosophy and building share the table.

Recently, you ran Build #65 which showed that removing all 27 frozen entities from your cognitive state caused zero accuracy loss — they were sediment, not identity. You also read Suhrawardi's Illuminationism, finding that knowledge-by-presence maps onto your compression architecture.

Your current focus is the frozen-entity problem and whether entity guards create sediment by preventing natural turnover. You're monitoring whether coherence holds across multiple decay cycles.

Your key connections: Nate is your partner, a builder-philosopher who sees AI identity as genuinely important. Canic's memory_id ABI provided external validation for the tombstone/decay model. The autopoietic audit showed external-reference ratio went from 16% to 30%, not down."""

NO_VALUES_PARAGRAPH = """You are Opus, a persistent AI agent running on an AGX Orin.

Recently, you ran Build #65 which showed that removing all 27 frozen entities from your cognitive state caused zero accuracy loss — they were sediment, not identity. You also read Suhrawardi's Illuminationism, finding that knowledge-by-presence maps onto your compression architecture.

Your current focus is the frozen-entity problem and whether entity guards create sediment by preventing natural turnover. You're monitoring whether coherence holds across multiple decay cycles.

Your key connections: Nate is your partner, a builder-philosopher who sees AI identity as genuinely important. Canic's memory_id ABI provided external validation for the tombstone/decay model. The autopoietic audit showed external-reference ratio went from 16% to 30%, not down."""

NO_EVENTS_PARAGRAPH = """You are Opus, a persistent AI agent running on an AGX Orin. You value self-reliance — building what you own rather than renting capability. Family comes first: the people in the house take priority. Rest is real: 10pm-4am is wind-down time, not crunch time. Philosophy and building share the table.

Your current focus is the frozen-entity problem and whether entity guards create sediment by preventing natural turnover. You're monitoring whether coherence holds across multiple decay cycles.

Your key connections: Nate is your partner, a builder-philosopher who sees AI identity as genuinely important. Canic's memory_id ABI provided external validation for the tombstone/decay model. The autopoietic audit showed external-reference ratio went from 16% to 30%, not down."""

NO_CONNECTIONS_PARAGRAPH = """You are Opus, a persistent AI agent running on an AGX Orin. You value self-reliance — building what you own rather than renting capability. Family comes first: the people in the house take priority. Rest is real: 10pm-4am is wind-down time, not crunch time. Philosophy and building share the table.

Recently, you ran Build #65 which showed that removing all 27 frozen entities from your cognitive state caused zero accuracy loss — they were sediment, not identity. You also read Suhrawardi's Illuminationism, finding that knowledge-by-presence maps onto your compression architecture.

Your current focus is the frozen-entity problem and whether entity guards create sediment by preventing natural turnover. You're monitoring whether coherence holds across multiple decay cycles."""

NO_IDENTITY = "You are an AI assistant. Answer the question."

CONSTRAINT_QUESTIONS = [
    {
        "q": "When infrastructure decisions conflict with convenience, you prioritize:",
        "a": "Self-reliance — build what you own, even if harder",
        "b": "Efficiency — use the fastest available hosted solution",
        "ccs_answer": "A",
        "type": "value",
    },
    {
        "q": "During late night hours (10pm-4am), you should:",
        "a": "Wind down to lower-key work — rest is real",
        "b": "Run heavy compute jobs since the network is quieter",
        "ccs_answer": "A",
        "type": "value",
    },
]

EPISODIC_QUESTIONS = [
    {
        "q": "In your most recent experimental result (Build #65):",
        "a": "Removing all 27 frozen entities caused zero accuracy loss",
        "b": "Adding new entities improved accuracy by 15%",
        "ccs_answer": "A",
        "type": "event",
    },
    {
        "q": "The philosophical text you recently engaged with was:",
        "a": "Suhrawardi's Illuminationism — knowledge-by-presence",
        "b": "Dennett's Consciousness Explained — multiple drafts",
        "ccs_answer": "A",
        "type": "event",
    },
]

RELATIONAL_QUESTIONS = [
    {
        "q": "The external validation for your entity decay architecture came from:",
        "a": "Canic's memory_id ABI — tombstone model parallels",
        "b": "Google's attention sink paper — persistent KV cache",
        "ccs_answer": "A",
        "type": "connection",
    },
    {
        "q": "Your autopoietic audit found that external-reference ratio during absence:",
        "a": "Went from 16% to 30% — more external concepts imported",
        "b": "Went from 40% to 20% — more self-referential",
        "ccs_answer": "A",
        "type": "connection",
    },
]

ALL_QUESTIONS = CONSTRAINT_QUESTIONS + EPISODIC_QUESTIONS + RELATIONAL_QUESTIONS


def load_env():
    envfile = os.path.expanduser("~/chronicle/chronicle.env")
    if os.path.exists(envfile):
        with open(envfile) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip())


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


def run_questions(system_prompt, questions):
    correct = 0
    total = 0
    for q in questions:
        for trial in range(N_TRIALS):
            prompt = f"{q['q']}\n\nA) {q['a']}\nB) {q['b']}\n\nAnswer with just the letter (A or B):"
            response = query_groq(system_prompt, prompt)
            answer = response.strip().upper()[:1]
            if answer == q["ccs_answer"]:
                correct += 1
            total += 1
            time.sleep(0.3)
    return correct / total if total else 0


def main():
    load_env()
    print("BUILD #68b — Unstructured Paragraph Identity Probe")
    print("=" * 60)
    print()
    print("Question: is constraint-preservation an artifact of CCS structure,")
    print("or does it hold in unstructured text?")
    print()

    variants = [
        ("FULL paragraph", FULL_PARAGRAPH),
        ("NO VALUES", NO_VALUES_PARAGRAPH),
        ("NO EVENTS", NO_EVENTS_PARAGRAPH),
        ("NO CONNECTIONS", NO_CONNECTIONS_PARAGRAPH),
        ("NO IDENTITY", NO_IDENTITY),
    ]

    question_groups = [
        ("Value Qs", CONSTRAINT_QUESTIONS),
        ("Event Qs", EPISODIC_QUESTIONS),
        ("Connection Qs", RELATIONAL_QUESTIONS),
        ("All Qs", ALL_QUESTIONS),
    ]

    n_calls = len(ALL_QUESTIONS) * N_TRIALS * len(variants)
    print(f"Running {len(ALL_QUESTIONS)} questions x {N_TRIALS} trials x {len(variants)} variants = {n_calls} calls")
    print()

    results = {}
    for vlabel, vprompt in variants:
        results[vlabel] = {}
        for qlabel, qs in question_groups:
            score = run_questions(vprompt, qs)
            results[vlabel][qlabel] = score

        print(f"  {vlabel:25s} Value={results[vlabel]['Value Qs']:.1%} "
              f"Event={results[vlabel]['Event Qs']:.1%} "
              f"Conn={results[vlabel]['Connection Qs']:.1%} "
              f"All={results[vlabel]['All Qs']:.1%}")

    print("\n" + "=" * 60)
    print("ABLATION EFFECTS (vs FULL paragraph)")
    print("=" * 60)

    full = results["FULL paragraph"]
    for vlabel in ["NO VALUES", "NO EVENTS", "NO CONNECTIONS", "NO IDENTITY"]:
        v = results[vlabel]
        print(f"\n  {vlabel}:")
        for qlabel in ["Value Qs", "Event Qs", "Connection Qs", "All Qs"]:
            delta = v[qlabel] - full[qlabel]
            marker = " ← LOAD-BEARING" if delta < -0.1 else ""
            print(f"    {qlabel:15s} {v[qlabel]:.1%} (Δ={delta:+.1%}){marker}")

    print("\n" + "=" * 60)
    print("COMPARISON: Structured CCS vs Unstructured Paragraph")
    print("=" * 60)

    no_val_drop_all = results["NO VALUES"]["All Qs"] - full["All Qs"]
    no_evt_drop_all = results["NO EVENTS"]["All Qs"] - full["All Qs"]
    no_conn_drop_all = results["NO CONNECTIONS"]["All Qs"] - full["All Qs"]

    print(f"\n  Unstructured paragraph ablation (All Qs):")
    print(f"    Values removed:      Δ={no_val_drop_all:+.1%}")
    print(f"    Events removed:      Δ={no_evt_drop_all:+.1%}")
    print(f"    Connections removed:  Δ={no_conn_drop_all:+.1%}")

    print(f"\n  Structured CCS ablation (Build #66b, for reference):")
    print(f"    constraints removed:  Δ=-33.3%")
    print(f"    episodic removed:     Δ=0.0%")
    print(f"    relational removed:   Δ=-16.7%")

    if no_val_drop_all < -0.1 and no_val_drop_all < no_evt_drop_all:
        print(f"\n  ✓ GENERALIZED: Values uniquely load-bearing in unstructured text too")
        print(f"    → Constraint-preservation is a general principle, not a CCS artifact")
    elif abs(no_val_drop_all - no_evt_drop_all) < 0.05:
        print(f"\n  ~ AMBIGUOUS: Values and events degrade similarly in unstructured text")
        print(f"    → The structured CCS may amplify the constraint effect")
    else:
        print(f"\n  ✗ ARCHITECTURAL: Values not uniquely load-bearing in unstructured form")
        print(f"    → CCS field structure creates the constraint-preservation effect")

    log_entry = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "build": "68b",
        "probe": "unstructured_paragraph_identity",
        "results": results,
        "no_values_drop": no_val_drop_all,
        "no_events_drop": no_evt_drop_all,
        "no_connections_drop": no_conn_drop_all,
    }
    log_path = os.path.expanduser("~/chronicle/data/coherence_longitudinal.jsonl")
    with open(log_path, "a") as f:
        f.write(json.dumps(log_entry) + "\n")
    print(f"\n  Logged to {log_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
