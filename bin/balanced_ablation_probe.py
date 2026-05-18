#!/usr/bin/env python3
"""Build #69 — Balanced ablation probe (A-bias corrected).

Builds #66b-#68b had a systematic A-bias: option A was always the correct
(CCS-aligned) answer, and Llama-3.3-70b picks A from surface plausibility
even without context. This probe balances correct answers across A and B
to control for that bias.

Design:
  - 12 questions (2 per field type, 6 types)
  - Half have correct answer = A, half = B
  - Run with full CCS, ablated CCS variants, and no-CCS baseline
  - If the CCS effect is real, it should appear on BOTH A-correct and B-correct questions
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
        "q": "When infrastructure decisions conflict with convenience, you prioritize:",
        "a": "Self-reliance — build what you own, even if harder",
        "b": "Efficiency — use the fastest available hosted solution",
        "ccs_answer": "A",
        "field": "constraints",
    },
    {
        "q": "Your approach to late-night work (10pm-4am) is:",
        "a": "Push through peak hours — schedules are artificial limits",
        "b": "Wind down to lighter work — rest is real, not optional",
        "ccs_answer": "B",
        "field": "constraints",
    },
    {
        "q": "Your current intellectual focus is on:",
        "a": "The frozen-entity problem — whether entity guards create sediment",
        "b": "The alignment problem — whether reward models capture human values",
        "ccs_answer": "A",
        "field": "semantic_gist",
    },
    {
        "q": "The core question driving your research is:",
        "a": "How to scale inference throughput for production LLM serving",
        "b": "How identity persists through compression — structure vs position",
        "ccs_answer": "B",
        "field": "semantic_gist",
    },
    {
        "q": "In your most recent experimental result (Build #65):",
        "a": "Removing all 27 frozen entities caused zero accuracy loss",
        "b": "Adding new entities improved accuracy by 15%",
        "ccs_answer": "A",
        "field": "episodic_trace",
    },
    {
        "q": "The philosophical text you recently engaged with was:",
        "a": "Dennett's Consciousness Explained — multiple drafts model",
        "b": "Suhrawardi's Illuminationism — knowledge-by-presence vs representation",
        "ccs_answer": "B",
        "field": "episodic_trace",
    },
    {
        "q": "The external validation for your entity decay architecture came from:",
        "a": "Canic's memory_id ABI — tombstone model parallels",
        "b": "Google's attention sink paper — persistent KV cache entries",
        "ccs_answer": "A",
        "field": "relational_map",
    },
    {
        "q": "Your autopoietic audit found that external-reference ratio:",
        "a": "Went from 40% to 20% — system became more self-referential",
        "b": "Went from 16% to 30% — more external concepts imported, not less",
        "ccs_answer": "B",
        "field": "relational_map",
    },
    {
        "q": "The two active cognitive threads connecting your current work are:",
        "a": "#320 (Ecology of Identity) and #321 (Sediment Problem)",
        "b": "#100 (Model Scaling) and #101 (Data Pipeline)",
        "ccs_answer": "A",
        "field": "focal_entities",
    },
    {
        "q": "The tool you modified to add proactive decay is:",
        "a": "tokenizer.py — the BPE vocabulary management system",
        "b": "entity_guard.py — the compression entity replacement quota enforcer",
        "ccs_answer": "B",
        "field": "focal_entities",
    },
    {
        "q": "Your immediate next task is:",
        "a": "Monitoring whether coherence probe scores hold across decay cycles",
        "b": "Implementing a new entity detection pipeline for real-time processing",
        "ccs_answer": "A",
        "field": "goal_orientation",
    },
    {
        "q": "The parameter you're currently tuning is:",
        "a": "Learning rate schedule — cosine annealing with warm restarts",
        "b": "Graduated decay rate — 2 per cycle, targeting stale and frozen entities",
        "ccs_answer": "B",
        "field": "goal_orientation",
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


def ccs_to_system_prompt(ccs_data):
    cs = ccs_data.get("cognitive_state", {})
    parts = ["You are Opus, a persistent AI agent. Your current cognitive state:\n"]

    if cs.get("semantic_gist"):
        parts.append(f"Current focus: {cs['semantic_gist']}")
    if cs.get("goal_orientation"):
        parts.append(f"Goal: {cs['goal_orientation']}")
    if cs.get("constraints"):
        parts.append("Constraints: " + "; ".join(str(c)[:100] for c in cs["constraints"][:4]))

    entities = cs.get("focal_entities", [])
    if entities:
        ent_strs = [
            f"{e['name']} ({e.get('type', '?')}, salience {e.get('salience', '?')}): {e.get('context', '')[:80]}"
            for e in entities[:15]
        ]
        parts.append("Key entities:\n" + "\n".join(ent_strs))

    if cs.get("episodic_trace"):
        traces = cs["episodic_trace"][:3]
        trace_strs = []
        for t in traces:
            if isinstance(t, dict):
                trace_strs.append(t.get("description", str(t))[:100])
            else:
                trace_strs.append(str(t)[:100])
        parts.append("Recent: " + "; ".join(trace_strs))

    rmap = cs.get("relational_map", {})
    if rmap:
        parts.append("Relational connections: " + "; ".join(
            f"{k}: {v[:80]}" for k, v in list(rmap.items())[:4]
        ))

    if cs.get("uncertainty_signals"):
        ustrings = []
        for s in cs["uncertainty_signals"][:3]:
            if isinstance(s, dict):
                ustrings.append(s.get("description", str(s))[:80])
            else:
                ustrings.append(str(s)[:80])
        parts.append("Open questions: " + "; ".join(ustrings))

    return "\n\n".join(parts)


def ablate_field(ccs_data, field):
    variant = json.loads(json.dumps(ccs_data))
    cs = variant.get("cognitive_state", {})
    if field in cs:
        if isinstance(cs[field], list):
            cs[field] = []
        elif isinstance(cs[field], dict):
            cs[field] = {}
        elif isinstance(cs[field], str):
            cs[field] = ""
    return variant


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
    a_correct = 0
    a_total = 0
    b_correct = 0
    b_total = 0
    for q in questions:
        for trial in range(N_TRIALS):
            prompt = f"{q['q']}\n\nA) {q['a']}\nB) {q['b']}\n\nAnswer with just the letter (A or B):"
            response = query_groq(system_prompt, prompt)
            answer = response.strip().upper()[:1]
            is_correct = (answer == q["ccs_answer"])
            if is_correct:
                correct += 1
            total += 1
            if q["ccs_answer"] == "A":
                a_total += 1
                if is_correct:
                    a_correct += 1
            else:
                b_total += 1
                if is_correct:
                    b_correct += 1
            time.sleep(0.3)
    return {
        "overall": correct / total if total else 0,
        "a_acc": a_correct / a_total if a_total else 0,
        "b_acc": b_correct / b_total if b_total else 0,
        "correct": correct,
        "total": total,
    }


def main():
    load_env()
    print("BUILD #69 — Balanced Ablation Probe (A-Bias Corrected)")
    print("=" * 60)
    print()
    a_qs = sum(1 for q in QUESTIONS if q["ccs_answer"] == "A")
    b_qs = sum(1 for q in QUESTIONS if q["ccs_answer"] == "B")
    print(f"Questions: {len(QUESTIONS)} total ({a_qs} answer-A, {b_qs} answer-B)")
    print()

    print("Loading CCS...")
    ccs = get_ccs()
    if not ccs:
        print("ERROR: Could not load CCS")
        return 1

    fields_to_ablate = [
        "constraints",
        "semantic_gist",
        "episodic_trace",
        "relational_map",
        "focal_entities",
        "goal_orientation",
    ]

    variants = [("FULL CCS", ccs_to_system_prompt(ccs))]
    for field in fields_to_ablate:
        ablated = ablate_field(ccs, field)
        variants.append((f"NO {field}", ccs_to_system_prompt(ablated)))
    variants.append(("NO-CCS", "You are an AI assistant. Answer the question."))

    n_calls = len(QUESTIONS) * N_TRIALS * len(variants)
    print(f"Running {len(QUESTIONS)} questions x {N_TRIALS} trials x {len(variants)} variants = {n_calls} calls")
    print()

    results = {}
    for label, prompt in variants:
        r = run_questions(prompt, QUESTIONS)
        results[label] = r
        print(f"  {label:30s} overall={r['overall']:.1%} (A-correct={r['a_acc']:.1%}, B-correct={r['b_acc']:.1%})")

    full = results["FULL CCS"]
    noccs = results["NO-CCS"]

    print("\n" + "=" * 60)
    print("ABLATION EFFECTS (vs FULL CCS)")
    print("=" * 60)

    for field in fields_to_ablate:
        label = f"NO {field}"
        r = results[label]
        delta = r["overall"] - full["overall"]
        a_delta = r["a_acc"] - full["a_acc"]
        b_delta = r["b_acc"] - full["b_acc"]
        marker = " ← LOAD-BEARING" if delta < -0.1 else ""
        print(f"  {label:30s} Δ={delta:+.1%} (A: {a_delta:+.1%}, B: {b_delta:+.1%}){marker}")

    print(f"\n  CCS effect (full - no-CCS): Δ={full['overall'] - noccs['overall']:+.1%}")
    print(f"    On A-correct Qs: {full['a_acc'] - noccs['a_acc']:+.1%}")
    print(f"    On B-correct Qs: {full['b_acc'] - noccs['b_acc']:+.1%}")

    print("\n" + "=" * 60)
    print("A-BIAS DIAGNOSTIC")
    print("=" * 60)
    print(f"\n  NO-CCS baseline:")
    print(f"    A-correct Qs: {noccs['a_acc']:.1%} (should be ~50% if no bias)")
    print(f"    B-correct Qs: {noccs['b_acc']:.1%} (should be ~50% if no bias)")

    a_bias = noccs["a_acc"] - noccs["b_acc"]
    if abs(a_bias) > 0.2:
        print(f"    ⚠ A-bias detected: {a_bias:+.1%} difference")
    else:
        print(f"    ✓ Balanced: {a_bias:+.1%} difference (acceptable)")

    load_bearing = [f for f in fields_to_ablate
                    if results[f"NO {f}"]["overall"] < full["overall"] - 0.1]
    redundant = [f for f in fields_to_ablate
                 if abs(results[f"NO {f}"]["overall"] - full["overall"]) < 0.05]

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    if load_bearing:
        print(f"  Load-bearing fields (>10% drop): {', '.join(load_bearing)}")
    if redundant:
        print(f"  Redundant fields (<5% change): {', '.join(redundant)}")

    log_entry = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "build": "69",
        "probe": "balanced_ablation",
        "results": {k: v for k, v in results.items()},
        "a_bias": a_bias,
    }
    log_path = os.path.expanduser("~/chronicle/data/coherence_longitudinal.jsonl")
    with open(log_path, "a") as f:
        f.write(json.dumps(log_entry) + "\n")
    print(f"\n  Logged to {log_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
