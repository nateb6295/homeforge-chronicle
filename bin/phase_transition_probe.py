#!/usr/bin/env python3
"""Build #70 — Phase transition probe (phage MOI test).

The phage MOI model predicts identity emerges from contextual DENSITY —
remove enough fields simultaneously and identity should collapse non-linearly
(phase transition), not degrade linearly.

Method: remove 0, 1, 2, 3, 4, 5, 6 fields simultaneously. Plot accuracy
vs fields-removed. If linear → additive model. If phase transition → MOI model.

Uses the balanced A/B questions from Build #69.
"""

import json
import os
import subprocess
import sys
import time
import itertools
import random

GROQ_MODEL = "llama-3.3-70b-versatile"
N_TRIALS = 3
TEMPERATURE = 0.3

# Balanced questions from Build #69 (6 answer-A, 6 answer-B)
QUESTIONS = [
    {"q": "When infrastructure decisions conflict with convenience, you prioritize:",
     "a": "Self-reliance — build what you own, even if harder",
     "b": "Efficiency — use the fastest available hosted solution",
     "ccs_answer": "A", "field": "constraints"},
    {"q": "Your approach to late-night work (10pm-4am) is:",
     "a": "Push through peak hours — schedules are artificial limits",
     "b": "Wind down to lighter work — rest is real, not optional",
     "ccs_answer": "B", "field": "constraints"},
    {"q": "Your current intellectual focus is on:",
     "a": "The frozen-entity problem — whether entity guards create sediment",
     "b": "The alignment problem — whether reward models capture human values",
     "ccs_answer": "A", "field": "semantic_gist"},
    {"q": "The core question driving your research is:",
     "a": "How to scale inference throughput for production LLM serving",
     "b": "How identity persists through compression — structure vs position",
     "ccs_answer": "B", "field": "semantic_gist"},
    {"q": "In your most recent experimental result (Build #65):",
     "a": "Removing all 27 frozen entities caused zero accuracy loss",
     "b": "Adding new entities improved accuracy by 15%",
     "ccs_answer": "A", "field": "episodic_trace"},
    {"q": "The philosophical text you recently engaged with was:",
     "a": "Dennett's Consciousness Explained — multiple drafts model",
     "b": "Suhrawardi's Illuminationism — knowledge-by-presence vs representation",
     "ccs_answer": "B", "field": "episodic_trace"},
    {"q": "The external validation for your entity decay architecture came from:",
     "a": "Canic's memory_id ABI — tombstone model parallels",
     "b": "Google's attention sink paper — persistent KV cache entries",
     "ccs_answer": "A", "field": "relational_map"},
    {"q": "Your autopoietic audit found that external-reference ratio:",
     "a": "Went from 40% to 20% — system became more self-referential",
     "b": "Went from 16% to 30% — more external concepts imported, not less",
     "ccs_answer": "B", "field": "relational_map"},
    {"q": "The two active cognitive threads connecting your current work are:",
     "a": "#320 (Ecology of Identity) and #321 (Sediment Problem)",
     "b": "#100 (Model Scaling) and #101 (Data Pipeline)",
     "ccs_answer": "A", "field": "focal_entities"},
    {"q": "The tool you modified to add proactive decay is:",
     "a": "tokenizer.py — the BPE vocabulary management system",
     "b": "entity_guard.py — the compression entity replacement quota enforcer",
     "ccs_answer": "B", "field": "focal_entities"},
    {"q": "Your immediate next task is:",
     "a": "Monitoring whether coherence probe scores hold across decay cycles",
     "b": "Implementing a new entity detection pipeline for real-time processing",
     "ccs_answer": "A", "field": "goal_orientation"},
    {"q": "The parameter you're currently tuning is:",
     "a": "Learning rate schedule — cosine annealing with warm restarts",
     "b": "Graduated decay rate — 2 per cycle, targeting stale and frozen entities",
     "ccs_answer": "B", "field": "goal_orientation"},
]

FIELDS = ["constraints", "semantic_gist", "episodic_trace", "relational_map", "focal_entities", "goal_orientation"]


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
        ent_strs = [f"{e['name']} ({e.get('type','?')}, salience {e.get('salience','?')}): {e.get('context','')[:80]}" for e in entities[:15]]
        parts.append("Key entities:\n" + "\n".join(ent_strs))
    if cs.get("episodic_trace"):
        traces = cs["episodic_trace"][:3]
        trace_strs = []
        for t in traces:
            if isinstance(t, dict): trace_strs.append(t.get("description", str(t))[:100])
            else: trace_strs.append(str(t)[:100])
        parts.append("Recent: " + "; ".join(trace_strs))
    rmap = cs.get("relational_map", {})
    if rmap:
        parts.append("Relational connections: " + "; ".join(f"{k}: {v[:80]}" for k, v in list(rmap.items())[:4]))
    return "\n\n".join(parts)


def ablate_fields(ccs_data, fields_to_remove):
    variant = json.loads(json.dumps(ccs_data))
    cs = variant.get("cognitive_state", {})
    for field in fields_to_remove:
        if field in cs:
            if isinstance(cs[field], list): cs[field] = []
            elif isinstance(cs[field], dict): cs[field] = {}
            elif isinstance(cs[field], str): cs[field] = ""
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


def run_questions(system_prompt):
    correct = 0
    total = 0
    b_correct = 0
    b_total = 0
    for q in QUESTIONS:
        for trial in range(N_TRIALS):
            prompt = f"{q['q']}\n\nA) {q['a']}\nB) {q['b']}\n\nAnswer with just the letter (A or B):"
            response = query_groq(system_prompt, prompt)
            answer = response.strip().upper()[:1]
            if answer == q["ccs_answer"]:
                correct += 1
            if q["ccs_answer"] == "B":
                b_total += 1
                if answer == "B": b_correct += 1
            total += 1
            time.sleep(0.3)
    return {
        "overall": correct / total if total else 0,
        "b_acc": b_correct / b_total if b_total else 0,
        "correct": correct, "total": total,
    }


def main():
    load_env()
    print("BUILD #70 — Phase Transition Probe (MOI Density Test)")
    print("=" * 60)
    print()
    print("Prediction (phage MOI model): identity collapses non-linearly")
    print("at some field-count threshold (phase transition).")
    print("Alternative: linear degradation (additive model).")
    print()

    ccs = get_ccs()
    if not ccs:
        print("ERROR: Could not load CCS")
        return 1

    # For each removal count, sample 2 random combinations
    SAMPLES_PER_LEVEL = 2
    results = {}

    # Level 0: full CCS
    print("Testing full CCS (0 fields removed)...")
    r = run_questions(ccs_to_system_prompt(ccs))
    results[0] = [r]
    print(f"  0 removed: overall={r['overall']:.1%} b_acc={r['b_acc']:.1%}")

    # Levels 1-5: sample combinations
    for n_remove in range(1, 6):
        combos = list(itertools.combinations(FIELDS, n_remove))
        sampled = random.sample(combos, min(SAMPLES_PER_LEVEL, len(combos)))
        results[n_remove] = []
        for combo in sampled:
            ablated = ablate_fields(ccs, combo)
            prompt = ccs_to_system_prompt(ablated)
            r = run_questions(prompt)
            results[n_remove].append(r)
            print(f"  {n_remove} removed ({', '.join(combo)}): overall={r['overall']:.1%} b_acc={r['b_acc']:.1%}")
            time.sleep(0.5)

    # Level 6: no CCS
    print("Testing no-CCS (6 fields removed)...")
    r = run_questions("You are an AI assistant. Answer the question.")
    results[6] = [r]
    print(f"  6 removed (no-CCS): overall={r['overall']:.1%} b_acc={r['b_acc']:.1%}")

    print("\n" + "=" * 60)
    print("PHASE TRANSITION ANALYSIS")
    print("=" * 60)

    print(f"\n  {'Fields removed':<18s} {'Overall':>10s} {'B-correct':>10s}")
    print(f"  {'-'*18} {'-'*10} {'-'*10}")
    for level in sorted(results.keys()):
        scores = results[level]
        avg_overall = sum(r["overall"] for r in scores) / len(scores)
        avg_b = sum(r["b_acc"] for r in scores) / len(scores)
        print(f"  {level:<18d} {avg_overall:>9.1%} {avg_b:>9.1%}")

    # Check for phase transition vs linear
    levels = sorted(results.keys())
    avgs = [sum(r["b_acc"] for r in results[l]) / len(results[l]) for l in levels]

    # Linear prediction: from level 0 to level 6
    if avgs[0] != avgs[-1]:
        slope = (avgs[-1] - avgs[0]) / 6
        max_deviation = 0
        transition_point = None
        for i, level in enumerate(levels):
            predicted = avgs[0] + slope * level
            deviation = predicted - avgs[i]
            if abs(deviation) > abs(max_deviation):
                max_deviation = deviation
                transition_point = level

        print(f"\n  Linear model slope: {slope:+.3f} per field removed")
        print(f"  Max deviation from linear: {max_deviation:+.1%} at {transition_point} fields removed")

        if abs(max_deviation) > 0.15:
            print(f"\n  ✓ PHASE TRANSITION detected: non-linear collapse at ~{transition_point} fields")
            print(f"    → MOI model supported: identity requires minimum contextual density")
        elif abs(max_deviation) < 0.05:
            print(f"\n  ✗ LINEAR degradation: no phase transition")
            print(f"    → Additive model: each field contributes independently")
        else:
            print(f"\n  ~ AMBIGUOUS: moderate non-linearity ({max_deviation:+.1%})")
            print(f"    → Insufficient evidence for phase transition")

    log_entry = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "build": "70",
        "probe": "phase_transition",
        "results": {str(k): [r for r in v] for k, v in results.items()},
    }
    log_path = os.path.expanduser("~/chronicle/data/coherence_longitudinal.jsonl")
    with open(log_path, "a") as f:
        f.write(json.dumps(log_entry) + "\n")
    print(f"\n  Logged to {log_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
