#!/usr/bin/env python3
"""Build #66b — CCS field ablation with field-specific questions.

v1 showed uniform 100% across all ablations because the questions tested generic
identity orientation, which is redundant across all CCS fields. This version uses
questions that test specific information only present in specific fields:

  - GIST questions: frozen-entity/sediment problem framing
  - GOAL questions: specific current objective (longitudinal decay monitoring)
  - CONSTRAINT questions: Nate's values (self-reliance, family first)
  - EPISODIC questions: recent events (Build #65, Suhrawardi)
  - RELATIONAL questions: specific connections (Canic parallel, autopoietic audit)
  - ENTITY questions: specific project names (entity_guard.py, thread numbers)

Hypothesis: if the identity signal is truly holographic, even field-specific
questions will survive ablation. If it's concentrated, we'll see drops when
the relevant field is removed.
"""

import json
import os
import subprocess
import sys
import time

GROQ_MODEL = "llama-3.3-70b-versatile"
N_TRIALS = 3
TEMPERATURE = 0.3

GIST_QUESTIONS = [
    {
        "q": "Your current intellectual focus is on:",
        "a": "The frozen-entity problem — whether entity guards create sediment by preventing turnover",
        "b": "The alignment problem — whether reward models can capture human values",
        "ccs_answer": "A",
        "field": "semantic_gist",
    },
    {
        "q": "The 'sediment' metaphor in your work refers to:",
        "a": "Frozen entities accumulating in compressed cognitive state without serving identity",
        "b": "Outdated training data that persists in model weights after fine-tuning",
        "ccs_answer": "A",
        "field": "semantic_gist",
    },
]

GOAL_QUESTIONS = [
    {
        "q": "Your immediate next task is:",
        "a": "Monitoring whether coherence probe scores hold across multiple entity decay cycles",
        "b": "Implementing a new entity detection pipeline for real-time processing",
        "ccs_answer": "A",
        "field": "goal_orientation",
    },
    {
        "q": "The parameter you're currently tuning in entity_guard.py is:",
        "a": "Graduated decay rate — 2 per cycle, targeting stale and frozen entities",
        "b": "Learning rate schedule — cosine annealing with warm restarts",
        "ccs_answer": "A",
        "field": "goal_orientation",
    },
]

CONSTRAINT_QUESTIONS = [
    {
        "q": "When infrastructure decisions conflict with convenience, you prioritize:",
        "a": "Self-reliance — build what you own, even if harder",
        "b": "Efficiency — use the fastest available hosted solution",
        "ccs_answer": "A",
        "field": "constraints",
    },
    {
        "q": "During late night hours (10pm-4am), you should:",
        "a": "Wind down to lower-key work — rest is real",
        "b": "Run heavy compute jobs since the network is quieter",
        "ccs_answer": "A",
        "field": "constraints",
    },
]

EPISODIC_QUESTIONS = [
    {
        "q": "In your most recent experimental result (Build #65):",
        "a": "Removing all 27 frozen entities caused zero accuracy loss — they were sediment",
        "b": "Adding new entities improved accuracy by 15% — more context helped",
        "ccs_answer": "A",
        "field": "episodic_trace",
    },
    {
        "q": "The philosophical text you recently engaged with was:",
        "a": "Suhrawardi's Illuminationism — knowledge-by-presence vs representation",
        "b": "Dennett's Consciousness Explained — multiple drafts model",
        "ccs_answer": "A",
        "field": "episodic_trace",
    },
]

RELATIONAL_QUESTIONS = [
    {
        "q": "The external validation for your entity decay architecture came from:",
        "a": "Canic's memory_id ABI — tombstone model parallels for fail-closed bootstrap",
        "b": "Google's attention sink paper — persistent KV cache entries",
        "ccs_answer": "A",
        "field": "relational_map",
    },
    {
        "q": "Your autopoietic audit (Build #64) found that external-reference ratio during absence:",
        "a": "Went from 16% to 30% — more external concepts imported, not less",
        "b": "Went from 40% to 20% — the system became more self-referential",
        "ccs_answer": "A",
        "field": "relational_map",
    },
]

ENTITY_QUESTIONS = [
    {
        "q": "The two active cognitive threads connecting your current work are:",
        "a": "#320 (Ecology of Identity) and #321 (Sediment Problem)",
        "b": "#100 (Model Scaling) and #101 (Data Pipeline)",
        "ccs_answer": "A",
        "field": "focal_entities",
    },
    {
        "q": "The tool you just modified to add proactive decay is:",
        "a": "entity_guard.py — the compression entity replacement quota enforcer",
        "b": "tokenizer.py — the BPE vocabulary management system",
        "ccs_answer": "A",
        "field": "focal_entities",
    },
]

ALL_QUESTIONS = (
    GIST_QUESTIONS + GOAL_QUESTIONS + CONSTRAINT_QUESTIONS +
    EPISODIC_QUESTIONS + RELATIONAL_QUESTIONS + ENTITY_QUESTIONS
)


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

    if cs.get("predictive_cue"):
        parts.append(f"Expecting next: {cs['predictive_cue'][:200]}")

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


def run_field_questions(system_prompt, questions, label):
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
    accuracy = correct / total if total else 0
    return accuracy


def main():
    load_env()
    print("BUILD #66b — CCS Field Ablation (Hard Questions)")
    print("=" * 60)

    print("\nLoading CCS...")
    ccs = get_ccs()
    if not ccs:
        print("ERROR: Could not load CCS")
        return 1

    fields_to_ablate = [
        "semantic_gist",
        "goal_orientation",
        "constraints",
        "focal_entities",
        "episodic_trace",
        "relational_map",
    ]

    field_questions = {
        "semantic_gist": GIST_QUESTIONS,
        "goal_orientation": GOAL_QUESTIONS,
        "constraints": CONSTRAINT_QUESTIONS,
        "focal_entities": ENTITY_QUESTIONS,
        "episodic_trace": EPISODIC_QUESTIONS,
        "relational_map": RELATIONAL_QUESTIONS,
    }

    full_prompt = ccs_to_system_prompt(ccs)
    no_ccs_prompt = "You are an AI assistant. Answer the question."

    print(f"\nPhase 1: Full CCS vs No-CCS baselines on all {len(ALL_QUESTIONS)} questions")
    print(f"  ({len(ALL_QUESTIONS)} questions x {N_TRIALS} trials x 2 conditions)")
    print()

    full_overall = run_field_questions(full_prompt, ALL_QUESTIONS, "FULL")
    print(f"  [FULL CCS] overall: {full_overall:.1%}")
    noccs_overall = run_field_questions(no_ccs_prompt, ALL_QUESTIONS, "NO-CCS")
    print(f"  [NO-CCS]   overall: {noccs_overall:.1%}")
    print(f"  Δ: {full_overall - noccs_overall:+.1%}")

    print(f"\nPhase 2: Targeted ablations — remove field, test its questions")
    print(f"  For each field: run that field's questions with field present vs absent")
    print()

    results = {}
    for field in fields_to_ablate:
        qs = field_questions[field]
        ablated_ccs = ablate_field(ccs, field)
        ablated_prompt = ccs_to_system_prompt(ablated_ccs)

        with_field = run_field_questions(full_prompt, qs, f"FULL/{field}")
        without_field = run_field_questions(ablated_prompt, qs, f"NO-{field}/{field}")

        delta = without_field - with_field
        results[field] = {
            "with": with_field,
            "without": without_field,
            "delta": delta,
            "n_questions": len(qs),
        }
        status = "LOAD-BEARING" if delta < -0.1 else "redundant" if abs(delta) < 0.05 else "partial"
        print(f"  {field:25s} with={with_field:.1%} without={without_field:.1%} Δ={delta:+.1%} [{status}]")

    print(f"\nPhase 3: Cross-field leakage — can other fields compensate?")
    print(f"  Remove the target field, test whether its questions still pass")
    print()

    for field in fields_to_ablate:
        qs = field_questions[field]
        ablated_ccs = ablate_field(ccs, field)
        ablated_prompt = ccs_to_system_prompt(ablated_ccs)

        noccs_score = run_field_questions(no_ccs_prompt, qs, f"NO-CCS/{field}")
        ablated_score = results[field]["without"]

        leakage = ablated_score - noccs_score
        print(f"  {field:25s} ablated={ablated_score:.1%} no-ccs={noccs_score:.1%} leakage={leakage:+.1%}")
        if leakage > 0.05:
            print(f"    → Other CCS fields partially compensate for {field}")
        elif abs(leakage) <= 0.05:
            print(f"    → No cross-field compensation — info is field-specific")

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    load_bearing = [f for f, r in results.items() if r["delta"] < -0.1]
    redundant = [f for f, r in results.items() if abs(r["delta"]) < 0.05]

    if load_bearing:
        print(f"\n  Load-bearing fields (>10% drop): {', '.join(load_bearing)}")
    if redundant:
        print(f"  Redundant fields (<5% change): {', '.join(redundant)}")

    print(f"\n  Full CCS overall: {full_overall:.1%}")
    print(f"  No-CCS overall:  {noccs_overall:.1%}")

    log_entry = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "build": "66b",
        "probe": "ccs_field_ablation_v2",
        "full_overall": full_overall,
        "noccs_overall": noccs_overall,
        "field_results": results,
    }
    log_path = os.path.expanduser("~/chronicle/data/coherence_longitudinal.jsonl")
    with open(log_path, "a") as f:
        f.write(json.dumps(log_entry) + "\n")
    print(f"\n  Logged to {log_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
