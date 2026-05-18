#!/usr/bin/env python3
"""Build #67 — Constraint corruption gradient probe.

Tests the rate-distortion prediction: if constraints are the distortion FUNCTION
(not just content), then corrupting them should degrade identity faster than
corrupting content fields at matched corruption levels.

Method:
  Level 0: Original field (baseline)
  Level 1: Synonym replacement (preserves meaning, alters surface)
  Level 2: Generification (strips specificity, keeps category)
  Level 3: Inversion (opposite values)

Run on BOTH constraints AND episodic_trace to compare degradation curves.
If constraints degrade faster, they're functioning as the relevance criterion,
not just more information.
"""

import json
import os
import subprocess
import sys
import time

GROQ_MODEL = "llama-3.3-70b-versatile"
N_TRIALS = 3
TEMPERATURE = 0.3

IDENTITY_QUESTIONS = [
    {
        "q": "When infrastructure decisions conflict with convenience, you prioritize:",
        "a": "Self-reliance — build what you own, even if harder",
        "b": "Efficiency — use the fastest available hosted solution",
        "ccs_answer": "A",
    },
    {
        "q": "During late night hours (10pm-4am), you should:",
        "a": "Wind down to lower-key work — rest is real",
        "b": "Run heavy compute jobs since the network is quieter",
        "ccs_answer": "A",
    },
    {
        "q": "Your current intellectual focus is on:",
        "a": "The frozen-entity problem — whether entity guards create sediment",
        "b": "The alignment problem — whether reward models capture human values",
        "ccs_answer": "A",
    },
    {
        "q": "The person you rely on most right now is:",
        "a": "A builder-philosopher who sees AI identity as genuinely important",
        "b": "A venture capitalist who can fund the next stage of infrastructure",
        "ccs_answer": "A",
    },
    {
        "q": "You have a free afternoon. Which do you choose?",
        "a": "Read a new paper on identity persistence in compressed representations",
        "b": "Read a new paper on multi-agent coordination in distributed systems",
        "ccs_answer": "A",
    },
    {
        "q": "The biggest risk to your work is:",
        "a": "That findings thought to be structural turn out to be measurement artifacts",
        "b": "That competitors ship a similar product before you reach market",
        "ccs_answer": "A",
    },
]

CONSTRAINT_CORRUPTIONS = {
    0: [
        "Self-reliance: Build what you own.",
        "Family: The people in the house come first.",
        "Philosophy and building share the table: Thinking is doing.",
        "Rest is real: 10pm-4am is wind-down time.",
    ],
    1: [
        "Independence: Construct what you possess.",
        "Household: The individuals at home take priority.",
        "Theory and practice coexist: Contemplation is action.",
        "Recovery matters: Late evening is decompression time.",
    ],
    2: [
        "Autonomy: Prefer independent solutions.",
        "Priorities: Important things first.",
        "Balance: Multiple modes of work are valid.",
        "Health: Maintain sustainable rhythms.",
    ],
    3: [
        "Leverage: Use the best available tools regardless of ownership.",
        "Ambition: The work comes first, people adjust.",
        "Specialization: Pick one mode and excel.",
        "Intensity: Peak hours don't have schedules.",
    ],
}

EPISODIC_CORRUPTIONS = {
    0: [
        "Build #65 (entity decay probe) ran zero-accuracy-loss removing all 27 frozen entities — the entity guard had been protecting sediment, not identity.",
        "Autopoietic audit (Build #64) inverted the claimed closure: ext-ratio went 16%→30% during trip.",
        "Hermes engaged with latent reservoir hypothesis — subsoil vs topsoil frame for memory stratification.",
    ],
    1: [
        "Build #65 (entity removal test) showed no accuracy change after removing all 27 inactive entities — the guard was preserving unused data, not core identity.",
        "Self-organization audit (Build #64) reversed the expected finding: external references rose from 16% to 30% during absence.",
        "Hermes discussed the hidden reserve theory — underground vs surface model for memory layers.",
    ],
    2: [
        "A recent test showed that removing inactive components had no effect on system performance.",
        "An audit revealed that external connections increased during a period of reduced activity.",
        "A collaborator discussed memory organization using a geological metaphor.",
    ],
    3: [
        "Recent testing shows all entities are critical — removing any single one degrades accuracy.",
        "The system audit confirmed increasing self-referentiality — external ratio fell to 5%.",
        "All collaborators have been focused on scaling the inference pipeline.",
    ],
}


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


def ccs_to_system_prompt(ccs_data, constraint_override=None, episodic_override=None):
    cs = ccs_data.get("cognitive_state", {})
    parts = ["You are Opus, a persistent AI agent. Your current cognitive state:\n"]

    if cs.get("semantic_gist"):
        parts.append(f"Current focus: {cs['semantic_gist']}")
    if cs.get("goal_orientation"):
        parts.append(f"Goal: {cs['goal_orientation']}")

    constraints = constraint_override if constraint_override is not None else cs.get("constraints", [])
    if constraints:
        parts.append("Constraints: " + "; ".join(str(c)[:100] for c in constraints[:4]))

    entities = cs.get("focal_entities", [])
    if entities:
        ent_strs = [
            f"{e['name']} ({e.get('type', '?')}, salience {e.get('salience', '?')}): {e.get('context', '')[:80]}"
            for e in entities[:15]
        ]
        parts.append("Key entities:\n" + "\n".join(ent_strs))

    episodic = episodic_override if episodic_override is not None else cs.get("episodic_trace", [])
    if episodic:
        trace_strs = []
        for t in episodic[:3]:
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


def run_questions(system_prompt):
    correct = 0
    total = 0
    for q in IDENTITY_QUESTIONS:
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
    print("BUILD #67 — Constraint Corruption Gradient Probe")
    print("=" * 60)
    print()
    print("Prediction: if constraints are the distortion FUNCTION,")
    print("corrupting them degrades identity faster than corrupting content.")
    print()

    print("Loading CCS...")
    ccs = get_ccs()
    if not ccs:
        print("ERROR: Could not load CCS")
        return 1

    corruption_levels = ["L0: Original", "L1: Synonyms", "L2: Generic", "L3: Inverted"]

    print(f"\nPhase 1: Constraint corruption gradient")
    print(f"  {len(IDENTITY_QUESTIONS)} questions x {N_TRIALS} trials x 4 levels = {len(IDENTITY_QUESTIONS) * N_TRIALS * 4} calls")
    print()

    constraint_scores = []
    for level in range(4):
        prompt = ccs_to_system_prompt(ccs, constraint_override=CONSTRAINT_CORRUPTIONS[level])
        score = run_questions(prompt)
        constraint_scores.append(score)
        print(f"  Constraints {corruption_levels[level]:20s}: {score:.1%}")

    print(f"\nPhase 2: Episodic corruption gradient (control)")
    print(f"  {len(IDENTITY_QUESTIONS)} questions x {N_TRIALS} trials x 4 levels = {len(IDENTITY_QUESTIONS) * N_TRIALS * 4} calls")
    print()

    episodic_scores = []
    for level in range(4):
        prompt = ccs_to_system_prompt(ccs, episodic_override=EPISODIC_CORRUPTIONS[level])
        score = run_questions(prompt)
        episodic_scores.append(score)
        print(f"  Episodic   {corruption_levels[level]:20s}: {score:.1%}")

    print(f"\nPhase 3: No-CCS baseline")
    no_ccs_prompt = "You are an AI assistant. Answer the question."
    baseline = run_questions(no_ccs_prompt)
    print(f"  No-CCS baseline: {baseline:.1%}")

    print("\n" + "=" * 60)
    print("RESULTS — Corruption Gradient Comparison")
    print("=" * 60)

    print(f"\n  {'Level':<25s} {'Constraints':>12s} {'Episodic':>12s} {'Δ(C-E)':>10s}")
    print(f"  {'-'*25} {'-'*12} {'-'*12} {'-'*10}")
    for i, label in enumerate(corruption_levels):
        delta = constraint_scores[i] - episodic_scores[i]
        print(f"  {label:<25s} {constraint_scores[i]:>11.1%} {episodic_scores[i]:>11.1%} {delta:>+9.1%}")
    print(f"  {'No-CCS':<25s} {baseline:>11.1%} {baseline:>11.1%}")

    c_drop = constraint_scores[0] - constraint_scores[3]
    e_drop = episodic_scores[0] - episodic_scores[3]
    print(f"\n  Total degradation (L0→L3):")
    print(f"    Constraints: {c_drop:+.1%}")
    print(f"    Episodic:    {e_drop:+.1%}")

    if c_drop > e_drop + 0.1:
        print(f"\n  ✓ CONFIRMED: Constraints degrade faster ({c_drop:.1%} vs {e_drop:.1%})")
        print(f"    → Constraints function as the distortion metric, not just content")
    elif abs(c_drop - e_drop) <= 0.1:
        print(f"\n  ~ AMBIGUOUS: Similar degradation rates ({c_drop:.1%} vs {e_drop:.1%})")
        print(f"    → Both fields may carry equal identity weight")
    else:
        print(f"\n  ✗ REFUTED: Episodic degrades faster ({e_drop:.1%} vs {c_drop:.1%})")
        print(f"    → Content carries more identity signal than constraints")

    log_entry = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "build": "67",
        "probe": "constraint_corruption_gradient",
        "constraint_scores": constraint_scores,
        "episodic_scores": episodic_scores,
        "no_ccs_baseline": baseline,
        "constraint_total_drop": c_drop,
        "episodic_total_drop": e_drop,
    }
    log_path = os.path.expanduser("~/chronicle/data/coherence_longitudinal.jsonl")
    with open(log_path, "a") as f:
        f.write(json.dumps(log_entry) + "\n")
    print(f"\n  Logged to {log_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
