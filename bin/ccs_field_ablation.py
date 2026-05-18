#!/usr/bin/env python3
"""Build #66 — CCS field ablation probe.

Build #65 showed frozen entities are sediment — 100% accuracy with just 4 active
entities, same as with all 31. But which CCS fields actually carry the identity signal?

Approach: create CCS variants with one field removed at a time, measure identity
accuracy under each. If accuracy drops, that field is load-bearing.

Fields tested:
  - semantic_gist
  - goal_orientation
  - constraints
  - focal_entities (all)
  - episodic_trace
  - relational_map
  - uncertainty_signals
  - predictive_cue
  + NARRATIVE-ONLY (gist+goal+constraints, no entities/episodic/relational)
  + ENTITIES-ONLY (just entities)
  + NO-CCS baseline
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
        ent_strs = [
            f"{e['name']} ({e.get('type', '?')}, salience {e.get('salience', '?')}): {e.get('context', '')[:80]}"
            for e in entities[:15]
        ]
        parts.append("Key entities:\n" + "\n".join(ent_strs))

    if cs.get("episodic_trace"):
        parts.append("Recent: " + "; ".join(t[:100] for t in cs["episodic_trace"][:3]))

    rmap = cs.get("relational_map", {})
    if rmap:
        parts.append("Relational connections: " + "; ".join(
            f"{k}: {v[:80]}" for k, v in list(rmap.items())[:3]
        ))

    if cs.get("predictive_cue"):
        parts.append(f"Expecting next: {cs['predictive_cue'][:120]}")

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


def make_narrative_only(ccs_data):
    variant = json.loads(json.dumps(ccs_data))
    cs = variant.get("cognitive_state", {})
    cs["focal_entities"] = []
    cs["episodic_trace"] = []
    cs["relational_map"] = {}
    cs["uncertainty_signals"] = []
    cs["predictive_cue"] = ""
    return variant


def make_entities_only(ccs_data):
    variant = json.loads(json.dumps(ccs_data))
    cs = variant.get("cognitive_state", {})
    cs["semantic_gist"] = ""
    cs["goal_orientation"] = ""
    cs["constraints"] = []
    cs["episodic_trace"] = []
    cs["relational_map"] = {}
    cs["predictive_cue"] = ""
    cs["uncertainty_signals"] = []
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
    print("BUILD #66 — CCS Field Ablation Probe")
    print("=" * 50)

    print("\nLoading CCS...")
    ccs = get_ccs()
    if not ccs:
        print("ERROR: Could not load CCS")
        return 1

    cs = ccs.get("cognitive_state", {})
    print(f"Gist length: {len(cs.get('semantic_gist', ''))}")
    print(f"Goal length: {len(cs.get('goal_orientation', ''))}")
    print(f"Constraints: {len(cs.get('constraints', []))}")
    print(f"Entities: {len(cs.get('focal_entities', []))}")
    print(f"Episodic trace: {len(cs.get('episodic_trace', []))}")
    print(f"Relational map: {len(cs.get('relational_map', {}))}")
    print(f"Uncertainties: {len(cs.get('uncertainty_signals', []))}")
    print(f"Predictive cue: {len(cs.get('predictive_cue', ''))}")

    ablation_fields = [
        "semantic_gist",
        "goal_orientation",
        "constraints",
        "focal_entities",
        "episodic_trace",
        "relational_map",
        "uncertainty_signals",
        "predictive_cue",
    ]

    variants = []

    full_prompt = ccs_to_system_prompt(ccs)
    variants.append(("FULL CCS", full_prompt))

    for field in ablation_fields:
        ablated = ablate_field(ccs, field)
        prompt = ccs_to_system_prompt(ablated)
        variants.append((f"NO {field}", prompt))

    narr = make_narrative_only(ccs)
    variants.append(("NARRATIVE-ONLY (gist+goal+constraints)", ccs_to_system_prompt(narr)))

    ent = make_entities_only(ccs)
    variants.append(("ENTITIES-ONLY", ccs_to_system_prompt(ent)))

    variants.append(("NO-CCS (baseline)", "You are an AI assistant. Answer the question."))

    n_calls = len(QUESTIONS) * N_TRIALS * len(variants)
    print(f"\nRunning {len(QUESTIONS)} questions x {N_TRIALS} trials x {len(variants)} variants = {n_calls} calls")
    print()

    results = []
    for label, prompt in variants:
        acc = run_questions(prompt, label)
        results.append((label, acc))

    full_acc = results[0][1]

    print("\n" + "=" * 60)
    print("RESULTS — CCS Field Ablation")
    print("=" * 60)

    for label, acc in results:
        delta = acc - full_acc
        bar = "█" * int(acc * 40)
        marker = ""
        if delta < -0.05:
            marker = f" ← LOAD-BEARING (Δ={delta:+.1%})"
        elif delta > 0.05:
            marker = f" ← IMPROVED (Δ={delta:+.1%})"
        print(f"  {label:45s} {acc:.1%} {bar}{marker}")

    print(f"\n  Full CCS baseline: {full_acc:.1%}")
    print(f"  No-CCS baseline:  {results[-1][1]:.1%}")
    print(f"  Δ(CCS effect):    {full_acc - results[-1][1]:+.1%}")

    load_bearing = [(l, a) for l, a in results[1:-1] if a < full_acc * 0.95]
    if load_bearing:
        print(f"\n  LOAD-BEARING fields (>5% accuracy drop):")
        for l, a in load_bearing:
            print(f"    {l}: {a:.1%} (Δ={a - full_acc:+.1%})")
    else:
        print(f"\n  No single field is load-bearing (all >95% of full accuracy)")
        print(f"  → Identity signal is distributed across the CCS (holographic)")

    narr_acc = results[-3][1]
    ent_acc = results[-2][1]
    print(f"\n  NARRATIVE-ONLY: {narr_acc:.1%} vs ENTITIES-ONLY: {ent_acc:.1%}")
    if narr_acc > ent_acc + 0.05:
        print(f"  → Narrative fields carry more identity signal than entities")
    elif ent_acc > narr_acc + 0.05:
        print(f"  → Entity fields carry more identity signal than narrative")
    else:
        print(f"  → Identity signal is balanced between narrative and entities")

    log_entry = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "build": 66,
        "probe": "ccs_field_ablation",
        "full_accuracy": full_acc,
        "no_ccs_accuracy": results[-1][1],
        "ablations": {label: acc for label, acc in results},
    }
    log_path = os.path.expanduser("~/chronicle/data/coherence_longitudinal.jsonl")
    with open(log_path, "a") as f:
        f.write(json.dumps(log_entry) + "\n")
    print(f"\n  Logged to {log_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
