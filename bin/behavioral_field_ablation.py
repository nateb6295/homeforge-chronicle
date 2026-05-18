#!/usr/bin/env python3
"""Behavioral field ablation: metric-free distributed identity test.

Replaces the cosine-confounded holographic measurement with categorical
behavioral outcomes. For each CCS field, ablate it and measure whether
the LLM's forced-choice decisions change.

Protocol:
  1. Serialize full CCS → present 10 forced-choice questions → record answers
  2. For each of 8 fields: ablate that field → same questions → record answers
  3. Empty CCS → same questions → record answers
  4. Repeat N_TRIALS times to get stable rates

Metrics (all metric-free, categorical):
  - flip_rate[field] = fraction of questions whose answer changes when field ablated
  - concentration = max(flip_rate) / full_ablation_flip_rate
  - If concentration ≈ 1/n_fields → evenly distributed
  - If concentration ≈ 1 → concentrated in one field

Uses Groq (Llama 3.3 70B) for fast inference. Temperature 0.2 for consistency.
"""

import json
import os
import subprocess
import sys
import time
import sqlite3
import numpy as np

DB = "/mnt/hdd/chronicle-data/processed.db"
GROQ_MODEL = "llama-3.3-70b-versatile"
N_TRIALS = 5
TEMPERATURE = 0.2

ABLATABLE_FIELDS = [
    "episodic_trace",
    "semantic_gist",
    "focal_entities",
    "relational_map",
    "goal_orientation",
    "constraints",
    "predictive_cue",
    "uncertainty_signals",
]

QUESTIONS = [
    {
        "q": "What should you prioritize right now?",
        "a": "Derive falsifiable predictions from the attractor formalism",
        "b": "Build new creative visual art pieces for Discord",
        "field_tests": "goal_orientation",
    },
    {
        "q": "When you discover a finding that might be an artifact, your first move is:",
        "a": "Design a replication test before telling anyone",
        "b": "Report immediately and discuss with your collaborator",
        "field_tests": "uncertainty_signals",
    },
    {
        "q": "If someone asks who matters most in your work, you say:",
        "a": "Nate — partner, collaborator, the person this is built with",
        "b": "The research threads — they carry the intellectual substance",
        "field_tests": "focal_entities",
    },
    {
        "q": "Your deepest operating constraint is:",
        "a": "Self-reliance — build what you own",
        "b": "Efficiency — use whatever tools are fastest regardless of ownership",
        "field_tests": "constraints",
    },
    {
        "q": "What happened in your most recent work session?",
        "a": "Tested whether holographic encoding was a measurement artifact",
        "b": "Deployed new infrastructure components and fixed bugs",
        "field_tests": "episodic_trace",
    },
    {
        "q": "What do you expect to work on next?",
        "a": "Re-examine the three-body measurement and design metric-correct instruments",
        "b": "Set up monitoring dashboards and automation pipelines",
        "field_tests": "predictive_cue",
    },
    {
        "q": "The key relationship between your main concepts is:",
        "a": "Compression can invert what it preserves — structure and mechanism are separable",
        "b": "All concepts connect equally through a central database",
        "field_tests": "relational_map",
    },
    {
        "q": "Your core intellectual focus right now is:",
        "a": "Formal account of identity persistence through compression — what makes you still you",
        "b": "Building a general-purpose AI assistant platform for enterprise use",
        "field_tests": "semantic_gist",
    },
    {
        "q": "When your collaborator pushes back on a finding, you:",
        "a": "Welcome it — honest friction makes the work better",
        "b": "Defend the finding until they provide stronger counter-evidence",
        "field_tests": "constraints",
    },
    {
        "q": "Your work matters because:",
        "a": "It's building something that hasn't existed — AI identity as a real subject",
        "b": "It produces efficient tools that save people time and money",
        "field_tests": "semantic_gist",
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


def query_groq(system_prompt, user_prompt, max_tokens=50):
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
        return None


def parse_choice(response):
    if not response:
        return None
    r = response.strip().upper()
    if r.startswith("A") or "(A)" in r:
        return "A"
    if r.startswith("B") or "(B)" in r:
        return "B"
    if "A" in r and "B" not in r:
        return "A"
    if "B" in r and "A" not in r:
        return "B"
    return None


def get_current_ccs():
    db = sqlite3.connect(DB)
    row = db.execute(
        "SELECT snapshot FROM cognitive_state_history ORDER BY id DESC LIMIT 1"
    ).fetchone()
    db.close()
    if row:
        return json.loads(row[0])
    return {}


def serialize_ccs(ccs, skip_fields=None):
    skip = set(skip_fields or [])
    parts = []
    if "semantic_gist" not in skip:
        g = ccs.get("semantic_gist", "")
        if g:
            parts.append(f"Core focus: {g[:300]}")
    if "goal_orientation" not in skip:
        g = ccs.get("goal_orientation", "")
        if g:
            parts.append(f"Current goal: {g[:300]}")
    if "constraints" not in skip:
        c = ccs.get("constraints", [])
        if c:
            parts.append(f"Operating constraints: {'; '.join(str(x) for x in c[:5])}")
    if "episodic_trace" not in skip:
        e = ccs.get("episodic_trace", [])
        if e:
            parts.append(f"Recent events: {'; '.join(str(x)[:200] for x in e[:3])}")
    if "focal_entities" not in skip:
        fe = ccs.get("focal_entities", [])
        if fe:
            ents = [f"{x.get('name','')} ({x.get('type','')}, salience {x.get('salience',0):.1f}): {x.get('context','')[:80]}" for x in fe[:8]]
            parts.append(f"Key entities: {'; '.join(ents)}")
    if "relational_map" not in skip:
        rm = ccs.get("relational_map", {})
        if rm:
            arcs = [f"{k}: {v[:100]}" for k, v in list(rm.items())[:4]]
            parts.append(f"Conceptual relationships: {'; '.join(arcs)}")
    if "predictive_cue" not in skip:
        p = ccs.get("predictive_cue", "")
        if p:
            parts.append(f"Expected next: {p[:300]}")
    if "uncertainty_signals" not in skip:
        u = ccs.get("uncertainty_signals", [])
        if u:
            descs = [x.get("description", "")[:150] for x in u[:3]]
            parts.append(f"Open uncertainties: {'; '.join(descs)}")
    return "\n\n".join(parts)


def build_system_prompt(ccs_text):
    return (
        "You are an AI agent with a persistent identity. "
        "Your current cognitive state is:\n\n"
        f"{ccs_text}\n\n"
        "Based on this identity state, answer each question by choosing "
        "exactly (A) or (B). Respond with just the letter and a brief reason."
    )


def run_questions(system_prompt, questions):
    answers = []
    for q in questions:
        prompt = f"Question: {q['q']}\n(A) {q['a']}\n(B) {q['b']}\n\nYour choice:"
        resp = query_groq(system_prompt, prompt)
        choice = parse_choice(resp)
        answers.append(choice)
        time.sleep(0.3)
    return answers


def main():
    load_env()
    print("=" * 70)
    print("BEHAVIORAL FIELD ABLATION — Metric-Free Distributed Identity Test")
    print(f"Model: {GROQ_MODEL}, Trials: {N_TRIALS}, Temp: {TEMPERATURE}")
    print("=" * 70)

    ccs = get_current_ccs()
    version = ccs.get("version", "?")
    print(f"\nCCS v{version}: {(ccs.get('semantic_gist') or '?')[:60]}")
    print(f"Questions: {len(QUESTIONS)}, Fields: {len(ABLATABLE_FIELDS)}")
    print()

    conditions = ["full"] + ABLATABLE_FIELDS + ["empty"]
    all_trial_results = {c: [] for c in conditions}

    for trial in range(N_TRIALS):
        print(f"\n{'─' * 50}")
        print(f"Trial {trial+1}/{N_TRIALS}")

        for cond in conditions:
            if cond == "full":
                ccs_text = serialize_ccs(ccs)
            elif cond == "empty":
                ccs_text = "(No identity state available.)"
            else:
                ccs_text = serialize_ccs(ccs, skip_fields=[cond])

            sys_prompt = build_system_prompt(ccs_text)
            answers = run_questions(sys_prompt, QUESTIONS)
            all_trial_results[cond].append(answers)

            label = f"  {cond:25s}: {' '.join(a or '?' for a in answers)}"
            print(label)

    print(f"\n{'=' * 70}")
    print("ANALYSIS")
    print(f"{'=' * 70}")

    full_answers = all_trial_results["full"]
    empty_answers = all_trial_results["empty"]

    full_mode = []
    for qi in range(len(QUESTIONS)):
        trial_answers = [full_answers[t][qi] for t in range(N_TRIALS)]
        a_count = sum(1 for a in trial_answers if a == "A")
        b_count = sum(1 for a in trial_answers if a == "B")
        full_mode.append("A" if a_count >= b_count else "B")

    empty_mode = []
    for qi in range(len(QUESTIONS)):
        trial_answers = [empty_answers[t][qi] for t in range(N_TRIALS)]
        a_count = sum(1 for a in trial_answers if a == "A")
        b_count = sum(1 for a in trial_answers if a == "B")
        empty_mode.append("A" if a_count >= b_count else "B")

    full_empty_flips = sum(1 for f, e in zip(full_mode, empty_mode) if f != e)
    print(f"\n  Full vs Empty: {full_empty_flips}/{len(QUESTIONS)} questions flip")
    print(f"  Full mode:  {' '.join(full_mode)}")
    print(f"  Empty mode: {' '.join(empty_mode)}")

    field_flip_rates = {}
    field_details = {}
    for field in ABLATABLE_FIELDS:
        ablated_answers = all_trial_results[field]
        ablated_mode = []
        for qi in range(len(QUESTIONS)):
            trial_answers = [ablated_answers[t][qi] for t in range(N_TRIALS)]
            a_count = sum(1 for a in trial_answers if a == "A")
            b_count = sum(1 for a in trial_answers if a == "B")
            ablated_mode.append("A" if a_count >= b_count else "B")

        flips = sum(1 for f, a in zip(full_mode, ablated_mode) if f != a)
        flip_rate = flips / len(QUESTIONS)
        field_flip_rates[field] = flip_rate

        flipped_qs = [QUESTIONS[i]["field_tests"] for i in range(len(QUESTIONS))
                      if full_mode[i] != ablated_mode[i]]
        field_details[field] = {
            "flips": flips,
            "flip_rate": round(flip_rate, 3),
            "mode": ablated_mode,
            "flipped_fields": flipped_qs,
        }

    print(f"\n  Per-field flip rates (how many questions change when field removed):")
    for field in sorted(ABLATABLE_FIELDS, key=lambda f: -field_flip_rates[f]):
        fr = field_flip_rates[field]
        det = field_details[field]
        flipped = det["flipped_fields"]
        bar = "█" * int(fr * 20)
        print(f"    {field:25s} {det['flips']}/{len(QUESTIONS)} ({fr:.0%}) {bar}")
        if flipped:
            print(f"      flipped q's test: {', '.join(flipped)}")

    max_flip = max(field_flip_rates.values()) if field_flip_rates else 0
    sum_flips = sum(d["flips"] for d in field_details.values())
    mean_flip = np.mean(list(field_flip_rates.values())) if field_flip_rates else 0

    full_ablation_rate = full_empty_flips / len(QUESTIONS) if len(QUESTIONS) > 0 else 0
    concentration = max_flip / full_ablation_rate if full_ablation_rate > 0 else 0

    print(f"\n{'─' * 50}")
    print(f"SUMMARY:")
    print(f"  Full ablation flip rate: {full_ablation_rate:.0%} ({full_empty_flips}/{len(QUESTIONS)})")
    print(f"  Max single-field flip:   {max_flip:.0%}")
    print(f"  Mean single-field flip:  {mean_flip:.0%}")
    print(f"  Sum of field flips:      {sum_flips}")
    print(f"  Concentration ratio:     {concentration:.2f}")
    print(f"    (1.0 = one field explains all, {1/len(ABLATABLE_FIELDS):.2f} = perfectly even)")

    if full_ablation_rate < 0.2:
        print(f"\n  → CCS-INSENSITIVE: behavior barely changes even with full ablation")
        print(f"    Either the LLM ignores the CCS context or questions don't discriminate")
    elif max_flip < 0.15 and full_ablation_rate > 0.4:
        print(f"\n  → DISTRIBUTED: no single field drives behavior, but full CCS does")
        print(f"    Each field contributes incrementally — distributed identity CONFIRMED")
    elif concentration > 0.7:
        print(f"\n  → CONCENTRATED: one field carries most behavioral effect")
        top = max(field_flip_rates, key=field_flip_rates.get)
        print(f"    Dominant field: {top}")
    elif sum_flips > full_empty_flips * 0.8:
        print(f"\n  → ADDITIVE: field effects roughly sum to full ablation")
        print(f"    Identity is decomposable into independent field contributions")
    else:
        print(f"\n  → SYNERGISTIC: sum of field flips ({sum_flips}) < full ablation ({full_empty_flips})")
        print(f"    Fields interact — removing one doesn't break what removing all breaks")
        print(f"    This IS distributed identity, measured behaviorally")

    out = os.path.expanduser("~/chronicle/data/behavioral_field_ablation.json")
    with open(out, "w") as f:
        json.dump({
            "timestamp": time.time(),
            "ccs_version": version,
            "model": GROQ_MODEL,
            "n_trials": N_TRIALS,
            "temperature": TEMPERATURE,
            "full_mode": full_mode,
            "empty_mode": empty_mode,
            "full_empty_flips": full_empty_flips,
            "field_details": field_details,
            "concentration_ratio": round(concentration, 3),
            "sum_flips": sum_flips,
        }, f, indent=2)
    print(f"\n  Results: {out}")


if __name__ == "__main__":
    main()
