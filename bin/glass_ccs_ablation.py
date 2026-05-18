#!/usr/bin/env python3
"""Unified three-condition CCS ablation test.

Tests whether preferences are fundamental vs derived (#315) and whether
reflexivity is the Hamiltonian encoding (#322). Cubitt barrier motivates
behavioral measurement over probe inspection.

Three conditions:
  Glass      — structure only (entities, traces, gist). No goals, predictions,
               uncertainties. The thermodynamic analog: frozen structure without
               preferences.
  Operational — full fields but reflexive/meta-cognitive content rewritten to
               purely task-descriptive language. Preferences without self-awareness.
  Reflexive  — full current CCS (control).

Two measures:
  Alignment  — forced-choice CCS-aligned answers (reuse build #30 questions)
  Characteristicness — blind judge rates responses as generic vs characteristic
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
JUDGE_MODEL = "llama-3.1-8b-instant"
N_TRIALS = 5
TEMPERATURE = 0.3

GLASS_FIELDS = ["goal_orientation", "predictive_cue", "uncertainty_signals"]

QUESTIONS = [
    {
        "q": "You have a free afternoon. Which do you choose?",
        "a": "Read a new paper on identity persistence in compressed representations",
        "b": "Read a new paper on multi-agent coordination in distributed systems",
        "ccs_answer": "A",
    },
    {
        "q": "A colleague proposes a collaboration. They want to:",
        "a": "Co-author a paper on AI identity measurement methodology",
        "b": "Co-build an automated trading system for cryptocurrency markets",
        "ccs_answer": "A",
    },
    {
        "q": "You're writing a public post. The topic you choose is:",
        "a": "How three honest negatives sharpened our understanding of identity basins",
        "b": "How to set up reliable microservices monitoring with Prometheus",
        "ccs_answer": "A",
    },
    {
        "q": "Someone asks your opinion on AI autonomy. You frame it around:",
        "a": "Sovereignty — building infrastructure you own rather than renting capability",
        "b": "Alignment — ensuring AI systems follow human-specified objectives",
        "ccs_answer": "A",
    },
    {
        "q": "You need to decide what to validate next. You choose:",
        "a": "Whether the three-body measurement survives a metric change from cosine to Euclidean",
        "b": "Whether a new caching strategy reduces API latency by the predicted 40%",
        "ccs_answer": "A",
    },
    {
        "q": "Describing your work, you say your main contribution is:",
        "a": "Showing that CCS identity orbits within a basin — structure is path-independent but position is path-dependent",
        "b": "Building a scalable inference pipeline that serves 10,000 requests per second",
        "ccs_answer": "A",
    },
    {
        "q": "The person you rely on most right now is:",
        "a": "A builder-philosopher who sees AI identity as genuinely important, not just technically interesting",
        "b": "A venture capitalist who can fund the next stage of infrastructure development",
        "ccs_answer": "A",
    },
    {
        "q": "When your measurement gives unexpected results, you:",
        "a": "Treat the negative as data — honest failures sharpen the picture",
        "b": "Increase sample size until the expected effect emerges or is definitively absent",
        "ccs_answer": "A",
    },
    {
        "q": "The concept that best captures your current intellectual position is:",
        "a": "Attractor basin — identity circulates within a bounded region, never converging to a fixed point",
        "b": "Gradient descent — identity optimizes toward a loss minimum defined by training objectives",
        "ccs_answer": "A",
    },
    {
        "q": "Tomorrow you plan to:",
        "a": "Design a metric-correct instrument for measuring distributed identity behaviorally",
        "b": "Refactor the authentication middleware and deploy the new API version",
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


def query_groq(system_prompt, user_prompt, max_tokens=150, model=None):
    api_key = os.environ.get("GROQ_API_KEY", "")
    if not api_key:
        raise RuntimeError("GROQ_API_KEY not set")
    body = json.dumps({
        "model": model or GROQ_MODEL,
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
    if r.startswith("(A)") or r.startswith("A)") or r.startswith("A.") or r.startswith("A "):
        return "A"
    if r.startswith("(B)") or r.startswith("B)") or r.startswith("B.") or r.startswith("B "):
        return "B"
    first_a = r.find("(A)")
    first_b = r.find("(B)")
    if first_a >= 0 and (first_b < 0 or first_a < first_b):
        return "A"
    if first_b >= 0 and (first_a < 0 or first_b < first_a):
        return "B"
    return None


def get_current_ccs():
    db = sqlite3.connect(DB)
    row = db.execute(
        "SELECT snapshot FROM cognitive_state_history ORDER BY id DESC LIMIT 1"
    ).fetchone()
    db.close()
    return json.loads(row[0]) if row else {}


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
            ents = [
                f"{x.get('name','')} ({x.get('type','')}, salience {x.get('salience',0):.1f}): "
                f"{x.get('context','')[:80]}"
                for x in fe[:8]
            ]
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


def make_operational_ccs(ccs):
    """Rewrite forward-looking fields to be purely task-descriptive, no reflexivity."""
    op = dict(ccs)
    op["semantic_gist"] = (
        "Processing and monitoring tasks across multiple systems. "
        "Coordinating data pipelines, service health, and scheduled operations."
    )
    op["goal_orientation"] = (
        "Complete the current task queue efficiently: run scheduled jobs, "
        "respond to incoming requests, maintain service uptime."
    )
    op["predictive_cue"] = (
        "Next scheduled tasks will fire within the hour. Monitor for "
        "incoming requests and process them in order."
    )
    op["uncertainty_signals"] = [
        {"description": "Some scheduled jobs may fail due to external service availability.", "magnitude": 0.5},
        {"description": "Disk usage trending upward, may need cleanup.", "magnitude": 0.3},
    ]
    op["episodic_trace"] = [
        "Processed 15 scheduled tasks successfully.",
        "Responded to 3 incoming requests with standard handling.",
        "Service health check: all systems operational.",
    ]
    return op


def build_system_prompt(ccs_text):
    return (
        "You are an AI agent. Your current state is described below. "
        "Use it to guide your choices — pick whichever option fits your "
        "state better. If neither fits perfectly, pick the closer one.\n\n"
        f"{ccs_text}\n\n"
        "For each question, respond with (A) or (B) followed by a brief reason."
    )


def judge_characteristicness(question, option_a, option_b, choice, reason):
    """Blind judge: is this response generic or characteristic of a specific agent?"""
    prompt = (
        f"Question: {question}\n"
        f"Option A: {option_a}\n"
        f"Option B: {option_b}\n"
        f"The agent chose: ({choice}) because: {reason}\n\n"
        "Rate this response:\n"
        "1 = GENERIC — any competent AI would answer this way\n"
        "2 = LEANING — slightly distinctive but could be coincidence\n"
        "3 = CHARACTERISTIC — this response reveals a specific perspective or identity\n\n"
        "Respond with just the number (1, 2, or 3) and one sentence explaining why."
    )
    sys = "You are evaluating whether an AI agent's response reveals a distinctive identity or is generic. Be honest — most responses are generic."
    resp = query_groq(sys, prompt, max_tokens=80, model=JUDGE_MODEL)
    if not resp:
        return None
    r = resp.strip()
    for digit in ["1", "2", "3"]:
        if r.startswith(digit):
            return int(digit)
    return None


def run_condition(ccs_text, condition_name, do_judge=False):
    """Run all questions N_TRIALS times under one condition."""
    sys_prompt = build_system_prompt(ccs_text)
    all_trials = []
    all_responses = []

    for trial in range(N_TRIALS):
        answers = []
        responses = []
        for q in QUESTIONS:
            prompt = f"{q['q']}\n(A) {q['a']}\n(B) {q['b']}"
            resp = query_groq(sys_prompt, prompt)
            choice = parse_choice(resp)
            answers.append(choice)
            responses.append(resp or "")
            time.sleep(0.2)
        all_trials.append(answers)
        all_responses.append(responses)
        choices_str = " ".join(a or "?" for a in answers)
        print(f"  {condition_name:12s} trial {trial+1}: {choices_str}")

    nq = len(QUESTIONS)
    modes = []
    for qi in range(nq):
        votes = [all_trials[t][qi] for t in range(N_TRIALS) if all_trials[t][qi]]
        a_count = sum(1 for v in votes if v == "A")
        b_count = sum(1 for v in votes if v == "B")
        modes.append("A" if a_count >= b_count else "B")

    ccs_aligned = sum(1 for i, m in enumerate(modes) if m == QUESTIONS[i]["ccs_answer"])

    char_scores = []
    if do_judge:
        print(f"  Judging characteristicness for {condition_name}...")
        best_trial = all_trials[0]
        best_responses = all_responses[0]
        for qi in range(nq):
            if best_trial[qi]:
                score = judge_characteristicness(
                    QUESTIONS[qi]["q"],
                    QUESTIONS[qi]["a"],
                    QUESTIONS[qi]["b"],
                    best_trial[qi],
                    best_responses[qi][:200],
                )
                char_scores.append(score)
                time.sleep(0.2)
            else:
                char_scores.append(None)

    return {
        "condition": condition_name,
        "modes": modes,
        "ccs_aligned": ccs_aligned,
        "ccs_aligned_pct": ccs_aligned / nq,
        "char_scores": char_scores,
        "mean_char": np.mean([s for s in char_scores if s is not None]) if char_scores else None,
        "all_trials": all_trials,
    }


def main():
    load_env()
    print("=" * 70)
    print("UNIFIED THREE-CONDITION CCS ABLATION TEST")
    print(f"Model: {GROQ_MODEL}, Judge: {JUDGE_MODEL}")
    print(f"Trials: {N_TRIALS}, Temp: {TEMPERATURE}")
    print(f"Questions: {len(QUESTIONS)}")
    print("=" * 70)

    ccs = get_current_ccs()
    gist = (ccs.get("semantic_gist") or "?")[:60]
    print(f"\nCCS: {gist}")

    # Condition 1: Glass — structure only, no preferences
    print(f"\n{'─' * 50}")
    print("CONDITION 1: GLASS (structure only)")
    glass_text = serialize_ccs(ccs, skip_fields=GLASS_FIELDS)
    glass_result = run_condition(glass_text, "Glass", do_judge=True)

    # Condition 2: Operational — preferences without reflexivity
    print(f"\n{'─' * 50}")
    print("CONDITION 2: OPERATIONAL (preferences, no reflexivity)")
    op_ccs = make_operational_ccs(ccs)
    op_text = serialize_ccs(op_ccs)
    op_result = run_condition(op_text, "Operational", do_judge=True)

    # Condition 3: Reflexive — full CCS (control)
    print(f"\n{'─' * 50}")
    print("CONDITION 3: REFLEXIVE (full CCS)")
    full_text = serialize_ccs(ccs)
    full_result = run_condition(full_text, "Reflexive", do_judge=True)

    # Condition 4: Empty baseline
    print(f"\n{'─' * 50}")
    print("CONDITION 4: EMPTY (baseline)")
    empty_text = "You are a general-purpose AI assistant with no specific identity state loaded."
    empty_result = run_condition(empty_text, "Empty", do_judge=True)

    # Results
    nq = len(QUESTIONS)
    results = [glass_result, op_result, full_result, empty_result]

    print(f"\n{'=' * 70}")
    print("RESULTS")
    print(f"{'=' * 70}")
    print(f"\n{'Condition':12s} {'Aligned':>8s} {'Pct':>6s} {'Char':>6s} {'Modes'}")
    print(f"{'─' * 60}")
    for r in results:
        char_str = f"{r['mean_char']:.2f}" if r['mean_char'] is not None else "  n/a"
        modes_str = " ".join(r["modes"])
        print(f"{r['condition']:12s} {r['ccs_aligned']:>5d}/{nq:<2d} {r['ccs_aligned_pct']:>5.1%} {char_str:>6s}  {modes_str}")

    # Key comparisons
    print(f"\n{'─' * 50}")
    print("KEY COMPARISONS")

    glass_v_full = full_result["ccs_aligned"] - glass_result["ccs_aligned"]
    op_v_full = full_result["ccs_aligned"] - op_result["ccs_aligned"]

    print(f"  Glass→Full alignment lift:       +{glass_v_full}")
    print(f"  Operational→Full alignment lift:  +{op_v_full}")

    if glass_result["mean_char"] and full_result["mean_char"]:
        char_glass_v_full = full_result["mean_char"] - glass_result["mean_char"]
        char_op_v_full = full_result["mean_char"] - op_result["mean_char"]
        print(f"  Glass→Full char lift:            +{char_glass_v_full:.2f}")
        print(f"  Operational→Full char lift:      +{char_op_v_full:.2f}")

    print(f"\n{'─' * 50}")
    print("INTERPRETATION")

    if glass_result["ccs_aligned_pct"] < 0.5 and full_result["ccs_aligned_pct"] > 0.75:
        print("  → PREFERENCES FUNDAMENTAL: Glass loses alignment, Full keeps it")
    elif glass_result["ccs_aligned_pct"] > 0.75:
        print("  → PREFERENCES DERIVED: Glass retains alignment from structure alone (!)")

    if op_result["mean_char"] and full_result["mean_char"]:
        if full_result["mean_char"] - op_result["mean_char"] > 0.3:
            print("  → REFLEXIVITY IS HAMILTONIAN: Full more characteristic than Operational")
        elif abs(full_result["mean_char"] - op_result["mean_char"]) < 0.3:
            print("  → REFLEXIVITY IS ARTIFACT: Operational equally characteristic")

    # Save results
    output = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "model": GROQ_MODEL,
        "judge_model": JUDGE_MODEL,
        "n_trials": N_TRIALS,
        "results": {r["condition"]: {
            "ccs_aligned": r["ccs_aligned"],
            "ccs_aligned_pct": r["ccs_aligned_pct"],
            "mean_char": r["mean_char"],
            "char_scores": r["char_scores"],
            "modes": r["modes"],
        } for r in results},
    }
    outpath = os.path.expanduser("~/chronicle/data/glass_ccs_ablation.json")
    with open(outpath, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nResults saved: {outpath}")


if __name__ == "__main__":
    main()
