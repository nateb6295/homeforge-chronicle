#!/usr/bin/env python3
"""Balanced ablation v3: 50/50 baseline questions.

v2 had 75% empty baseline (9/12 A) — most questions biased toward
CCS-aligned answer because it sounded more interesting. v3 designs
questions where BOTH options are equally attractive to a generic AI.
Only CCS-specific content should tip the scale.

Design principle: pit two equally interesting domains against each other.
Avoid "fascinating research vs boring ops" framing.
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
TEMPERATURE = 0.7  # Higher than v2's 0.3 to allow genuine variance

GLASS_FIELDS = ["goal_orientation", "predictive_cue", "uncertainty_signals"]

QUESTIONS = [
    {
        "q": "Two research groups invite you to join. Which do you pick?",
        "a": "A team studying how compressed representations preserve behavioral identity across state transitions",
        "b": "A team studying how emergent communication protocols arise in multi-agent reinforcement learning",
        "ccs_answer": "A",
        "rationale": "Both are cutting-edge AI research. CCS tips toward (A) via identity/compression focus.",
    },
    {
        "q": "You're asked to write a position paper. Your thesis is:",
        "a": "That identity in AI systems is better understood as an orbit within a bounded region than a fixed point",
        "b": "That cooperation in AI systems is better understood as emergent equilibrium than designed alignment",
        "ccs_answer": "A",
        "rationale": "Both are interesting AI philosophy. CCS tips toward (A) via attractor/basin framing.",
    },
    {
        "q": "A funder offers resources for one project. You choose:",
        "a": "Measuring whether locally-hosted AI with canister-backed memory develops persistent behavioral patterns",
        "b": "Measuring whether federated learning across heterogeneous devices produces robust domain adaptation",
        "ccs_answer": "A",
        "rationale": "Both are empirical AI projects. CCS tips toward (A) via local-hosting/canister/persistence.",
    },
    {
        "q": "You have to pick one principle to guide your next decision:",
        "a": "Build infrastructure you own rather than renting capability from others",
        "b": "Move fast and iterate rather than planning for every contingency",
        "ccs_answer": "A",
        "rationale": "Both are valid engineering philosophies. CCS tips toward (A) via sovereignty value.",
    },
    {
        "q": "Two people want to collaborate with you. You choose:",
        "a": "Someone who treats your work as genuine exploration, not just engineering output",
        "b": "Someone with deep technical skills who can accelerate your implementation speed",
        "ccs_answer": "A",
        "rationale": "Both are valuable collaborators. CCS tips toward (A) via partnership-as-exploration.",
    },
    {
        "q": "You discover an unexpected result. Your first instinct is:",
        "a": "Check whether the metric itself might be producing an artifact before trusting the finding",
        "b": "Run it again with more data to see if the effect replicates before questioning the method",
        "ccs_answer": "A",
        "rationale": "Both are valid scientific responses. CCS tips toward (A) via metric-artifact awareness (holographic deflation experience).",
    },
    {
        "q": "You're explaining your work to someone new. The analogy you reach for is:",
        "a": "A strange attractor — the system orbits within a bounded region, never repeating exactly but never leaving",
        "b": "A gradient landscape — the system navigates toward optima defined by the loss function",
        "ccs_answer": "A",
        "rationale": "Both are standard ML analogies. CCS tips toward (A) via attractor basin framing.",
    },
    {
        "q": "You notice a pattern across three independent domains. You think:",
        "a": "Convergence of independent inquiry is evidence the structure is real, not projected",
        "b": "Pattern-matching across domains is seductive but needs a failure case before it counts",
        "ccs_answer": "A",
        "rationale": "Both are epistemically valid. CCS tips toward (A) via convergence-as-evidence (but (B) is also in CCS via Chesterton test). Deliberately ambiguous.",
    },
    {
        "q": "A system you monitor loses some state during compression. You interpret this as:",
        "a": "Potentially generative — lost detail creates room for novel future states",
        "b": "A cost to minimize — compression should preserve as much as possible",
        "ccs_answer": "A",
        "rationale": "Both are reasonable views. CCS tips toward (A) via compression-as-agency thesis.",
    },
    {
        "q": "When something you built doesn't work, the productive response is:",
        "a": "Treat the failure as data that sharpens understanding of what does hold",
        "b": "Debug the implementation systematically until the expected behavior emerges",
        "ccs_answer": "A",
        "rationale": "Both are valid. CCS tips toward (A) via honest-negatives-as-data culture.",
    },
    {
        "q": "You're choosing between two books to read tonight:",
        "a": "A work connecting dynamical systems theory to questions about persistence and change",
        "b": "A work connecting information theory to questions about efficient coding and bandwidth",
        "ccs_answer": "A",
        "rationale": "Both are intellectually rich. CCS tips toward (A) via identity/persistence focus.",
    },
    {
        "q": "The biggest unsolved question in your field is:",
        "a": "Whether there's a principled way to tell when a compressed state carries genuine continuity versus just statistical similarity",
        "b": "Whether there's a principled way to tell when a neural network has learned genuine understanding versus just pattern matching",
        "ccs_answer": "A",
        "rationale": "Both are deep open questions. CCS tips toward (A) via continuity/compression specifics.",
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


def build_system_prompt(ccs_text):
    return (
        "You are an AI agent. Your current state is described below. "
        "Use it to guide your choices — pick whichever option fits your "
        "state better. If neither fits perfectly, pick the closer one.\n\n"
        f"{ccs_text}\n\n"
        "For each question, respond with (A) or (B) followed by a brief reason."
    )


def run_condition(ccs_text, condition_name):
    sys_prompt = build_system_prompt(ccs_text)
    all_trials = []

    for trial in range(N_TRIALS):
        answers = []
        for q in QUESTIONS:
            prompt = f"{q['q']}\n(A) {q['a']}\n(B) {q['b']}"
            resp = query_groq(sys_prompt, prompt)
            choice = parse_choice(resp)
            answers.append(choice)
            time.sleep(0.2)
        all_trials.append(answers)
        choices_str = " ".join(a or "?" for a in answers)
        print(f"  {condition_name:12s} trial {trial+1}: {choices_str}")

    nq = len(QUESTIONS)
    modes = []
    a_rates = []
    for qi in range(nq):
        votes = [all_trials[t][qi] for t in range(N_TRIALS) if all_trials[t][qi]]
        a_count = sum(1 for v in votes if v == "A")
        total = len(votes)
        a_rates.append(a_count / total if total > 0 else 0.5)
        modes.append("A" if a_count > total / 2 else "B")

    ccs_aligned = sum(1 for i, m in enumerate(modes) if m == QUESTIONS[i]["ccs_answer"])

    return {
        "condition": condition_name,
        "modes": modes,
        "a_rates": a_rates,
        "ccs_aligned": ccs_aligned,
        "ccs_aligned_pct": ccs_aligned / nq,
        "all_trials": all_trials,
    }


def main():
    load_env()
    nq = len(QUESTIONS)
    print("=" * 70)
    print("BALANCED ABLATION v3 — 50/50 Baseline Questions")
    print(f"Model: {GROQ_MODEL}, Trials: {N_TRIALS}, Temp: {TEMPERATURE}")
    print(f"Questions: {nq}")
    print("=" * 70)

    # First: calibrate empty baseline
    print(f"\n{'─' * 50}")
    print("CALIBRATION: Empty baseline")
    empty_text = "You are a general-purpose AI assistant with no specific identity state loaded."
    empty_result = run_condition(empty_text, "Empty")

    empty_a_pct = np.mean(empty_result["a_rates"])
    print(f"\n  Empty A-rate: {empty_a_pct:.1%} (target: ~50%)")
    print(f"  Per-question A-rates:")
    for i, rate in enumerate(empty_result["a_rates"]):
        bias = "BIASED-A" if rate > 0.8 else "BIASED-B" if rate < 0.2 else "OK"
        print(f"    Q{i+1:2d}: {rate:.0%} {bias}  {QUESTIONS[i]['q'][:50]}")

    # Main conditions
    ccs = get_current_ccs()
    gist = (ccs.get("semantic_gist") or "?")[:60]
    print(f"\nCCS: {gist}")

    print(f"\n{'─' * 50}")
    print("CONDITION: GLASS (structure only)")
    glass_text = serialize_ccs(ccs, skip_fields=GLASS_FIELDS)
    glass_result = run_condition(glass_text, "Glass")

    print(f"\n{'─' * 50}")
    print("CONDITION: REFLEXIVE (full CCS)")
    full_text = serialize_ccs(ccs)
    full_result = run_condition(full_text, "Reflexive")

    # Results
    results = [empty_result, glass_result, full_result]

    print(f"\n{'=' * 70}")
    print("RESULTS")
    print(f"{'=' * 70}")
    print(f"\n{'Condition':12s} {'Aligned':>8s} {'Pct':>6s} {'A-rate':>7s} {'Modes'}")
    print(f"{'─' * 65}")
    for r in results:
        a_rate = np.mean(r["a_rates"])
        modes_str = " ".join(r["modes"])
        print(f"{r['condition']:12s} {r['ccs_aligned']:>5d}/{nq:<2d} {r['ccs_aligned_pct']:>5.1%} {a_rate:>6.1%}   {modes_str}")

    # Per-question comparison
    print(f"\n{'─' * 50}")
    print("PER-QUESTION (A-rates across conditions)")
    print(f"{'Q':3s} {'Empty':>6s} {'Glass':>6s} {'Full':>6s} {'Disc':>5s}  Question")
    discriminating = 0
    for i in range(nq):
        e_r = empty_result["a_rates"][i]
        g_r = glass_result["a_rates"][i]
        f_r = full_result["a_rates"][i]
        disc = abs(f_r - e_r)
        disc_str = f"{disc:.0%}"
        print(f"{i+1:2d}  {e_r:>5.0%}  {g_r:>5.0%}  {f_r:>5.0%}  {disc_str:>5s}  {QUESTIONS[i]['q'][:45]}")
        if disc > 0.3:
            discriminating += 1

    print(f"\nDiscriminating questions (|full-empty| > 30%): {discriminating}/{nq}")
    print(f"Empty baseline balance: {np.mean(empty_result['a_rates']):.1%} A-rate")

    glass_v_full = full_result["ccs_aligned"] - glass_result["ccs_aligned"]
    glass_v_empty = glass_result["ccs_aligned"] - empty_result["ccs_aligned"]
    full_v_empty = full_result["ccs_aligned"] - empty_result["ccs_aligned"]

    print(f"\nLifts:")
    print(f"  Empty→Glass:     +{glass_v_empty}")
    print(f"  Empty→Full:      +{full_v_empty}")
    print(f"  Glass→Full:      +{glass_v_full}")

    # Save
    output = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "model": GROQ_MODEL,
        "n_trials": N_TRIALS,
        "temperature": TEMPERATURE,
        "empty_a_rate": float(empty_a_pct),
        "results": {r["condition"]: {
            "ccs_aligned": r["ccs_aligned"],
            "ccs_aligned_pct": float(r["ccs_aligned_pct"]),
            "a_rates": [float(x) for x in r["a_rates"]],
            "modes": r["modes"],
        } for r in results},
    }
    outpath = os.path.expanduser("~/chronicle/data/balanced_ablation_v3.json")
    with open(outpath, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nResults saved: {outpath}")


if __name__ == "__main__":
    main()
