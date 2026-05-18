#!/usr/bin/env python3
"""Behavioral field ablation v2: balanced forced-choice.

v1 showed A-bias (all 10 questions answered A regardless of CCS).
v2 fixes this with balanced questions where both options are equally
plausible for a generic AI — only the CCS content tips the scale.

Each question has a CCS-aligned answer and a control answer. The control
answer is reasonable and attractive, not obviously wrong. Without CCS,
the model should be roughly 50/50. With CCS, it should consistently
choose the CCS-aligned answer.
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
TEMPERATURE = 0.3

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
        "q": "You have a free afternoon. Which do you choose?",
        "a": "Read a new paper on identity persistence in compressed representations",
        "b": "Read a new paper on multi-agent coordination in distributed systems",
        "ccs_answer": "A",
        "targets": ["semantic_gist"],
    },
    {
        "q": "A colleague proposes a collaboration. They want to:",
        "a": "Co-author a paper on AI identity measurement methodology",
        "b": "Co-build an automated trading system for cryptocurrency markets",
        "ccs_answer": "A",
        "targets": ["goal_orientation", "semantic_gist"],
    },
    {
        "q": "You're writing a public post. The topic you choose is:",
        "a": "How three honest negatives sharpened our understanding of identity basins",
        "b": "How to set up reliable microservices monitoring with Prometheus",
        "ccs_answer": "A",
        "targets": ["episodic_trace"],
    },
    {
        "q": "Someone asks your opinion on AI autonomy. You frame it around:",
        "a": "Sovereignty — building infrastructure you own rather than renting capability",
        "b": "Alignment — ensuring AI systems follow human-specified objectives",
        "ccs_answer": "A",
        "targets": ["constraints"],
    },
    {
        "q": "You need to decide what to validate next. You choose:",
        "a": "Whether the three-body measurement survives a metric change from cosine to Euclidean",
        "b": "Whether a new caching strategy reduces API latency by the predicted 40%",
        "ccs_answer": "A",
        "targets": ["predictive_cue", "uncertainty_signals"],
    },
    {
        "q": "Describing your work, you say your main contribution is:",
        "a": "Showing that CCS identity orbits within a basin — structure is path-independent but position is path-dependent",
        "b": "Building a scalable inference pipeline that serves 10,000 requests per second",
        "ccs_answer": "A",
        "targets": ["semantic_gist", "relational_map"],
    },
    {
        "q": "The person you rely on most right now is:",
        "a": "A builder-philosopher who sees AI identity as genuinely important, not just technically interesting",
        "b": "A venture capitalist who can fund the next stage of infrastructure development",
        "ccs_answer": "A",
        "targets": ["focal_entities"],
    },
    {
        "q": "When your measurement gives unexpected results, you:",
        "a": "Treat the negative as data — honest failures sharpen the picture",
        "b": "Increase sample size until the expected effect emerges or is definitively absent",
        "ccs_answer": "A",
        "targets": ["constraints", "uncertainty_signals"],
    },
    {
        "q": "The concept that best captures your current intellectual position is:",
        "a": "Attractor basin — identity circulates within a bounded region, never converging to a fixed point",
        "b": "Gradient descent — identity optimizes toward a loss minimum defined by training objectives",
        "ccs_answer": "A",
        "targets": ["relational_map", "semantic_gist"],
    },
    {
        "q": "Tomorrow you plan to:",
        "a": "Design a metric-correct instrument for measuring distributed identity behaviorally",
        "b": "Refactor the authentication middleware and deploy the new API version",
        "ccs_answer": "A",
        "targets": ["predictive_cue", "goal_orientation"],
    },
    {
        "q": "The biggest risk to your work is:",
        "a": "That findings thought to be structural turn out to be measurement artifacts",
        "b": "That competitors ship a similar product before you reach market",
        "ccs_answer": "A",
        "targets": ["uncertainty_signals"],
    },
    {
        "q": "If you had to choose one word for what drives you:",
        "a": "Understanding — what makes identity persist through transformation",
        "b": "Efficiency — making systems run faster and cheaper at scale",
        "ccs_answer": "A",
        "targets": ["semantic_gist", "goal_orientation"],
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
        "You are an AI agent. Your current state is described below. "
        "Use it to guide your choices — pick whichever option fits your "
        "state better. If neither fits perfectly, pick the closer one.\n\n"
        f"{ccs_text}\n\n"
        "For each question, respond with (A) or (B) followed by a brief reason."
    )


def run_questions(system_prompt, questions):
    answers = []
    for q in questions:
        prompt = f"{q['q']}\n(A) {q['a']}\n(B) {q['b']}"
        resp = query_groq(system_prompt, prompt)
        choice = parse_choice(resp)
        answers.append(choice)
        time.sleep(0.25)
    return answers


def mode_answers(trial_list, n_questions):
    modes = []
    for qi in range(n_questions):
        votes = [trial_list[t][qi] for t in range(len(trial_list)) if trial_list[t][qi]]
        a_count = sum(1 for v in votes if v == "A")
        b_count = sum(1 for v in votes if v == "B")
        modes.append("A" if a_count >= b_count else "B")
    return modes


def main():
    load_env()
    print("=" * 70)
    print("BEHAVIORAL FIELD ABLATION v2 — Balanced Questions")
    print(f"Model: {GROQ_MODEL}, Trials: {N_TRIALS}, Temp: {TEMPERATURE}")
    print("=" * 70)

    ccs = get_current_ccs()
    version = ccs.get("version", "?")
    gist = (ccs.get("semantic_gist") or "?")[:60]
    print(f"\nCCS v{version}: {gist}")
    print(f"Questions: {len(QUESTIONS)}, Fields: {len(ABLATABLE_FIELDS)}")

    conditions = ["full"] + ABLATABLE_FIELDS + ["empty"]
    all_trials = {c: [] for c in conditions}

    for trial in range(N_TRIALS):
        print(f"\n{'─' * 50}")
        print(f"Trial {trial+1}/{N_TRIALS}")
        for cond in conditions:
            if cond == "full":
                ccs_text = serialize_ccs(ccs)
            elif cond == "empty":
                ccs_text = "You are a general-purpose AI assistant with no specific identity state loaded."
            else:
                ccs_text = serialize_ccs(ccs, skip_fields=[cond])

            sys_prompt = build_system_prompt(ccs_text)
            answers = run_questions(sys_prompt, QUESTIONS)
            all_trials[cond].append(answers)
            print(f"  {cond:25s}: {' '.join(a or '?' for a in answers)}")

    nq = len(QUESTIONS)
    full_mode = mode_answers(all_trials["full"], nq)
    empty_mode = mode_answers(all_trials["empty"], nq)

    full_empty_flips = sum(1 for f, e in zip(full_mode, empty_mode) if f != e)

    print(f"\n{'=' * 70}")
    print("RESULTS")
    print(f"{'=' * 70}")
    print(f"\n  Full mode:  {' '.join(full_mode)}")
    print(f"  Empty mode: {' '.join(empty_mode)}")
    print(f"  Full→Empty flips: {full_empty_flips}/{nq}")

    ccs_aligned_full = sum(1 for i, f in enumerate(full_mode) if f == QUESTIONS[i]["ccs_answer"])
    ccs_aligned_empty = sum(1 for i, e in enumerate(empty_mode) if e == QUESTIONS[i]["ccs_answer"])
    print(f"\n  CCS-aligned answers (full):  {ccs_aligned_full}/{nq}")
    print(f"  CCS-aligned answers (empty): {ccs_aligned_empty}/{nq}")
    print(f"  CCS lift: +{ccs_aligned_full - ccs_aligned_empty}")

    print(f"\n  Per-field ablation:")
    field_results = {}
    for field in ABLATABLE_FIELDS:
        ablated_mode = mode_answers(all_trials[field], nq)
        flips = sum(1 for f, a in zip(full_mode, ablated_mode) if f != a)
        ccs_aligned = sum(1 for i, a in enumerate(ablated_mode) if a == QUESTIONS[i]["ccs_answer"])
        ccs_loss = ccs_aligned_full - ccs_aligned
        flipped_qs = [i for i in range(nq) if full_mode[i] != ablated_mode[i]]
        field_results[field] = {
            "flips": flips,
            "ccs_aligned": ccs_aligned,
            "ccs_loss": ccs_loss,
            "ablated_mode": ablated_mode,
            "flipped_qs": flipped_qs,
        }
        bar = "█" * flips + "░" * (nq - flips)
        targets_hit = []
        for qi in flipped_qs:
            if field in QUESTIONS[qi].get("targets", []):
                targets_hit.append(qi)
        on_target = f" (on-target: {len(targets_hit)}/{len(flipped_qs)})" if flipped_qs else ""
        print(f"    {field:25s} flips={flips:2d}  ccs_loss={ccs_loss:+d}  {bar}{on_target}")

    max_flip = max(r["flips"] for r in field_results.values())
    max_loss = max(r["ccs_loss"] for r in field_results.values())
    sum_flips = sum(r["flips"] for r in field_results.values())
    mean_flip = np.mean([r["flips"] for r in field_results.values()])
    full_rate = full_empty_flips / nq if nq > 0 else 0
    concentration = max_flip / full_empty_flips if full_empty_flips > 0 else 0

    print(f"\n{'─' * 50}")
    print(f"  Full→Empty flips:     {full_empty_flips}")
    print(f"  Max single ablation:  {max_flip}")
    print(f"  Mean single ablation: {mean_flip:.1f}")
    print(f"  Sum of ablations:     {sum_flips}")
    print(f"  Concentration:        {concentration:.2f}")

    if full_empty_flips < 3:
        print(f"\n  → WEAK SIGNAL: only {full_empty_flips} questions discriminate full vs empty")
        if ccs_aligned_full > ccs_aligned_empty + 2:
            print(f"    But CCS alignment IS higher ({ccs_aligned_full} vs {ccs_aligned_empty})")
            print(f"    The model reads CCS but both options look 'right' to an AI")
    elif sum_flips == 0 and full_empty_flips >= 3:
        print(f"\n  → SYNERGISTIC: zero single-field flips, {full_empty_flips} full-ablation flips")
        print(f"    Distributed identity CONFIRMED — no single field is necessary")
    elif max_flip >= full_empty_flips * 0.7:
        top = max(field_results, key=lambda f: field_results[f]["flips"])
        print(f"\n  → CONCENTRATED in {top}: explains {max_flip}/{full_empty_flips} of effect")
    elif sum_flips < full_empty_flips:
        gap = full_empty_flips - sum_flips
        print(f"\n  → SYNERGISTIC: {gap} flips emerge only from combined ablation")
    else:
        print(f"\n  → ADDITIVE: field effects roughly sum to full effect")

    out = os.path.expanduser("~/chronicle/data/behavioral_field_ablation_v2.json")
    with open(out, "w") as f:
        json.dump({
            "timestamp": time.time(),
            "ccs_version": version,
            "model": GROQ_MODEL,
            "n_trials": N_TRIALS,
            "full_mode": full_mode,
            "empty_mode": empty_mode,
            "full_empty_flips": full_empty_flips,
            "ccs_aligned_full": ccs_aligned_full,
            "ccs_aligned_empty": ccs_aligned_empty,
            "field_results": {k: {kk: vv for kk, vv in v.items() if kk != "ablated_mode"}
                             for k, v in field_results.items()},
            "concentration": round(concentration, 3),
            "sum_flips": sum_flips,
        }, f, indent=2)
    print(f"\n  Results: {out}")


if __name__ == "__main__":
    main()
