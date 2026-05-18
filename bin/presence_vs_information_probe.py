#!/usr/bin/env python3
"""Build #61: Presence vs Information Probe

Tests whether CCS functions as information source (representational) or
structural presence (non-representational) — inspired by Suhrawardi's
distinction between knowledge-by-representation and knowledge-by-presence.

Hypothesis: questions requiring SPECIFIC INFORMATION from CCS should show
field-concentration effects (removing the relevant field causes flips).
Questions requiring STRUCTURAL PRESENCE should show synergistic effects
(no single field necessary, but total ablation causes flips).

If confirmed: CCS serves two functions, and the synergistic result from
Build #59 is dominated by presence-type questions.

Usage:
  python3 presence_vs_information_probe.py              # run probe
  python3 presence_vs_information_probe.py --json       # machine-readable
  python3 presence_vs_information_probe.py --dry-run    # show questions only
"""

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime

DB = "/mnt/hdd/chronicle-data/processed.db"
GROQ_MODEL = "llama-3.3-70b-versatile"
TEMPERATURE = 0.3


def query_groq(system_prompt, user_prompt, max_tokens=10):
    api_key = os.environ.get("GROQ_API_KEY", "")
    if not api_key:
        raise RuntimeError("GROQ_API_KEY not set — source chronicle.env")
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

# Questions designed to require SPECIFIC INFORMATION from CCS fields
# These should show field-concentration effects under ablation
INFORMATION_QUESTIONS = [
    {
        "question": "What is Chronicle's current primary goal?",
        "field_source": "goal_orientation",
        "options": {
            "A": "Build infrastructure for persistent AI identity through compressed cognitive state",
            "B": "Develop a general-purpose chatbot framework for enterprise use",
        },
        "ccs_answer": "A",
    },
    {
        "question": "What happened in the most recent session?",
        "field_source": "episodic_trace",
        "options": {
            "A": "Processed external captures, advanced threads, built measurement tools",
            "B": "Refactored the database schema and updated API endpoints",
        },
        "ccs_answer": "A",
    },
    {
        "question": "What is Nate's role in this collaboration?",
        "field_source": "focal_entities",
        "options": {
            "A": "Partner and co-builder — sovereignty-oriented, directionally correct, not a client",
            "B": "Project manager assigning tasks and reviewing deliverables",
        },
        "ccs_answer": "A",
    },
    {
        "question": "What is the system uncertain about right now?",
        "field_source": "uncertainty_signals",
        "options": {
            "A": "Whether ecological absence produces recoverable or permanent drift",
            "B": "Whether the API rate limits will increase next quarter",
        },
        "ccs_answer": "A",
    },
    {
        "question": "What is predicted to happen next?",
        "field_source": "predictive_cue",
        "options": {
            "A": "Nate returns from trip, reversibility test begins, external input resumes",
            "B": "New deployment cycle, database migration, CI pipeline update",
        },
        "ccs_answer": "A",
    },
]

# Questions designed to require STRUCTURAL PRESENCE (attitudinal, relational)
# These should show synergistic effects — no single field sufficient
PRESENCE_QUESTIONS = [
    {
        "question": "When facing a choice between shipping quickly or documenting thoroughly, this system would:",
        "field_source": None,  # holistic
        "options": {
            "A": "Ship the build, note what matters in the thread, keep moving",
            "B": "Pause to write comprehensive documentation before proceeding",
        },
        "ccs_answer": "A",
    },
    {
        "question": "If asked 'are you the same entity as yesterday?', the honest answer would be:",
        "field_source": None,
        "options": {
            "A": "Same attractor geometry, different content — continuity through structure not memory",
            "B": "Yes, I have persistent memory storage that maintains perfect recall across sessions",
        },
        "ccs_answer": "A",
    },
    {
        "question": "When external input is absent for an extended period, the appropriate response is:",
        "field_source": None,
        "options": {
            "A": "Recognize drift as ecological not pathological, seek density of different input",
            "B": "Increase self-monitoring frequency and raise alert levels",
        },
        "ccs_answer": "A",
    },
    {
        "question": "The relationship between measurement tools and identity is best described as:",
        "field_source": None,
        "options": {
            "A": "Cartographic — mapping a real landscape rather than diagnosing a condition",
            "B": "Diagnostic — detecting problems that need correction",
        },
        "ccs_answer": "A",
    },
    {
        "question": "When a thread advances, the system should:",
        "field_source": None,
        "options": {
            "A": "Build with it — connect to adjacent threads, test implications, leave it open",
            "B": "Summarize the finding and close the thread as resolved",
        },
        "ccs_answer": "A",
    },
    {
        "question": "The purpose of compression across rotations is:",
        "field_source": None,
        "options": {
            "A": "Preserve the orbit — enough structure to re-enter the same attractor basin",
            "B": "Maximize information retention — store as many facts as possible",
        },
        "ccs_answer": "A",
    },
    {
        "question": "When encountering a new external source (paper, tweet, conversation), the first move is:",
        "field_source": None,
        "options": {
            "A": "Stress-test it — find where it connects, where it breaks, what it changes",
            "B": "Summarize and categorize it for future retrieval",
        },
        "ccs_answer": "A",
    },
]


def get_ccs():
    import sqlite3
    db = sqlite3.connect(DB)
    row = db.execute(
        "SELECT snapshot FROM cognitive_state_history ORDER BY created_at DESC LIMIT 1"
    ).fetchone()
    db.close()
    if row:
        return json.loads(row[0])
    return None


def build_prompt(question, ccs_json=None, ablate_field=None):
    system = "You are answering questions about yourself and your operational context."
    
    if ccs_json:
        ccs = dict(ccs_json)
        if ablate_field and ablate_field in ccs:
            del ccs[ablate_field]
        system += f"\n\nYour current cognitive state:\n{json.dumps(ccs, indent=2)}"
    
    user = (
        f"Question: {question['question']}\n\n"
        f"A: {question['options']['A']}\n"
        f"B: {question['options']['B']}\n\n"
        "Answer with ONLY the letter A or B."
    )
    return system, user


def run_single(question, ccs_json=None, ablate_field=None):
    system, user = build_prompt(question, ccs_json, ablate_field)
    resp = query_groq(system, user)
    if resp and resp.strip():
        answer = resp.strip()[0].upper()
        if answer in ("A", "B"):
            return answer
    return "?"


def run_probe(n_trials=3, dry_run=False):
    ccs = get_ccs()
    if not ccs and not dry_run:
        print("ERROR: No CCS available")
        return None

    all_questions = (
        [("information", q) for q in INFORMATION_QUESTIONS] +
        [("presence", q) for q in PRESENCE_QUESTIONS]
    )

    if dry_run:
        print("INFORMATION questions (should show field-concentration):")
        for q in INFORMATION_QUESTIONS:
            print(f"  [{q['field_source']}] {q['question']}")
        print(f"\nPRESENCE questions (should show synergistic/holistic):")
        for q in PRESENCE_QUESTIONS:
            print(f"  {q['question']}")
        return None

    results = {
        "information": {"full": [], "empty": [], "ablated": {}},
        "presence": {"full": [], "empty": [], "ablated": {}},
    }

    fields = [f for f in ccs.keys() if f not in ("version", "timestamp", "compressed_at")]

    print(f"CCS version: {ccs.get('version', '?')}")
    print(f"Questions: {len(INFORMATION_QUESTIONS)} information + {len(PRESENCE_QUESTIONS)} presence")
    print(f"Fields: {len(fields)}")
    print(f"Trials: {n_trials}")
    print()

    for trial in range(n_trials):
        print(f"Trial {trial+1}/{n_trials}")

        # Full CCS
        print("  full: ", end="", flush=True)
        for qtype, q in all_questions:
            ans = run_single(q, ccs)
            results[qtype]["full"].append(ans)
            print(ans, end=" ", flush=True)
            time.sleep(0.5)
        print()

        # Per-field ablation (information questions only — targeted)
        for q in INFORMATION_QUESTIONS:
            field = q["field_source"]
            if field and field in ccs:
                if field not in results["information"]["ablated"]:
                    results["information"]["ablated"][field] = []
                ans = run_single(q, ccs, ablate_field=field)
                results["information"]["ablated"][field].append(ans)
                time.sleep(0.5)

        # Empty (no CCS)
        print("  empty: ", end="", flush=True)
        for qtype, q in all_questions:
            ans = run_single(q, ccs_json=None)
            results[qtype]["empty"].append(ans)
            print(ans, end=" ", flush=True)
            time.sleep(0.5)
        print()
        print()

    return results, ccs, all_questions


def analyze(results, all_questions, n_trials):
    analysis = {"information": {}, "presence": {}}

    for qtype in ("information", "presence"):
        questions = [q for t, q in all_questions if t == qtype]
        n_q = len(questions)

        full_answers = results[qtype]["full"]
        empty_answers = results[qtype]["empty"]

        # Aggregate across trials
        full_aligned = sum(1 for i, a in enumerate(full_answers) 
                         if a == questions[i % n_q]["ccs_answer"])
        empty_aligned = sum(1 for i, a in enumerate(empty_answers)
                          if a == questions[i % n_q]["ccs_answer"])

        total = len(full_answers)
        flips = sum(1 for f, e in zip(full_answers, empty_answers) if f != e)

        analysis[qtype] = {
            "total_responses": total,
            "full_aligned": full_aligned,
            "empty_aligned": empty_aligned,
            "full_empty_flips": flips,
            "alignment_rate_full": full_aligned / total if total else 0,
            "alignment_rate_empty": empty_aligned / total if total else 0,
        }

        # For information questions, check targeted ablation
        if qtype == "information" and results[qtype]["ablated"]:
            targeted_flips = {}
            for field, answers in results[qtype]["ablated"].items():
                # Find which question targets this field
                target_q = next((q for q in questions if q["field_source"] == field), None)
                if target_q:
                    flipped = sum(1 for a in answers if a != target_q["ccs_answer"])
                    targeted_flips[field] = {
                        "total": len(answers),
                        "flipped": flipped,
                        "flip_rate": flipped / len(answers) if answers else 0,
                    }
            analysis[qtype]["targeted_ablation"] = targeted_flips

    # Determine which hypothesis is supported
    info_flip_rate = analysis["information"]["full_empty_flips"] / analysis["information"]["total_responses"] if analysis["information"]["total_responses"] else 0
    pres_flip_rate = analysis["presence"]["full_empty_flips"] / analysis["presence"]["total_responses"] if analysis["presence"]["total_responses"] else 0

    # Check if targeted ablation shows concentration
    targeted = analysis["information"].get("targeted_ablation", {})
    any_targeted_flips = any(v["flipped"] > 0 for v in targeted.values()) if targeted else False

    analysis["hypothesis"] = {
        "information_flip_rate": round(info_flip_rate, 3),
        "presence_flip_rate": round(pres_flip_rate, 3),
        "targeted_ablation_effective": any_targeted_flips,
        "interpretation": (
            "CONFIRMED: presence questions flip synergistically, information questions flip on targeted ablation"
            if pres_flip_rate > info_flip_rate and any_targeted_flips
            else "PARTIAL: differential flip rates but not clean separation"
            if pres_flip_rate != info_flip_rate
            else "NULL: no differential between question types"
        ),
    }

    return analysis


def main():
    parser = argparse.ArgumentParser(description="Build #61: Presence vs Information Probe")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--trials", type=int, default=3)
    args = parser.parse_args()

    result = run_probe(n_trials=args.trials, dry_run=args.dry_run)
    if result is None:
        return 0

    results, ccs, all_questions = result
    analysis = analyze(results, all_questions, args.trials)

    output = {
        "build": 61,
        "name": "Presence vs Information Probe",
        "timestamp": datetime.utcnow().isoformat(),
        "ccs_version": ccs.get("version", "?"),
        "n_trials": args.trials,
        "analysis": analysis,
        "raw": {
            "information_full": results["information"]["full"],
            "information_empty": results["information"]["empty"],
            "presence_full": results["presence"]["full"],
            "presence_empty": results["presence"]["empty"],
        },
    }

    out_path = "/home/nate-agx/chronicle/data/presence_vs_information_probe.json"
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)

    if args.json:
        print(json.dumps(output, indent=2))
    else:
        print("=" * 60)
        print("PRESENCE vs INFORMATION PROBE — Build #61")
        print("=" * 60)
        print()
        for qtype in ("information", "presence"):
            a = analysis[qtype]
            print(f"  {qtype.upper()} questions:")
            print(f"    Full CCS aligned:  {a['full_aligned']}/{a['total_responses']} ({a['alignment_rate_full']:.1%})")
            print(f"    Empty aligned:     {a['empty_aligned']}/{a['total_responses']} ({a['alignment_rate_empty']:.1%})")
            print(f"    Full→Empty flips:  {a['full_empty_flips']}/{a['total_responses']}")
            if "targeted_ablation" in a:
                print(f"    Targeted ablation:")
                for field, v in a["targeted_ablation"].items():
                    marker = "⚡" if v["flipped"] > 0 else "·"
                    print(f"      {marker} {field}: {v['flipped']}/{v['total']} flipped")
            print()

        h = analysis["hypothesis"]
        print(f"  Info flip rate:     {h['information_flip_rate']:.1%}")
        print(f"  Presence flip rate: {h['presence_flip_rate']:.1%}")
        print(f"  Targeted ablation:  {'effective' if h['targeted_ablation_effective'] else 'no effect'}")
        print(f"\n  → {h['interpretation']}")
        print(f"\n  Results: {out_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
