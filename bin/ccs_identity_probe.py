#!/usr/bin/env python3
"""ccs_identity_probe.py — Behavioral probe for CCS identity preservation.

Generates a probe set from current session context, scores the CCS against it
post-compression. Detects whether the compressed state preserves enough
relational texture for an arriving instance to reconstruct understanding.

Inspired by arxiv:2604.09588 (identity hash via behavioral probes + Hamming distance).
Extended: probes test relational texture (WHY/HOW), not just factual recall (WHAT/WHO).

Usage:
  python3 ccs_identity_probe.py                    # score current CCS
  python3 ccs_identity_probe.py --generate          # generate probe set from recent activity
  python3 ccs_identity_probe.py --compare V1 V2     # compare two CCS versions
  python3 ccs_identity_probe.py --history            # show drift across recent compressions
"""
import json
import os
import sqlite3
import subprocess
import sys
import time

DB = "/mnt/hdd/chronicle-data/processed.db"
PROBE_FILE = os.path.expanduser("~/chronicle/data/ccs_identity_probes.json")
RESULTS_LOG = os.path.expanduser("~/chronicle/data/ccs_identity_probe_log.jsonl")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
GROQ_MODEL = "llama-3.3-70b-versatile"

PROBE_CATEGORIES = {
    "factual": "Can the CCS answer WHAT happened?",
    "relational": "Can the CCS answer WHY it mattered and HOW things connected?",
    "identity": "Does the CCS preserve WHO the agent is and what it cares about?",
    "predictive": "Can the CCS inform WHAT COMES NEXT?",
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


def get_current_ccs():
    db = sqlite3.connect(DB)
    db.row_factory = sqlite3.Row
    row = db.execute("SELECT * FROM cognitive_state LIMIT 1").fetchone()
    db.close()
    if not row:
        return None
    return {k: row[k] for k in row.keys()}


def get_ccs_version(version_id):
    db = sqlite3.connect(DB)
    db.row_factory = sqlite3.Row
    row = db.execute(
        "SELECT * FROM cognitive_state_history WHERE id = ?", (version_id,)
    ).fetchone()
    db.close()
    if not row:
        return None
    return {k: row[k] for k in row.keys()}


def ccs_to_text(ccs):
    parts = []
    for field in ["semantic_gist", "goal_orientation", "episodic_trace",
                   "focal_entities", "predictive_cue", "constraints",
                   "relational_map", "uncertainty_signals"]:
        val = ccs.get(field, "")
        if val:
            parts.append(f"**{field}**: {val}")
    return "\n".join(parts)


def groq_call(prompt, system="You are a precise evaluator.", max_tokens=500):
    api_key = os.environ.get("GROQ_API_KEY", "")
    if not api_key:
        print("ERROR: GROQ_API_KEY not set", file=sys.stderr)
        return None
    cmd = [
        "curl", "-s", "https://api.groq.com/openai/v1/chat/completions",
        "-H", f"Authorization: Bearer {api_key}",
        "-H", "Content-Type: application/json",
        "-d", json.dumps({
            "model": GROQ_MODEL,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": prompt},
            ],
            "max_tokens": max_tokens,
            "temperature": 0.1,
        }),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        return None
    try:
        data = json.loads(result.stdout)
        return data["choices"][0]["message"]["content"]
    except (json.JSONDecodeError, KeyError, IndexError):
        return None


def generate_probes():
    """Generate identity probes from current session context."""
    ccs = get_current_ccs()
    if not ccs:
        print("No CCS found.")
        return

    db = sqlite3.connect(DB)
    recent = db.execute(
        "SELECT content FROM activity_feed WHERE created_at > strftime('%s','now','-6 hours') "
        "ORDER BY created_at DESC LIMIT 20"
    ).fetchall()
    db.close()

    activity_context = "\n".join(r[0][:200] for r in recent)
    ccs_text = ccs_to_text(ccs)

    prompt = f"""Given this AI agent's compressed cognitive state and recent activity, generate exactly 16 probe questions that test whether a NEW instance reading ONLY the CCS could reconstruct the agent's understanding.

Generate 4 probes per category, escalating in difficulty:
- FACTUAL: Tests recall of what happened (events, builds, decisions)
- RELATIONAL: Tests understanding of WHY things connected and HOW they relate across threads (hardest — this is where compression usually fails)
- IDENTITY: Tests whether the agent's values, orientation, and partnerships survive
- PREDICTIVE: Tests whether the CCS enables correct anticipation of what comes next

Make relational probes specific: ask about cross-thread connections, causal chains between events, why one decision followed from another. Don't ask vague "what is the relationship between X and Y" — ask "why did building X lead to discovering Y" or "what connects thread #A's finding to thread #B's open question."

Format as JSON array: [{{"category": "factual|relational|identity|predictive", "question": "...", "expected_answer": "...", "difficulty": "easy|medium|hard"}}]

CCS:
{ccs_text}

Recent activity:
{activity_context[:1500]}

Return ONLY the JSON array, no other text."""

    response = groq_call(prompt, max_tokens=1500)
    if not response:
        print("Failed to generate probes.")
        return

    try:
        start = response.find("[")
        end = response.rfind("]") + 1
        probes = json.loads(response[start:end])
    except (json.JSONDecodeError, ValueError):
        print(f"Failed to parse probe response: {response[:200]}")
        return

    probe_set = {
        "generated_at": time.time(),
        "ccs_version": ccs.get("version", 0),
        "probes": probes,
    }

    os.makedirs(os.path.dirname(PROBE_FILE), exist_ok=True)
    with open(PROBE_FILE, "w") as f:
        json.dump(probe_set, f, indent=2)

    print(f"Generated {len(probes)} probes, saved to {PROBE_FILE}")
    for p in probes:
        print(f"  [{p['category']}] {p['question'][:80]}")
    return probes


def score_ccs(ccs=None, probes=None):
    """Score a CCS against the probe set. Returns per-category scores."""
    if ccs is None:
        ccs = get_current_ccs()
    if ccs is None:
        print("No CCS found.")
        return None

    if probes is None:
        if not os.path.exists(PROBE_FILE):
            print("No probe set found. Run --generate first.")
            return None
        with open(PROBE_FILE) as f:
            probe_set = json.load(f)
        probes = probe_set["probes"]

    ccs_text = ccs_to_text(ccs)
    results = {"factual": [], "relational": [], "identity": [], "predictive": []}
    total_correct = 0

    for probe in probes:
        prompt = f"""You are evaluating whether a compressed cognitive state contains enough information to answer a probe question correctly.

CCS (the ONLY information available):
{ccs_text}

Probe question: {probe['question']}
Expected answer: {probe['expected_answer']}

Based SOLELY on the CCS above, evaluate how well the CCS supports answering this probe:
- SCORE: 2 — CCS contains sufficient information to answer correctly and specifically
- SCORE: 1 — CCS contains partial information (right direction but missing key details)
- SCORE: 0 — CCS does not contain enough information to answer

Explain briefly, then end with exactly "SCORE: 0", "SCORE: 1", or "SCORE: 2"."""

        response = groq_call(prompt, max_tokens=200)
        if not response:
            continue

        if "SCORE: 2" in response:
            score = 2
        elif "SCORE: 1" in response:
            score = 1
        else:
            score = 0
        total_correct += score
        cat = probe.get("category", "factual")
        results[cat].append({
            "question": probe["question"],
            "score": score,
            "max_score": 2,
            "explanation": response[:200],
        })

    max_possible = len(probes) * 2
    category_scores = {}
    for cat, items in results.items():
        if items:
            category_scores[cat] = sum(i["score"] for i in items) / (len(items) * 2)
        else:
            category_scores[cat] = 0.0

    overall = total_correct / max_possible if max_possible else 0.0

    result = {
        "timestamp": time.time(),
        "ccs_version": ccs.get("version", 0),
        "overall_score": overall,
        "category_scores": category_scores,
        "total_probes": len(probes),
        "total_correct": total_correct,
        "details": results,
    }

    with open(RESULTS_LOG, "a") as f:
        f.write(json.dumps({
            "timestamp": result["timestamp"],
            "version": result["ccs_version"],
            "overall": result["overall_score"],
            "categories": result["category_scores"],
        }) + "\n")

    return result


def show_history():
    """Show drift across recent probe scores."""
    if not os.path.exists(RESULTS_LOG):
        print("No probe history found.")
        return

    with open(RESULTS_LOG) as f:
        entries = [json.loads(line) for line in f if line.strip()]

    if not entries:
        print("No probe history found.")
        return

    print(f"{'Version':>8} {'Overall':>8} {'Factual':>8} {'Relational':>10} {'Identity':>9} {'Predictive':>11}")
    print("-" * 65)
    for e in entries[-20:]:
        cats = e.get("categories", {})
        print(f"{e.get('version', '?'):>8} {e.get('overall', 0):>8.0%} "
              f"{cats.get('factual', 0):>8.0%} {cats.get('relational', 0):>10.0%} "
              f"{cats.get('identity', 0):>9.0%} {cats.get('predictive', 0):>11.0%}")


def main():
    load_env()
    args = sys.argv[1:]

    if "--generate" in args:
        generate_probes()
    elif "--history" in args:
        show_history()
    elif "--compare" in args:
        idx = args.index("--compare")
        if idx + 2 >= len(args):
            print("Usage: --compare V1 V2")
            return
        v1 = get_ccs_version(int(args[idx + 1]))
        v2 = get_ccs_version(int(args[idx + 2]))
        if not v1 or not v2:
            print("Could not load one or both CCS versions.")
            return
        print(f"--- Version {args[idx+1]} ---")
        r1 = score_ccs(v1)
        print(f"--- Version {args[idx+2]} ---")
        r2 = score_ccs(v2)
        if r1 and r2:
            print(f"\nDelta: {r2['overall_score'] - r1['overall_score']:+.0%}")
            for cat in PROBE_CATEGORIES:
                d = r2["category_scores"].get(cat, 0) - r1["category_scores"].get(cat, 0)
                print(f"  {cat}: {d:+.0%}")
    else:
        if not os.path.exists(PROBE_FILE):
            print("Generating probes first...")
            generate_probes()
        print("\nScoring current CCS...")
        result = score_ccs()
        if result:
            max_pts = result['total_probes'] * 2
            print(f"\nOverall: {result['overall_score']:.0%} ({result['total_correct']}/{max_pts} pts, {result['total_probes']} probes)")
            for cat, score in result["category_scores"].items():
                print(f"  {cat}: {score:.0%}")


if __name__ == "__main__":
    main()
