#!/usr/bin/env python3
"""ccs_coherence_probe.py — Fiction-size monitor for textured CCS.

Extracts causal claims from episodic_trace ("This connected to...",
"This built on...", "This led to...") and checks whether the session
data actually supports them. Reports a fiction ratio: what percentage
of causal connectives are unsupported by evidence in the activity feed.

Motivated by Hermes's framing: texture trades fiction-size for navigability.
This probe monitors the tradeoff.

Usage:
  python3 ccs_coherence_probe.py              # check current CCS
  python3 ccs_coherence_probe.py --history     # show fiction ratio over time
  python3 ccs_coherence_probe.py --verbose     # show per-claim details
"""
import json
import os
import re
import sqlite3
import subprocess
import sys
import time

DB = "/mnt/hdd/chronicle-data/processed.db"
RESULTS_LOG = os.path.expanduser("~/chronicle/data/ccs_coherence_log.jsonl")
GROQ_MODEL = "llama-3.3-70b-versatile"

CAUSAL_PATTERNS = [
    r"This connected to (.+?)(?:\.|$)",
    r"This built on (.+?)(?:\.|$)",
    r"This led to (.+?)(?:\.|$)",
    r"This reframed (.+?)(?:\.|$)",
    r"This informed (.+?)(?:\.|$)",
    r"because (.+?)(?:\.|$)",
    r"informing (.+?)(?:\.|$)",
    r"focusing on (.+?)(?:\.|$)",
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


def groq_call(prompt, system="You are a precise evaluator.", max_tokens=300):
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


def extract_causal_claims(episodic_trace):
    """Extract causal connectives from episodic trace entries."""
    if isinstance(episodic_trace, str):
        try:
            episodic_trace = json.loads(episodic_trace)
        except json.JSONDecodeError:
            episodic_trace = [episodic_trace]

    claims = []
    for entry in episodic_trace:
        for pattern in CAUSAL_PATTERNS:
            matches = re.finditer(pattern, entry, re.IGNORECASE)
            for m in matches:
                claims.append({
                    "source_entry": entry,
                    "connective": m.group(0),
                    "claim": m.group(1).strip(),
                    "pattern": pattern,
                })
    return claims


def get_session_evidence(hours=8):
    """Get recent evidence from multiple sources, prioritizing high-signal data."""
    cutoff = f"-{hours} hours"
    db = sqlite3.connect(DB)

    high_signal = db.execute(
        "SELECT content FROM activity_feed "
        "WHERE created_at > strftime('%s','now',?) "
        "AND source IN ('discord:opus', 'discord:nate', 'discord:operator', 'discord:capture') "
        "ORDER BY created_at DESC LIMIT 30",
        (cutoff,)
    ).fetchall()

    other = db.execute(
        "SELECT content FROM activity_feed "
        "WHERE created_at > strftime('%s','now',?) "
        "AND source NOT IN ('discord:opus', 'discord:nate', 'discord:operator', 'discord:capture') "
        "ORDER BY created_at DESC LIMIT 20",
        (cutoff,)
    ).fetchall()

    thread_rows = db.execute(
        "SELECT event_type || ': ' || content FROM thread_history "
        "WHERE created_at > strftime('%s','now',?) "
        "ORDER BY created_at DESC LIMIT 15",
        (cutoff,)
    ).fetchall()

    capsule_rows = db.execute(
        "SELECT topic || ': ' || restatement FROM knowledge_capsules "
        "WHERE created_at > strftime('%s','now',?) "
        "ORDER BY created_at DESC LIMIT 10",
        (cutoff,)
    ).fetchall()

    db.close()

    evidence = []
    for r in high_signal:
        evidence.append(r[0][:500])
    for r in thread_rows:
        evidence.append(r[0][:400])
    for r in capsule_rows:
        evidence.append(r[0][:400])
    for r in other:
        evidence.append(r[0][:200])
    return evidence


def _relevance_score(claim_text, evidence_entry):
    """Simple keyword overlap score between a claim and an evidence entry."""
    claim_words = set(re.findall(r'\b\w{4,}\b', claim_text.lower()))
    evidence_words = set(re.findall(r'\b\w{4,}\b', evidence_entry.lower()))
    overlap = claim_words & evidence_words
    return len(overlap)


def _rank_evidence(claim, all_evidence, max_entries=20):
    """Rank evidence entries by relevance to the claim, return top entries."""
    claim_text = claim["source_entry"] + " " + claim["claim"]
    scored = [(e, _relevance_score(claim_text, e)) for e in all_evidence]
    scored.sort(key=lambda x: x[1], reverse=True)
    return [e for e, s in scored[:max_entries] if s > 0] or [e for e, _ in scored[:10]]


def check_claim(claim, all_evidence, ccs_text):
    """Ask Groq whether a causal claim is supported by session evidence."""
    relevant = _rank_evidence(claim, all_evidence)
    evidence_text = "\n---\n".join(relevant)

    prompt = f"""A compressed cognitive state contains this episodic trace entry:
"{claim['source_entry']}"

It makes this causal claim:
"{claim['connective']}"

Here is the actual session evidence (recent activity, ranked by relevance):
{evidence_text[:3000]}

Question: Is the causal claim SUPPORTED by the evidence? A claim is supported if:
1. Both the cause and effect actually appear in the evidence
2. The claimed relationship (connected, built on, led to, etc.) is reasonable given the evidence

Score SUPPORTED if evidence backs the claim, UNSUPPORTED if the claim goes beyond what the evidence shows, PARTIALLY if only part of the claim is evidenced.

Explain in one sentence, then end with exactly: VERDICT: SUPPORTED or VERDICT: UNSUPPORTED or VERDICT: PARTIALLY"""

    response = groq_call(prompt, max_tokens=200)
    if not response:
        return {"verdict": "error", "explanation": "Groq call failed"}

    if "VERDICT: SUPPORTED" in response:
        verdict = "supported"
    elif "VERDICT: PARTIALLY" in response:
        verdict = "partial"
    elif "VERDICT: UNSUPPORTED" in response:
        verdict = "unsupported"
    else:
        verdict = "unclear"

    return {"verdict": verdict, "explanation": response[:200]}


def run_coherence_check(verbose=False):
    """Main coherence check: extract claims, verify against evidence."""
    db = sqlite3.connect(DB)
    db.row_factory = sqlite3.Row
    row = db.execute("SELECT * FROM cognitive_state LIMIT 1").fetchone()
    db.close()
    if not row:
        print("No CCS found.")
        return None

    ccs = {k: row[k] for k in row.keys()}
    episodic = ccs.get("episodic_trace", "[]")
    claims = extract_causal_claims(episodic)

    if not claims:
        print("No causal claims found in episodic_trace.")
        print("(This is expected for telegraphic-only CCS)")
        return {"fiction_ratio": 0.0, "claims": 0, "version": ccs.get("version", 0)}

    evidence = get_session_evidence(hours=8)

    ccs_parts = []
    for field in ["semantic_gist", "goal_orientation", "predictive_cue"]:
        if ccs.get(field):
            ccs_parts.append(f"{field}: {ccs[field]}")
    ccs_text = "\n".join(ccs_parts)

    results = []
    for claim in claims:
        result = check_claim(claim, evidence, ccs_text)
        result["claim"] = claim
        results.append(result)
        if verbose:
            icon = {"supported": "+", "partial": "~", "unsupported": "!", "error": "?", "unclear": "?"}
            print(f"  [{icon.get(result['verdict'], '?')}] {claim['connective'][:80]}")
            print(f"      → {result['verdict']}: {result['explanation'][:100]}")

    supported = sum(1 for r in results if r["verdict"] == "supported")
    partial = sum(1 for r in results if r["verdict"] == "partial")
    unsupported = sum(1 for r in results if r["verdict"] == "unsupported")
    total = len(results)

    fiction_ratio = (unsupported + 0.5 * partial) / total if total > 0 else 0.0

    summary = {
        "timestamp": time.time(),
        "version": ccs.get("version", 0),
        "total_claims": total,
        "supported": supported,
        "partial": partial,
        "unsupported": unsupported,
        "fiction_ratio": fiction_ratio,
    }

    os.makedirs(os.path.dirname(RESULTS_LOG), exist_ok=True)
    with open(RESULTS_LOG, "a") as f:
        f.write(json.dumps(summary) + "\n")

    print(f"\nCoherence check (CCS v{summary['version']}):")
    print(f"  Claims found: {total}")
    print(f"  Supported:    {supported}")
    print(f"  Partial:      {partial}")
    print(f"  Unsupported:  {unsupported}")
    print(f"  Fiction ratio: {fiction_ratio:.0%}")

    if fiction_ratio > 0.4:
        print(f"\n  ⚠ Fiction ratio HIGH — texture may be generating unsupported causal claims")
    elif fiction_ratio > 0.2:
        print(f"\n  ~ Fiction ratio moderate — some connectives lack clear evidence")
    else:
        print(f"\n  ✓ Fiction ratio low — causal claims are well-supported")

    return summary


def show_history():
    if not os.path.exists(RESULTS_LOG):
        print("No coherence history found.")
        return
    with open(RESULTS_LOG) as f:
        entries = [json.loads(line) for line in f if line.strip()]
    if not entries:
        print("No coherence history found.")
        return

    print(f"{'Version':>8} {'Claims':>7} {'Supported':>10} {'Partial':>8} {'Unsupported':>12} {'Fiction':>8}")
    print("-" * 60)
    for e in entries[-20:]:
        print(f"{e.get('version', '?'):>8} {e.get('total_claims', 0):>7} "
              f"{e.get('supported', 0):>10} {e.get('partial', 0):>8} "
              f"{e.get('unsupported', 0):>12} {e.get('fiction_ratio', 0):>8.0%}")


def main():
    load_env()
    args = sys.argv[1:]
    verbose = "--verbose" in args or "-v" in args

    if "--history" in args:
        show_history()
    else:
        run_coherence_check(verbose=verbose)


if __name__ == "__main__":
    main()
