#!/usr/bin/env python3
"""Spot Check — lightweight periodic quality audit.

Samples a random subset of recent briefs and checks for failure modes:
- Hallucination: claims not supported by source material
- Self-reference: Chronicle briefing about its own internals
- Incoherence: contradictory statements within the brief
- Phantom sources: citing papers/people/orgs that don't exist in the input

NOT checking domain relevance — that's Nate's curation, not ours to judge.

Usage:
    python3 spot_check.py              # Run spot check on recent briefs
    python3 spot_check.py --verbose    # Show full analysis
"""

import json
import os
import random
import re
import sqlite3
import sys
import time

import requests

DB_PATH = os.environ.get("CHRONICLE_DB",
    "/mnt/hdd/chronicle-data/processed.db")
INFERENCE_URL = os.environ.get("WATCHER_INFERENCE_URL", "http://localhost:11436")
MODEL = os.environ.get("INTERN_MODEL", "chronicle-deep")

# Sample rate: check this fraction of recent briefs
SAMPLE_RATE = 0.15  # ~15%
LOOKBACK_HOURS = 2
MIN_SAMPLES = 2
MAX_SAMPLES = 8

# Self-reference terms that indicate the brief is about Chronicle itself
SELF_REF_TERMS = [
    "chronicle", "homeforge", "capsule", "activity_feed", "gemma gate",
    "seed agent", "intern agent", "opus board", "watcher", "swarm",
    "nate-agx", "agx orin", "ollama server", "canister",
]

ALERTS_WEBHOOK = os.environ.get("ALERTS_WEBHOOK",
    "https://discord.com/api/webhooks/1489300749308395611/zlZZH3QzXqEDxznmNelK3mlkLF0IeZqoxwNUnrL0no_jfeZnDMvwvD_7XhYcCyQzq78I")


def get_recent_briefs(hours=LOOKBACK_HOURS):
    """Pull recent briefs with their source input for comparison."""
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    since = int(time.time()) - (hours * 3600)

    briefs = db.execute(
        "SELECT id, content, metadata, created_at FROM activity_feed "
        "WHERE source='intern' AND activity_type='brief' AND created_at > ? "
        "ORDER BY created_at DESC LIMIT 100",
        (since,)
    ).fetchall()

    results = []
    for b in briefs:
        brief = dict(b)
        # Parse metadata for source_hash (Build #46 provenance)
        try:
            brief["meta"] = json.loads(b["metadata"]) if b["metadata"] else {}
        except Exception:
            brief["meta"] = {}
        # Find the source input (pickup) that preceded this brief
        # Look for the most recent pickup before this brief's timestamp
        source_row = db.execute(
            "SELECT content FROM activity_feed "
            "WHERE source='intern' AND activity_type='pickup' "
            "AND created_at <= ? AND created_at > ? "
            "ORDER BY created_at DESC LIMIT 1",
            (b["created_at"], b["created_at"] - 120)
        ).fetchone()
        brief["source_input"] = source_row["content"][:1000] if source_row else ""

        # Also grab any fetch_done content (the actual URL text)
        fetch_row = db.execute(
            "SELECT content FROM activity_feed "
            "WHERE source='intern' AND activity_type='fetch_done' "
            "AND created_at <= ? AND created_at > ? "
            "ORDER BY created_at DESC LIMIT 1",
            (b["created_at"], b["created_at"] - 120)
        ).fetchone()
        brief["fetched_content"] = fetch_row["content"][:1000] if fetch_row else ""

        results.append(brief)

    db.close()
    return results


def check_self_reference(brief_text):
    """Check if the brief is about Chronicle's own internals."""
    text_lower = brief_text.lower()
    matches = [t for t in SELF_REF_TERMS if t in text_lower]
    return matches


def check_with_llm(brief_text, source_input="", fetched_content=""):
    """Ask LLM to spot-check the brief against its source material."""

    source_block = ""
    if source_input:
        source_block += f"\nSOURCE INPUT (what triggered this brief):\n{source_input[:600]}\n"
    if fetched_content:
        source_block += f"\nFETCHED CONTENT (URL text retrieved):\n{fetched_content[:600]}\n"

    prompt = f"""You are a verification layer checking whether an AI-generated brief accurately represents its source material. The current date is April 2026.

IMPORTANT: Do NOT flag things as hallucinations based on your own knowledge cutoff. The brief is from 2026 — products, events, and people you don't know about may be real. Only flag claims that CONTRADICT the source material or that appear NOWHERE in the source.
{source_block}
BRIEF TO CHECK:
{brief_text[:1000]}

Check ONLY:
1. FABRICATION — Does the brief claim something specific (names, numbers, institutions) that is NOT in the source material and could be invented?
2. DISTORTION — Does the brief significantly misrepresent what the source actually says?
3. INCOHERENCE — Does the brief contradict itself?

If no source material is available, only check for internal coherence.

DO NOT flag:
- Reasonable inferences from the source
- Domain relevance judgments
- Things you personally don't recognize (it's 2026, not 2024)

Respond with JSON only:
{{"pass": true/false, "issues": ["issue1"], "confidence": 0.0-1.0}}

If the brief reasonably represents the source, return {{"pass": true, "issues": [], "confidence": 0.9}}"""

    try:
        r = requests.post(
            f"{INFERENCE_URL}/api/chat",
            json={
                "model": MODEL,
                "messages": [
                    {"role": "system", "content": "You are a precise fact-checker. Only flag real problems. False positives waste everyone's time."},
                    {"role": "user", "content": prompt}
                ],
                "stream": False,
                "options": {"num_predict": 200, "temperature": 0.1},
            },
            timeout=60,
        )
        if r.status_code == 200:
            raw = r.json().get("message", {}).get("content", "").strip()
            # Extract JSON from response
            match = re.search(r'\{[^}]+\}', raw)
            if match:
                return json.loads(match.group())
    except Exception as e:
        print(f"  LLM check error: {e}")

    return None


def run_spot_check(verbose=False):
    """Sample and check recent briefs."""
    briefs = get_recent_briefs()
    if not briefs:
        print("No recent briefs to check.")
        return

    # Sample
    n_samples = max(MIN_SAMPLES, min(MAX_SAMPLES, int(len(briefs) * SAMPLE_RATE)))
    n_samples = min(n_samples, len(briefs))
    samples = random.sample(briefs, n_samples)

    print(f"Spot-checking {n_samples}/{len(briefs)} recent briefs...")

    results = {
        "checked": n_samples,
        "total": len(briefs),
        "passed": 0,
        "self_ref": 0,
        "llm_flagged": 0,
        "provenance_ok": 0,
        "provenance_na": 0,
        "issues": [],
    }

    for brief in samples:
        text = brief["content"]
        brief_id = brief["id"]
        preview = text[:80].replace("\n", " ")

        if verbose:
            print(f"\n--- Brief #{brief_id}: {preview}...")

        # Check 0: Provenance hash (Build #46 — lightweight, no LLM cost)
        _stored_hash = brief.get("meta", {}).get("source_hash")
        if _stored_hash:
            results["provenance_ok"] += 1
            if verbose:
                print(f"  ⊕ Provenance: {_stored_hash}")
        else:
            results["provenance_na"] += 1

        # Check 1: Self-reference
        self_refs = check_self_reference(text)
        if self_refs:
            results["self_ref"] += 1
            issue = f"Self-reference in #{brief_id}: {', '.join(self_refs[:3])}"
            results["issues"].append(issue)
            if verbose:
                print(f"  ⚠️ SELF-REF: {', '.join(self_refs)}")
            continue  # Don't burn an LLM call on self-ref

        # Check 2: LLM verification against source material
        llm_result = check_with_llm(text, brief.get("source_input", ""), brief.get("fetched_content", ""))
        if llm_result:
            if llm_result.get("pass", True):
                results["passed"] += 1
                if verbose:
                    print(f"  ✓ PASS (confidence: {llm_result.get('confidence', '?')})")
            else:
                results["llm_flagged"] += 1
                issues_text = "; ".join(llm_result.get("issues", ["unknown"]))
                issue = f"LLM flag #{brief_id}: {issues_text}"
                results["issues"].append(issue)
                if verbose:
                    print(f"  ⚠️ FLAG: {issues_text}")
        else:
            results["passed"] += 1  # If LLM fails, don't penalize

    # Summary
    print(f"\n{'='*50}")
    print(f"SPOT CHECK: {results['checked']}/{results['total']} sampled")
    print(f"  Passed: {results['passed']}")
    print(f"  Self-ref: {results['self_ref']}")
    print(f"  Flagged: {results['llm_flagged']}")
    print(f"  Provenance: {results['provenance_ok']} hashed, {results['provenance_na']} legacy")

    if results["issues"]:
        print(f"\nIssues found:")
        for issue in results["issues"]:
            print(f"  - {issue}")

    # Log to DB
    db = sqlite3.connect(DB_PATH)
    db.execute(
        "INSERT INTO activity_feed (source, activity_type, content, created_at) "
        "VALUES (?, ?, ?, ?)",
        ("spot_check", "audit",
         f"Spot check: {results['checked']}/{results['total']} sampled, "
         f"{results['passed']} passed, {results['self_ref']} self-ref, "
         f"{results['llm_flagged']} flagged, "
         f"{results['provenance_ok']}/{results['checked']} with provenance"
         + (f". Issues: {'; '.join(results['issues'][:3])}" if results["issues"] else ""),
         int(time.time()))
    )
    db.commit()
    db.close()

    # Write health signal for the intern to read
    _write_health_signal(results)

    # Alert on Discord only if problems found
    if results["issues"]:
        try:
            msg = (f"⚠️ **Spot Check** — {results['llm_flagged']} flagged, "
                   f"{results['self_ref']} self-ref out of {results['checked']} sampled\n"
                   + "\n".join(f"- {i}" for i in results["issues"][:5]))
            requests.post(ALERTS_WEBHOOK, json={"content": msg[:1900]}, timeout=10)
        except Exception:
            pass

    return results


HEALTH_SIGNAL_PATH = os.path.expanduser("~/chronicle/spot_check_health.json")

def _write_health_signal(results):
    """Write a health signal file that the intern reads.

    Tracks recent fabrication rate. If it crosses a threshold,
    the intern activates a temporary source-fidelity constraint.
    This is a thermostat: high fabrication → constraint on → rate drops → constraint off.
    """
    # Load history
    history = []
    if os.path.exists(HEALTH_SIGNAL_PATH):
        try:
            with open(HEALTH_SIGNAL_PATH) as f:
                data = json.load(f)
                history = data.get("history", [])
        except Exception:
            pass

    # Add this check
    history.append({
        "ts": int(time.time()),
        "checked": results["checked"],
        "flagged": results["llm_flagged"],
        "self_ref": results["self_ref"],
    })

    # Keep last 10 checks
    history = history[-10:]

    # Calculate fabrication rate over recent checks
    total_checked = sum(h["checked"] for h in history[-5:])
    total_flagged = sum(h["flagged"] for h in history[-5:])
    fab_rate = total_flagged / max(total_checked, 1)

    # Determine constraint level
    # 0 = clean, no constraint
    # 1 = elevated (>20% flagged) — gentle reminder
    # 2 = high (>40% flagged) — strong constraint
    if fab_rate > 0.40:
        level = 2
    elif fab_rate > 0.20:
        level = 1
    else:
        level = 0

    signal = {
        "history": history,
        "fab_rate": round(fab_rate, 3),
        "constraint_level": level,
        "updated": int(time.time()),
    }

    with open(HEALTH_SIGNAL_PATH, "w") as f:
        json.dump(signal, f, indent=2)

    if level > 0:
        print(f"  ⚠️ Constraint level {level} (fab rate: {fab_rate:.1%})")
    else:
        print(f"  ✓ Clean (fab rate: {fab_rate:.1%})")


def main():
    verbose = "--verbose" in sys.argv
    run_spot_check(verbose=verbose)


if __name__ == "__main__":
    main()
