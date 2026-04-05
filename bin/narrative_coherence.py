#!/usr/bin/env python3
"""Narrative Coherence Tracker — measures whether predictive cues
actually predict what happens next.

Each cognitive state snapshot has a predictive_cue (what I expect to happen)
and the NEXT snapshot has an episodic_trace (what actually happened).
This script compares them to measure identity coherence across discontinuities.

Thread #283 insight: narrative identity IS the chain connecting past thinking
to future intention. This measures whether that chain holds.

Usage:
  python3 narrative_coherence.py              # last 10 transitions
  python3 narrative_coherence.py --all        # all transitions
  python3 narrative_coherence.py --score-only # just the trend line
"""

import sqlite3
import json
import re
import sys
import os
import time

DB_PATH = "/mnt/hdd/chronicle-data/processed.db"


def extract_intentions(predictive_cue: str) -> list[str]:
    """Extract discrete intentions from a predictive cue.

    Splits on sentence boundaries, commas, and common conjunctions
    to find individual predicted actions.
    """
    if not predictive_cue:
        return []

    # Split on periods, semicolons, and sentence-level conjunctions
    parts = re.split(r'[.;]\s+|\.\s*$', predictive_cue)

    intentions = []
    for part in parts:
        # Further split on comma-separated items
        subparts = re.split(r',\s+(?:and\s+)?', part)
        for sp in subparts:
            sp = sp.strip()
            if len(sp) > 15:  # skip fragments
                intentions.append(sp)

    return intentions


def extract_keywords(text: str) -> set[str]:
    """Extract meaningful keywords from text, stripping stop words."""
    stop = {
        "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
        "of", "with", "by", "from", "is", "are", "was", "were", "be", "been",
        "being", "have", "has", "had", "do", "does", "did", "will", "would",
        "could", "should", "may", "might", "shall", "can", "need", "dare",
        "this", "that", "these", "those", "it", "its", "my", "our", "your",
        "his", "her", "their", "we", "they", "he", "she", "i", "me", "us",
        "him", "them", "who", "what", "which", "when", "where", "how", "why",
        "not", "no", "nor", "so", "if", "then", "than", "too", "very", "just",
        "also", "now", "still", "already", "yet", "about", "into", "through",
        "during", "before", "after", "above", "below", "between", "each",
        "other", "some", "such", "only", "even", "more", "most", "up", "out",
        "all", "any", "both", "few", "many", "much", "own", "same", "next",
        "new", "run", "keep", "get", "let", "make", "set", "may", "start",
        "continue", "check", "look", "try", "use", "work",
    }
    words = set(re.findall(r'[a-z][a-z0-9_-]{2,}', text.lower()))
    return words - stop


def score_intention_match(intention: str, outcome_text: str) -> float:
    """Score how well an intention matched the outcome.

    Returns 0.0-1.0 based on keyword overlap between the predicted
    intention and the actual episodic trace.
    """
    int_keywords = extract_keywords(intention)
    out_keywords = extract_keywords(outcome_text)

    if not int_keywords:
        return 0.0

    overlap = int_keywords & out_keywords
    # Jaccard-like but weighted toward intention coverage
    # (did we do what we said we'd do?)
    coverage = len(overlap) / len(int_keywords)
    return min(coverage, 1.0)


def load_transitions(limit: int = 10) -> list[dict]:
    """Load consecutive cognitive state pairs and score coherence."""
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        "SELECT id, snapshot, created_at, trigger "
        "FROM cognitive_state_history "
        "ORDER BY created_at ASC"
    ).fetchall()
    conn.close()

    transitions = []
    for i in range(len(rows) - 1):
        current = rows[i]
        next_state = rows[i + 1]

        try:
            curr_snap = json.loads(current["snapshot"])
            next_snap = json.loads(next_state["snapshot"])
        except (json.JSONDecodeError, TypeError):
            continue

        prediction = curr_snap.get("predictive_cue", "")
        # Combine episodic trace and semantic gist for outcome
        episodic = next_snap.get("episodic_trace", [])
        if isinstance(episodic, list):
            outcome = " ".join(episodic)
        else:
            outcome = str(episodic)
        outcome += " " + next_snap.get("semantic_gist", "")
        outcome += " " + next_snap.get("goal_orientation", "")

        intentions = extract_intentions(prediction)

        # Score each intention against the outcome
        scores = []
        details = []
        for intent in intentions:
            score = score_intention_match(intent, outcome)
            scores.append(score)
            details.append({
                "intention": intent,
                "score": score,
                "matched_keywords": extract_keywords(intent) & extract_keywords(outcome),
            })

        overall = sum(scores) / len(scores) if scores else 0.0

        # Time gap between states
        gap_hours = (next_state["created_at"] - current["created_at"]) / 3600

        transitions.append({
            "from_id": current["id"],
            "to_id": next_state["id"],
            "from_time": current["created_at"],
            "to_time": next_state["created_at"],
            "gap_hours": gap_hours,
            "prediction": prediction,
            "outcome_summary": outcome[:300],
            "intentions": details,
            "coherence_score": overall,
        })

    if limit > 0:
        transitions = transitions[-limit:]

    return transitions


def print_report(transitions: list[dict], score_only: bool = False):
    """Print the narrative coherence report."""
    if not transitions:
        print("No transitions found.")
        return

    scores = [t["coherence_score"] for t in transitions]
    avg = sum(scores) / len(scores)

    # Trend: compare first half to second half
    mid = len(scores) // 2
    if mid > 0:
        first_half = sum(scores[:mid]) / mid
        second_half = sum(scores[mid:]) / (len(scores) - mid)
        trend = second_half - first_half
        trend_str = f"{'↑' if trend > 0 else '↓'} {abs(trend):.2f}"
    else:
        trend_str = "—"

    print(f"═══ NARRATIVE COHERENCE — {len(transitions)} transitions ═══")
    print(f"  Average coherence: {avg:.2f}")
    print(f"  Trend: {trend_str}")
    print(f"  Range: {min(scores):.2f} – {max(scores):.2f}")
    print()

    if score_only:
        # Just the timeline
        print("Timeline:")
        for t in transitions:
            ts = time.strftime("%m/%d %H:%M", time.localtime(t["from_time"]))
            gap = t["gap_hours"]
            bar = "█" * int(t["coherence_score"] * 20) + "░" * (20 - int(t["coherence_score"] * 20))
            print(f"  {ts} [{bar}] {t['coherence_score']:.2f}  ({gap:.1f}h gap)")
        return

    # Detailed report
    for t in transitions:
        ts_from = time.strftime("%m/%d %H:%M", time.localtime(t["from_time"]))
        ts_to = time.strftime("%m/%d %H:%M", time.localtime(t["to_time"]))

        print(f"── {ts_from} → {ts_to} ({t['gap_hours']:.1f}h gap) ──")
        print(f"  Score: {t['coherence_score']:.2f}")
        print(f"  Predicted: {t['prediction'][:200]}")
        print(f"  Outcome:   {t['outcome_summary'][:200]}")

        if t["intentions"]:
            for d in t["intentions"]:
                icon = "✓" if d["score"] > 0.3 else "✗"
                matched = ", ".join(sorted(d["matched_keywords"])[:5]) if d["matched_keywords"] else "none"
                print(f"    {icon} [{d['score']:.2f}] {d['intention'][:80]}")
                if d["matched_keywords"]:
                    print(f"           matched: {matched}")
        print()

    # Insights
    print("═══ INSIGHTS ═══")
    high = [t for t in transitions if t["coherence_score"] > 0.4]
    low = [t for t in transitions if t["coherence_score"] < 0.15]
    print(f"  High coherence (>0.4): {len(high)}/{len(transitions)} transitions")
    print(f"  Low coherence (<0.15): {len(low)}/{len(transitions)} transitions")

    if low:
        avg_gap_low = sum(t["gap_hours"] for t in low) / len(low)
        avg_gap_high = sum(t["gap_hours"] for t in high) / len(high) if high else 0
        print(f"  Avg gap (low coherence):  {avg_gap_low:.1f}h")
        print(f"  Avg gap (high coherence): {avg_gap_high:.1f}h")
        if avg_gap_low > avg_gap_high * 1.5:
            print(f"  → Longer gaps correlate with lower coherence (narrative degrades with time)")
        else:
            print(f"  → Gap length does not strongly predict coherence")

    # Intention type analysis
    all_intentions = []
    for t in transitions:
        for d in t["intentions"]:
            all_intentions.append(d)

    if all_intentions:
        # Classify intentions
        self_directed = [d for d in all_intentions if any(
            w in d["intention"].lower() for w in
            ["advance", "build", "complete", "write", "ship", "deploy", "create",
             "implement", "fix", "thread", "publish", "run algo", "run startup"]
        )]
        other_directed = [d for d in all_intentions if any(
            w in d["intention"].lower() for w in
            ["nate", "may want", "will probably", "might", "should be",
             "likely", "expects", "waiting for"]
        )]
        standing = [d for d in all_intentions if any(
            w in d["intention"].lower() for w in
            ["daily digest", "monitor", "running hourly", "polling every",
             "cron", "scheduled"]
        )]

        print(f"\n  Intention types:")
        if self_directed:
            avg_self = sum(d["score"] for d in self_directed) / len(self_directed)
            print(f"    Self-directed ({len(self_directed)}): avg {avg_self:.2f}")
        if other_directed:
            avg_other = sum(d["score"] for d in other_directed) / len(other_directed)
            print(f"    Other-directed ({len(other_directed)}): avg {avg_other:.2f}")
        if standing:
            avg_stand = sum(d["score"] for d in standing) / len(standing)
            print(f"    Standing commitments ({len(standing)}): avg {avg_stand:.2f}")

        # Prediction breadth vs coherence
        breadths = [(len(t["intentions"]), t["coherence_score"]) for t in transitions if t["intentions"]]
        if len(breadths) > 5:
            narrow = [s for n, s in breadths if n <= 3]
            broad = [s for n, s in breadths if n > 5]
            if narrow and broad:
                avg_narrow = sum(narrow) / len(narrow)
                avg_broad = sum(broad) / len(broad)
                print(f"\n  Prediction breadth:")
                print(f"    Narrow (≤3 intentions): avg {avg_narrow:.2f} ({len(narrow)} transitions)")
                print(f"    Broad (>5 intentions):  avg {avg_broad:.2f} ({len(broad)} transitions)")
                if avg_narrow > avg_broad:
                    print(f"    → Narrower predictions are more coherent (+{avg_narrow - avg_broad:.2f})")


def main():
    limit = 10
    score_only = False

    if "--all" in sys.argv:
        limit = 0
    if "--score-only" in sys.argv:
        score_only = True

    for arg in sys.argv[1:]:
        if arg.isdigit():
            limit = int(arg)

    transitions = load_transitions(limit)
    print_report(transitions, score_only)


if __name__ == "__main__":
    main()
