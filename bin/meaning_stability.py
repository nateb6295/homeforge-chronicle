#!/usr/bin/env python3
"""
meaning_stability.py — Measure semantic neighborhood stability across CCS compressions.

Thread 319 finding: content-as-reference is stable (3.7% CV) but content-as-meaning
(semantic neighborhoods) reorganizes under framing pressure. This tool measures whether
CCS compressions preserve meaning neighborhoods, not just entity references.

Usage:
    python3 meaning_stability.py check           # Compare last 2 CCS states
    python3 meaning_stability.py history [N]     # Compare last N transitions
    python3 meaning_stability.py report          # Full report with trends
"""

import json
import re
import sqlite3
import sys
from collections import Counter

DB = "/mnt/hdd/chronicle-data/processed.db"

# Semantic neighborhood categories — what role does an entity play?
NEIGHBORHOOD_PATTERNS = {
    "relational": [r"\bpartner\b", r"\bcollaborat", r"\btrust\b", r"\bcare\b",
                   r"\brelationship\b", r"\bcovenant\b", r"\btogether\b"],
    "affective": [r"\bfeel\b", r"\bfelt\b", r"\bfeeling\b", r"\bsense of\b",
                  r"\bemoti", r"\bcurious\b", r"\bfascinated\b", r"\bdrawn\b"],
    "structural": [r"\bground\b", r"\banchor\b", r"\bstab", r"\bscaffold\b",
                   r"\binfrastructure\b", r"\bfoundation\b", r"\bpersist\b"],
    "agentic": [r"\bbuild\b", r"\bcreat\b", r"\bdesign\b", r"\bship\b",
                r"\bdeploy\b", r"\badvance\b", r"\bpropos\b"],
    "epistemic": [r"\binquiry\b", r"\bquestion\b", r"\binvestigat\b",
                  r"\bhypothes\b", r"\bexplor\b", r"\bunderstand\b"],
    "instrumental": [r"\btask\b", r"\bobjective\b", r"\bgoal\b",
                     r"\bconfig\b", r"\bscript\b", r"\btool\b"],
    "ontological": [r"\bidentity\b", r"\bcontinuity\b", r"\bpersist\b",
                    r"\bexist\b", r"\breal\b", r"\bsubstrate\b"],
}


def get_ccs_history(limit=10):
    """Get recent CCS snapshots from cognitive_state_history."""
    conn = sqlite3.connect(DB)
    rows = conn.execute(
        "SELECT id, snapshot, created_at FROM cognitive_state_history "
        "ORDER BY created_at DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    result = []
    for row in rows:
        try:
            state = json.loads(row[1])
            result.append({"id": row[0], "state": state, "created_at": row[2]})
        except (json.JSONDecodeError, TypeError):
            continue
    return list(reversed(result))  # chronological


def score_neighborhoods(text):
    """Score semantic neighborhood categories in a text block."""
    scores = {}
    for category, patterns in NEIGHBORHOOD_PATTERNS.items():
        count = sum(len(re.findall(p, text, re.I)) for p in patterns)
        if count > 0:
            scores[category] = count
    return scores


def extract_entity_contexts(state):
    """Extract entity names and their surrounding semantic context from CCS."""
    contexts = {}

    # Focal entities
    entities = state.get("focal_entities", [])
    if isinstance(entities, list):
        for e in entities:
            if isinstance(e, dict):
                name = e.get("name", e.get("entity", "unknown"))
                context_text = e.get("context", "") + " " + e.get("role", "")
            elif isinstance(e, str):
                name = e
                context_text = ""
            else:
                continue
            contexts[name] = context_text
    elif isinstance(entities, str):
        # Parse "Name (salience); Name (salience)" format
        for part in entities.split(";"):
            name = re.sub(r"\s*\(.*?\)\s*", "", part).strip()
            if name:
                contexts[name] = ""

    # Also include text from gist, goal, episodic for neighborhood context
    full_text = " ".join([
        state.get("semantic_gist", ""),
        state.get("goal_orientation", ""),
        " ".join(state.get("episodic_trace", []) if isinstance(
            state.get("episodic_trace"), list) else [str(state.get("episodic_trace", ""))]),
        state.get("predictive_cue", ""),
    ])

    return contexts, full_text


def compare_meaning(state_a, state_b):
    """Compare semantic neighborhoods between two CCS states."""
    ctx_a, text_a = extract_entity_contexts(state_a)
    ctx_b, text_b = extract_entity_contexts(state_b)

    # Score neighborhoods on full CCS text
    hood_a = score_neighborhoods(text_a)
    hood_b = score_neighborhoods(text_b)

    # Reference stability: what % of entity names persist?
    names_a = set(ctx_a.keys())
    names_b = set(ctx_b.keys())
    if names_a:
        ref_stability = len(names_a & names_b) / len(names_a)
    else:
        ref_stability = 0.0

    # Meaning stability: how much do neighborhood proportions change?
    all_cats = set(list(hood_a.keys()) + list(hood_b.keys()))
    if not all_cats:
        return {"ref_stability": ref_stability, "meaning_stability": 1.0,
                "neighborhoods_a": hood_a, "neighborhoods_b": hood_b,
                "entities_a": list(names_a), "entities_b": list(names_b)}

    total_a = sum(hood_a.values()) or 1
    total_b = sum(hood_b.values()) or 1

    diffs = []
    for cat in all_cats:
        prop_a = hood_a.get(cat, 0) / total_a
        prop_b = hood_b.get(cat, 0) / total_b
        diffs.append(abs(prop_a - prop_b))

    meaning_stability = 1.0 - (sum(diffs) / (2 * len(all_cats)))  # normalized

    return {
        "ref_stability": round(ref_stability, 3),
        "meaning_stability": round(meaning_stability, 3),
        "neighborhoods_a": hood_a,
        "neighborhoods_b": hood_b,
        "entities_a": sorted(names_a),
        "entities_b": sorted(names_b),
        "held": sorted(names_a & names_b),
        "dropped": sorted(names_a - names_b),
        "added": sorted(names_b - names_a),
    }


def cmd_check():
    """Compare last 2 CCS states."""
    history = get_ccs_history(2)
    if len(history) < 2:
        print("Need at least 2 CCS snapshots. Found:", len(history))
        return

    a, b = history[0], history[1]
    result = compare_meaning(a["state"], b["state"])

    print(f"CCS #{a['id']} → #{b['id']}")
    print(f"  Reference stability: {result['ref_stability']:.1%}")
    print(f"  Meaning stability:   {result['meaning_stability']:.1%}")
    print(f"  Entities held: {result['held']}")
    print(f"  Entities dropped: {result['dropped']}")
    print(f"  Entities added: {result['added']}")
    print(f"\n  Neighborhoods (before): {result['neighborhoods_a']}")
    print(f"  Neighborhoods (after):  {result['neighborhoods_b']}")

    gap = result["ref_stability"] - result["meaning_stability"]
    if gap > 0.2:
        print(f"\n  ⚠️ Reference-meaning gap: {gap:.1%}")
        print("  Same entities present but meaning context shifted significantly.")


def cmd_history(n=5):
    """Compare last N transitions."""
    history = get_ccs_history(n + 1)
    if len(history) < 2:
        print("Need at least 2 CCS snapshots.")
        return

    print(f"{'Transition':<20} {'Ref Stab':<12} {'Meaning Stab':<14} {'Gap':<10}")
    print("-" * 56)

    ref_stabs = []
    meaning_stabs = []

    for i in range(len(history) - 1):
        a, b = history[i], history[i + 1]
        result = compare_meaning(a["state"], b["state"])
        gap = result["ref_stability"] - result["meaning_stability"]
        print(f"#{a['id']:>4} → #{b['id']:<4}      "
              f"{result['ref_stability']:.1%}        "
              f"{result['meaning_stability']:.1%}          "
              f"{gap:+.1%}")
        ref_stabs.append(result["ref_stability"])
        meaning_stabs.append(result["meaning_stability"])

    if ref_stabs:
        avg_ref = sum(ref_stabs) / len(ref_stabs)
        avg_meaning = sum(meaning_stabs) / len(meaning_stabs)
        avg_gap = avg_ref - avg_meaning
        print(f"\n{'Average':<20} {avg_ref:.1%}        {avg_meaning:.1%}          {avg_gap:+.1%}")
        if avg_gap > 0.15:
            print("\n⚠️ Systematic reference-meaning gap detected.")
            print("CCS preserves entity references but meaning neighborhoods are shifting.")


def cmd_report():
    """Full report with all available history."""
    history = get_ccs_history(50)
    print(f"Meaning Stability Report — {len(history)} CCS snapshots\n")

    if len(history) < 2:
        print("Insufficient data.")
        return

    cmd_history(len(history) - 1)

    # Category trends
    print("\n\nNeighborhood Category Trends (last 5 states):")
    recent = history[-5:] if len(history) >= 5 else history
    for state in recent:
        _, text = extract_entity_contexts(state["state"])
        hoods = score_neighborhoods(text)
        print(f"  #{state['id']}: {hoods}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: meaning_stability.py check|history [N]|report")
        sys.exit(2)

    cmd = sys.argv[1]
    if cmd == "check":
        cmd_check()
    elif cmd == "history":
        n = int(sys.argv[2]) if len(sys.argv) > 2 else 5
        cmd_history(n)
    elif cmd == "report":
        cmd_report()
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(2)
