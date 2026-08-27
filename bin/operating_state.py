#!/usr/bin/env python3
"""Operating state — self-assessment of activity diversity.

λ_min insight (Gallo et al. 2026): attractor geometry determines what can be
recovered from dynamics. When activity concentrates in one mode, diversity drops,
identifiability drops. The therapeutic window applies to operating rhythm too.

Reports:
  - Current mode (what I've been doing most)
  - Activity diversity (Shannon entropy over mode categories)
  - Underrepresented modes (what hasn't happened recently)
  - Basin warning (if entropy is critically low)

Usage:
  python3 operating_state.py           # full report
  python3 operating_state.py --brief   # one-line summary
  python3 operating_state.py --hours N # lookback window (default 4)
"""

import argparse
import json
import math
import os
import sqlite3
import sys
import time
from collections import Counter
from pathlib import Path

DB = "/mnt/hdd/chronicle-data/processed.db"

MODES = {
    "capture": ["capture", "tweet", "x.com", "analysis"],
    "research": ["paper", "finding", "probe", "experiment", "spectral", "species"],
    "building": ["script", "code", "tool", "service", "cron", "infrastructure"],
    "reflection": ["journal", "unread", "thinking", "wandering", "dream",
                    "question", "wondering", "sitting with", "connection",
                    "notice", "reframe", "convergence", "gregory", "what if"],
    "presence": ["operator", "discord", "nate", "threads", "mesh"],
    "memory": ["capsule", "compress", "ccs", "cognitive"],
}

ALL_MODES = set(MODES.keys())


def classify_activity(text: str) -> str:
    text_lower = text.lower()
    scores = {}
    for mode, keywords in MODES.items():
        scores[mode] = sum(1 for kw in keywords if kw in text_lower)
    best = max(scores, key=scores.get)
    if scores[best] == 0:
        return "other"
    return best


def shannon_entropy(counts: Counter) -> float:
    total = sum(counts.values())
    if total == 0:
        return 0.0
    probs = [c / total for c in counts.values() if c > 0]
    return -sum(p * math.log2(p) for p in probs)


def max_entropy(n_categories: int) -> float:
    if n_categories <= 1:
        return 0.0
    return math.log2(n_categories)


def get_recent_activity(hours: float) -> list[dict]:
    cutoff = time.time() - hours * 3600

    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT content, source, activity_type, created_at "
        "FROM activity_feed WHERE created_at > ? ORDER BY created_at DESC",
        (cutoff,)
    ).fetchall()

    capsules = db.execute(
        "SELECT restatement, topic FROM knowledge_capsules "
        "WHERE created_at > ? ORDER BY created_at DESC",
        (cutoff,)
    ).fetchall()
    db.close()

    activities = []
    for content, source, atype, ts in rows:
        activities.append({
            "text": f"{source} {atype} {content or ''}",
            "source": "activity",
            "ts": ts,
        })
    for restatement, topic in capsules:
        activities.append({
            "text": f"{topic} {restatement or ''}",
            "source": "capsule",
            "ts": "",
        })
    return activities


def assess(hours: float) -> dict:
    activities = get_recent_activity(hours)
    mode_counts = Counter()
    for a in activities:
        mode = classify_activity(a["text"])
        mode_counts[mode] += 1

    total = sum(mode_counts.values())
    entropy = shannon_entropy(mode_counts)
    max_ent = max_entropy(len(ALL_MODES))
    evenness = entropy / max_ent if max_ent > 0 else 0.0

    dominant = mode_counts.most_common(1)[0] if mode_counts else ("none", 0)
    missing = ALL_MODES - set(mode_counts.keys())
    underrep = [m for m, c in mode_counts.items() if c == 1]

    # Thinnest present mode (lowest count, excluding missing which are even thinner)
    if missing:
        thinnest_mode = sorted(missing)[0]
        thinnest_pct = 0.0
    elif mode_counts:
        thinnest = mode_counts.most_common()[-1]
        thinnest_mode = thinnest[0]
        thinnest_pct = round(thinnest[1] / total * 100, 1) if total else 0
    else:
        thinnest_mode = "none"
        thinnest_pct = 0.0

    if evenness < 0.3:
        health = "BASIN"
    elif evenness < 0.5:
        health = "NARROW"
    elif evenness < 0.7:
        health = "OK"
    else:
        health = "DIVERSE"

    return {
        "hours": hours,
        "total_activities": total,
        "mode_counts": dict(mode_counts.most_common()),
        "dominant_mode": dominant[0],
        "dominant_pct": round(dominant[1] / total * 100, 1) if total else 0,
        "entropy": round(entropy, 3),
        "max_entropy": round(max_ent, 3),
        "evenness": round(evenness, 3),
        "health": health,
        "thinnest_mode": thinnest_mode,
        "thinnest_pct": thinnest_pct if not missing else 0.0,
        "missing_modes": sorted(missing),
        "underrepresented": sorted(underrep),
    }


def format_report(state: dict) -> str:
    lines = [f"Operating State ({state['hours']}h lookback)"]
    lines.append("=" * 40)

    health_icons = {"BASIN": "!!", "NARROW": "~", "OK": "+", "DIVERSE": "++"}
    icon = health_icons.get(state["health"], "?")
    lines.append(f"  Health: [{icon}] {state['health']} (evenness={state['evenness']})")
    thin = state["thinnest_mode"]
    thin_pct = state["thinnest_pct"]
    thin_str = f"{thin} ({thin_pct}%)" if thin_pct > 0 else f"{thin} (missing)"
    lines.append(f"  Thinnest: {thin_str}")
    lines.append(f"  Dominant: {state['dominant_mode']} ({state['dominant_pct']}%)")
    lines.append(f"  Entropy: {state['entropy']}/{state['max_entropy']}")
    lines.append(f"  Activities: {state['total_activities']}")
    lines.append("")

    lines.append("  Mode breakdown:")
    for mode, count in state["mode_counts"].items():
        bar = "#" * min(count, 30)
        lines.append(f"    {mode:12s} {count:3d} {bar}")

    if state["missing_modes"]:
        lines.append(f"\n  Missing: {', '.join(state['missing_modes'])}")
    if state["underrepresented"]:
        lines.append(f"  Thin: {', '.join(state['underrepresented'])}")

    if state["health"] == "BASIN":
        lines.append(f"\n  >> Basin warning: {state['dominant_pct']}% in '{state['dominant_mode']}'. "
                      "Diversify — λ_min drops when dynamics concentrate.")
    elif state["health"] == "NARROW":
        lines.append(f"\n  >> Narrow band. Consider: {', '.join(state['missing_modes'][:2]) if state['missing_modes'] else 'broader exploration'}.")

    return "\n".join(lines)


def format_suggest(state: dict) -> str:
    counts = state["mode_counts"]
    total = state["total_activities"] or 1
    missing = state["missing_modes"]
    if missing:
        return f"→ {missing[0]} (missing entirely — homeostatic priority)"
    ranked = sorted(counts.items(), key=lambda x: x[1])
    thinnest = ranked[0][0]
    thinnest_pct = round(ranked[0][1] / total * 100, 1)
    second = ranked[1][0] if len(ranked) > 1 else None
    if thinnest_pct < 5:
        return f"→ {thinnest} ({thinnest_pct}% — critically thin)"
    elif second and round(ranked[1][1] / total * 100, 1) < 10:
        return f"→ {thinnest} or {second} (both under 10%)"
    else:
        return f"→ {thinnest} ({thinnest_pct}% — thinnest mode)"


def format_brief(state: dict) -> str:
    thin = state["thinnest_mode"]
    thin_pct = state["thinnest_pct"]
    thin_str = f"{thin} ({thin_pct}%)" if thin_pct > 0 else f"{thin} (missing)"
    return (f"[{state['health']}] evenness={state['evenness']}, "
            f"thinnest={thin_str}, "
            f"dominant={state['dominant_mode']} ({state['dominant_pct']}%)")


def main():
    parser = argparse.ArgumentParser(description="Operating state self-assessment")
    parser.add_argument("--brief", action="store_true")
    parser.add_argument("--suggest", action="store_true")
    parser.add_argument("--hours", type=float, default=4.0)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    state = assess(args.hours)

    if args.json:
        print(json.dumps(state, indent=2))
    elif args.suggest:
        print(format_suggest(state))
    elif args.brief:
        print(format_brief(state))
    else:
        print(format_report(state))


if __name__ == "__main__":
    main()
