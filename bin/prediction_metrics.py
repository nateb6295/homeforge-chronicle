#!/usr/bin/env python3
"""prediction_metrics.py — compute the three testable predictions from the reservoir frame.

1. Time-to-first-action (TTFA): seconds from session_start to first substantive event
2. Productive ratio: substantive events / total events per time window
3. Task-aware compression quality: did the arriving instance work on what the CCS predicted?

Usage:
  python3 prediction_metrics.py              # Last 24h summary
  python3 prediction_metrics.py --hours 48   # Last 48h
  python3 prediction_metrics.py --json       # Machine-readable output
"""

import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path

DB = Path("/mnt/hdd/chronicle-data/processed.db")

SUBSTANTIVE_SOURCES = {
    "discord:opus", "discord:nate", "operator:capture", "discord:capture",
}
MONITORING_SOURCES = {
    "sentinel", "hal", "gemma", "eye", "prediction_monitor",
}


def get_ttfa_metrics(db: sqlite3.Connection, since: float) -> list[dict]:
    starts = db.execute(
        "SELECT created_at FROM activity_feed "
        "WHERE source = 'opus:session_start' AND created_at > ? "
        "ORDER BY created_at", (since,)
    ).fetchall()

    results = []
    for (start_ts,) in starts:
        first = db.execute(
            "SELECT MIN(created_at) FROM activity_feed "
            "WHERE created_at > ? AND created_at < ? "
            "AND source NOT IN ('opus:session_start', 'system:health') "
            "AND source NOT LIKE 'discord:nate%%'",
            (start_ts, start_ts + 600)
        ).fetchone()
        ttfa = (first[0] - start_ts) if first and first[0] else None
        results.append({
            "session_start": start_ts,
            "ttfa_seconds": ttfa,
            "quality": (
                "excellent" if ttfa and ttfa < 60 else
                "good" if ttfa and ttfa < 180 else
                "ok" if ttfa and ttfa < 300 else
                "slow" if ttfa else "no_action"
            ),
        })
    return results


def get_productive_ratio(db: sqlite3.Connection, since: float) -> dict:
    rows = db.execute(
        "SELECT source, COUNT(*) FROM activity_feed "
        "WHERE created_at > ? GROUP BY source", (since,)
    ).fetchall()

    total = sum(n for _, n in rows)
    substantive = sum(n for s, n in rows if s in SUBSTANTIVE_SOURCES)
    monitoring = sum(n for s, n in rows if s in MONITORING_SOURCES)
    other = total - substantive - monitoring

    return {
        "total": total,
        "substantive": substantive,
        "monitoring": monitoring,
        "other": other,
        "ratio": round(substantive / total, 3) if total else 0,
        "unique_sources": len(set(s for s, _ in rows)),
    }


def get_task_alignment(db: sqlite3.Connection, since: float) -> dict:
    """Check if session work aligned with predictive_cue from CCS."""
    cue_row = db.execute(
        "SELECT predictive_cue FROM cognitive_state WHERE id = 1"
    ).fetchone()
    cue = cue_row[0] if cue_row and cue_row[0] else ""

    history = db.execute(
        "SELECT snapshot FROM cognitive_state_history "
        "WHERE created_at > ? ORDER BY created_at DESC LIMIT 1", (since,)
    ).fetchone()
    prev_cue = ""
    if history:
        try:
            snap = json.loads(history[0])
            prev_cue = snap.get("predictive_cue", "")
        except (json.JSONDecodeError, TypeError):
            pass

    return {
        "current_cue": cue,
        "previous_cue": prev_cue,
        "cue_changed": cue != prev_cue,
    }


def main():
    parser = argparse.ArgumentParser(description="Prediction metrics dashboard")
    parser.add_argument("--hours", type=int, default=24, help="Lookback hours")
    parser.add_argument("--json", action="store_true", help="JSON output")
    args = parser.parse_args()

    since = time.time() - (args.hours * 3600)
    db = sqlite3.connect(str(DB))

    ttfa = get_ttfa_metrics(db, since)
    ratio = get_productive_ratio(db, since)
    alignment = get_task_alignment(db, since)
    db.close()

    metrics = {
        "window_hours": args.hours,
        "ttfa": ttfa,
        "productive_ratio": ratio,
        "task_alignment": alignment,
        "computed_at": int(time.time()),
    }

    if args.json:
        print(json.dumps(metrics, indent=2))
        return

    print(f"=== Prediction Metrics ({args.hours}h window) ===\n")

    print("1. Time-to-First-Action (TTFA):")
    if ttfa:
        for t in ttfa:
            ts = time.strftime("%Y-%m-%d %H:%M", time.localtime(t["session_start"]))
            secs = t["ttfa_seconds"]
            print(f"   {ts}: {secs}s ({t['quality']})" if secs else f"   {ts}: no action within 10min")
        avg = [t["ttfa_seconds"] for t in ttfa if t["ttfa_seconds"]]
        if avg:
            print(f"   Average: {sum(avg)/len(avg):.0f}s")
    else:
        print("   No session_start events yet (will accumulate after rotation)")

    print(f"\n2. Productive Ratio:")
    print(f"   Total events: {ratio['total']}")
    print(f"   Substantive: {ratio['substantive']} ({ratio['ratio']:.1%})")
    print(f"   Monitoring: {ratio['monitoring']}")
    print(f"   Other: {ratio['other']}")
    print(f"   Unique sources: {ratio['unique_sources']}")

    print(f"\n3. Task Alignment:")
    print(f"   Current cue: {alignment['current_cue'][:80]}")
    print(f"   Previous cue: {alignment['previous_cue'][:80]}")
    print(f"   Cue evolved: {'yes' if alignment['cue_changed'] else 'no'}")


if __name__ == "__main__":
    main()
