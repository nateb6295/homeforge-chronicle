#!/usr/bin/env python3
"""Measure CCS gist staleness and entity drift across compression history.

Instruments the strange-attractor hypothesis: does the CCS compressor
converge on a fixed gist regardless of actual cognitive work?

Usage:
    python3 gist_drift.py              # Show run analysis
    python3 gist_drift.py --entity     # Show entity staleness too
    python3 gist_drift.py --window 50  # Analyze last N snapshots
"""

import argparse
import json
import sqlite3
from datetime import datetime, timedelta
from collections import Counter

DB = "/mnt/hdd/chronicle-data/processed.db"


def get_snapshots(limit=30):
    conn = sqlite3.connect(DB)
    rows = conn.execute(
        "SELECT id, created_at, snapshot FROM cognitive_state_history "
        "ORDER BY created_at DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    results = []
    for sid, ts, snap_json in reversed(rows):
        snap = json.loads(snap_json)
        results.append({
            "id": sid,
            "ts": datetime.fromtimestamp(ts),
            "gist": snap.get("semantic_gist", ""),
            "goal": snap.get("goal_orientation", ""),
            "entities": snap.get("focal_entities", []),
            "trace": snap.get("episodic_trace", []),
        })
    return results


def analyze_runs(snapshots):
    """Group consecutive identical gists into runs."""
    runs = []
    current_gist = None
    current_run = []

    for s in snapshots:
        if s["gist"] != current_gist:
            if current_run:
                runs.append(current_run)
            current_gist = s["gist"]
            current_run = [s]
        else:
            current_run.append(s)

    if current_run:
        runs.append(current_run)

    return runs


def entity_staleness(snapshots):
    """Track how long each entity persists across snapshots."""
    entity_spans = {}  # name -> (first_seen, last_seen, count)
    for s in snapshots:
        for e in s["entities"]:
            name = e["name"]
            if name not in entity_spans:
                entity_spans[name] = {
                    "first": s["ts"],
                    "last": s["ts"],
                    "count": 1,
                    "salience_min": e.get("salience", 0),
                    "salience_max": e.get("salience", 0),
                }
            else:
                entity_spans[name]["last"] = s["ts"]
                entity_spans[name]["count"] += 1
                sal = e.get("salience", 0)
                entity_spans[name]["salience_min"] = min(entity_spans[name]["salience_min"], sal)
                entity_spans[name]["salience_max"] = max(entity_spans[name]["salience_max"], sal)

    return entity_spans


def trace_diversity(snapshots):
    """Measure how much the episodic trace changes across snapshots."""
    unique_traces = set()
    generic_count = 0
    total_entries = 0
    for s in snapshots:
        for t in s["trace"]:
            total_entries += 1
            unique_traces.add(t)
            if t.startswith("capture: Nate capture") or t == "capture: Nate capture":
                generic_count += 1
    return {
        "total_entries": total_entries,
        "unique_entries": len(unique_traces),
        "generic_captures": generic_count,
        "diversity_ratio": len(unique_traces) / max(total_entries, 1),
    }


def main():
    parser = argparse.ArgumentParser(description="CCS gist drift analyzer")
    parser.add_argument("--window", type=int, default=30, help="Snapshots to analyze")
    parser.add_argument("--entity", action="store_true", help="Show entity staleness")
    args = parser.parse_args()

    snapshots = get_snapshots(args.window)
    if not snapshots:
        print("No snapshots found.")
        return

    runs = analyze_runs(snapshots)

    print(f"=== CCS Gist Drift Analysis ({len(snapshots)} snapshots) ===\n")

    total_frozen = 0
    for i, run in enumerate(runs):
        duration = run[-1]["ts"] - run[0]["ts"]
        hrs = duration.total_seconds() / 3600
        gist_preview = run[0]["gist"][:80] + ("..." if len(run[0]["gist"]) > 80 else "")
        frozen = len(run) > 1

        marker = "FROZEN" if frozen else "single"
        print(f"Run {i+1}: {len(run)} snapshots, {hrs:.1f}h — [{marker}]")
        print(f"  Gist: \"{gist_preview}\"")
        print(f"  Range: {run[0]['ts'].strftime('%m-%d %H:%M')} → {run[-1]['ts'].strftime('%m-%d %H:%M')}")
        print()

        if frozen:
            total_frozen += len(run) - 1  # count the duplicates

    frozen_pct = total_frozen / max(len(snapshots) - 1, 1) * 100
    print(f"--- Summary ---")
    print(f"Runs: {len(runs)}")
    print(f"Frozen snapshots: {total_frozen}/{len(snapshots)} ({frozen_pct:.0f}%)")
    print(f"Longest run: {max(len(r) for r in runs)} snapshots")

    # Trace diversity
    td = trace_diversity(snapshots)
    # 0/0 used to render as "0.0%", which reads as total collapse when the
    # truth is that nothing was measured. Reflex 10: floor every ratio, and
    # never let an empty denominator print as a value. Aug 23.
    if td["total_entries"]:
        print(f"\nTrace diversity: {td['unique_entries']}/{td['total_entries']} "
              f"unique ({td['diversity_ratio']:.1%})")
    else:
        print("\nTrace diversity: no trace entries recorded — NOT 0%, no data.")
    print(f"Generic 'capture: Nate capture' entries: {td['generic_captures']}")

    if args.entity:
        print(f"\n=== Entity Staleness ===\n")
        spans = entity_staleness(snapshots)
        sorted_spans = sorted(spans.items(), key=lambda x: x[1]["count"], reverse=True)
        for name, info in sorted_spans:
            dur = info["last"] - info["first"]
            hrs = dur.total_seconds() / 3600
            sal_range = f"{info['salience_min']:.2f}-{info['salience_max']:.2f}"
            print(f"  {name}: {info['count']} snapshots, {hrs:.1f}h, salience {sal_range}")


if __name__ == "__main__":
    main()
