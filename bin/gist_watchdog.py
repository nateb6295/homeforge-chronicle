#!/usr/bin/env python3
"""Gist Watchdog — detects CCS gist freeze between compressions.

The 47-hour gist freeze (50 identical snapshots, discovered 2026-04-23) happened
because Hermes was compressing CCS directly via MCP, bypassing stabilized_compress.py.
The SOUL.md fix prevents recurrence, but this watchdog catches regressions.

Run periodically (every 30 min or so). If gist hasn't changed in N+ snapshots,
alerts to Discord #operator.

Usage:
  python3 gist_watchdog.py           # Check and alert if frozen
  python3 gist_watchdog.py --quiet   # Check, only print if frozen
  python3 gist_watchdog.py --threshold 8  # Custom freeze threshold (default 6)
"""

import argparse
import json
import os
import sqlite3
import sys
from difflib import SequenceMatcher
from pathlib import Path

DB = Path("/mnt/hdd/chronicle-data/processed.db")
ALERT_FIELDS = ["semantic_gist", "goal_orientation"]


def check_freeze(threshold: int = 6) -> dict:
    """Check if identity fields are frozen across recent CCS snapshots."""
    db = sqlite3.connect(str(DB))
    rows = db.execute(
        "SELECT id, snapshot, created_at FROM cognitive_state_history "
        "ORDER BY id DESC LIMIT ?",
        (threshold + 5,)
    ).fetchall()
    db.close()

    if len(rows) < 3:
        return {"status": "insufficient_data", "n_snapshots": len(rows)}

    snapshots = []
    for sid, snap_json, ts in reversed(rows):
        try:
            snap = json.loads(snap_json)
            snap["_id"] = sid
            snap["_ts"] = ts
            snapshots.append(snap)
        except json.JSONDecodeError:
            continue

    results = {}
    for field in ALERT_FIELDS:
        latest_val = str(snapshots[-1].get(field, ""))

        frozen_count = 0
        for snap in reversed(snapshots[:-1]):
            prev_val = str(snap.get(field, ""))
            sim = SequenceMatcher(None, latest_val, prev_val).ratio()
            if sim > 0.9:
                frozen_count += 1
            else:
                break

        results[field] = {
            "frozen_count": frozen_count + 1,
            "frozen": frozen_count >= threshold - 1,
            "latest_preview": latest_val[:80],
        }

    any_frozen = any(r["frozen"] for r in results.values())
    return {
        "status": "FROZEN" if any_frozen else "ok",
        "fields": results,
        "n_snapshots": len(snapshots),
        "threshold": threshold,
    }


def main():
    parser = argparse.ArgumentParser(description="CCS Gist Freeze Watchdog")
    parser.add_argument("--threshold", type=int, default=6,
                        help="Snapshots to consider frozen (default 6)")
    parser.add_argument("--quiet", action="store_true",
                        help="Only print if frozen")
    args = parser.parse_args()

    result = check_freeze(args.threshold)

    if result["status"] == "FROZEN":
        frozen_fields = [f for f, r in result["fields"].items() if r["frozen"]]
        counts = [f"{f} ({result['fields'][f]['frozen_count']} snapshots)" for f in frozen_fields]
        msg = f"GIST FREEZE DETECTED: {', '.join(counts)}"
        print(msg)
        for f in frozen_fields:
            print(f"  {f}: {result['fields'][f]['latest_preview']}...")
    elif result["status"] == "ok":
        if not args.quiet:
            for field, data in result["fields"].items():
                print(f"  {field}: {data['frozen_count']} consecutive (threshold {args.threshold}) — ok")
    else:
        print(f"  {result['status']} ({result['n_snapshots']} snapshots)")


if __name__ == "__main__":
    main()
