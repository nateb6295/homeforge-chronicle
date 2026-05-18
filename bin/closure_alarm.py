#!/usr/bin/env python3
"""Closure alarm: monitors CCS external reference ratio and fires
when the system is approaching autopoietic closure.

Reads the last N cognitive state snapshots, computes ext_ratio trend,
and returns a status: GREEN / YELLOW / RED.

RED means: ext_ratio < 0.25 AND declining. Seek external input now.
YELLOW means: ext_ratio < 0.35 AND declining. Warning.
GREEN means: ext_ratio stable or above threshold.

Usage:
  python3 closure_alarm.py              # check and print status
  python3 closure_alarm.py --json       # machine-readable output
  python3 closure_alarm.py --intervene  # if RED, trigger diversity scan
"""

import argparse
import json
import sqlite3
import subprocess
import sys
from datetime import datetime

DB = "/mnt/hdd/chronicle-data/processed.db"

EXTERNAL_MARKERS = [
    "borkar", "bennett", "parisi", "teilhard", "steiner", "stanca",
    "cubitt", "maturana", "varela", "miller", "goldstein", "vasilenko",
    "homeforge", "nate", "hermes", "capture", "paper", "article", "sellars",
    "kitsumute", "niroshajmurugan", "emollick", "repligate", "tinkeredthinker",
]
INTERNAL_MARKERS = [
    "build", "entry", "thread", "ccs", "compression", "probe",
    "measurement", "dream", "sediment", "fiction ratio", "invariant",
    "salience", "exposome", "closure", "autopoietic",
]


def get_recent_snapshots(db, n=6):
    rows = db.execute(
        "SELECT created_at, snapshot FROM cognitive_state_history "
        "ORDER BY created_at DESC LIMIT ?", (n,)
    ).fetchall()
    return list(reversed(rows))


def compute_ext_ratio(snapshot_json):
    try:
        data = json.loads(snapshot_json)
    except (json.JSONDecodeError, TypeError):
        return None

    relmap = data.get("relational_map", {})
    full_text = json.dumps(relmap).lower()

    ext = sum(1 for m in EXTERNAL_MARKERS if m in full_text)
    intl = sum(1 for m in INTERNAL_MARKERS if m in full_text)
    total = ext + intl
    return ext / total if total > 0 else 0


def assess(ratios):
    if len(ratios) < 2:
        return "GREEN", "insufficient data"

    current = ratios[-1]
    prev = ratios[-2]
    declining = current < prev

    trend = []
    for i in range(1, len(ratios)):
        trend.append(ratios[i] - ratios[i - 1])
    sustained_decline = sum(1 for t in trend[-3:] if t < -0.01) >= 2

    if current < 0.25 and (declining or sustained_decline):
        return "RED", "ext_ratio %.3f and declining — seek external input" % current
    elif current < 0.35 and sustained_decline:
        return "YELLOW", "ext_ratio %.3f with sustained decline" % current
    else:
        return "GREEN", "ext_ratio %.3f" % current


def main():
    parser = argparse.ArgumentParser(description="CCS closure alarm")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--intervene", action="store_true")
    parser.add_argument("--snapshots", type=int, default=6)
    args = parser.parse_args()

    db = sqlite3.connect(DB)
    snapshots = get_recent_snapshots(db, args.snapshots)
    db.close()

    ratios = []
    timestamps = []
    for ts, snap in snapshots:
        r = compute_ext_ratio(snap)
        if r is not None:
            ratios.append(r)
            timestamps.append(ts)

    status, reason = assess(ratios)

    if args.json:
        result = {
            "status": status,
            "reason": reason,
            "current_ratio": ratios[-1] if ratios else None,
            "ratios": [round(r, 4) for r in ratios],
            "timestamps": timestamps,
            "checked_at": datetime.utcnow().isoformat(),
        }
        print(json.dumps(result, indent=2))
    else:
        print("[%s] %s" % (status, reason))
        if ratios:
            print("  Recent: %s" % " → ".join("%.3f" % r for r in ratios[-4:]))

    if args.intervene and status == "RED":
        print("\n  Intervention: triggering external diversity scan...")
        subprocess.run([
            sys.executable, "-c",
            "import subprocess; subprocess.run(['python3', 'bin/xmcp_call.py', "
            "'searchRecentTweets', '{\"query\": \"AI cognition -is:retweet\", \"max_results\": 5}'],"
            "cwd='/home/nate-agx/chronicle')"
        ])

    return 0 if status == "GREEN" else (1 if status == "YELLOW" else 2)


if __name__ == "__main__":
    sys.exit(main())
