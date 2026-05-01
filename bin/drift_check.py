#!/usr/bin/env python3
"""Drift Check — detects holding patterns and suggests action.

Checks when the last generative action (thread advance, build, seek, story advance)
happened. If too long has passed, reports the drift and suggests what to do.

Usage:
    python3 bin/drift_check.py           # Check for drift
    python3 bin/drift_check.py --nudge   # Auto-trigger seek if drifting
"""

import json
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

DB_PATH = "/mnt/hdd/chronicle-data/processed.db"
DRIFT_THRESHOLD = 1200  # 20 minutes without generative action = drift


def get_last_generative_action():
    """Find the most recent generative action timestamp and type."""
    db = sqlite3.connect(DB_PATH)
    actions = []

    # Thread advances
    try:
        row = db.execute(
            "SELECT created_at FROM thread_events WHERE event_type='advanced' ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        if row:
            actions.append(("thread_advance", row[0]))
    except Exception:
        pass

    # Story advances (check file mtime)
    story_path = Path.home() / "chronicle" / "opus-story.md"
    if story_path.exists():
        actions.append(("story_update", int(story_path.stat().st_mtime)))

    # Thread seeks
    try:
        row = db.execute(
            "SELECT created_at FROM thread_seeks ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        if row:
            actions.append(("thread_seek", row[0]))
    except Exception:
        pass

    # Traces (proxy for active work)
    traces_dir = Path.home() / "chronicle" / "traces"
    if traces_dir.exists():
        traces = sorted(traces_dir.glob("*.md"), key=lambda p: p.stat().st_mtime, reverse=True)
        if traces:
            actions.append(("trace", int(traces[0].stat().st_mtime)))

    db.close()

    if not actions:
        return None, None, float("inf")

    actions.sort(key=lambda x: x[1], reverse=True)
    action_type, ts = actions[0]
    age = int(time.time()) - ts
    return action_type, ts, age


def main():
    auto_nudge = "--nudge" in sys.argv

    action_type, ts, age = get_last_generative_action()

    if action_type is None:
        print("DRIFT: No generative actions found.")
        status = "drift"
    elif age > DRIFT_THRESHOLD:
        minutes = age // 60
        print(f"DRIFT: Last action was {action_type} ({minutes}m ago)")
        status = "drift"
    else:
        minutes = age // 60
        print(f"ACTIVE: Last action was {action_type} ({minutes}m ago)")
        status = "active"

    if status == "drift":
        print("\nSuggested actions:")
        print("  1. python3 ~/chronicle/bin/thread_seeker.py --execute")
        print("  2. Check captures: sqlite3 ... activity_feed")
        print("  3. Read a paper from the seek list")
        print("  4. Advance the thread with a new connection")

        if auto_nudge:
            print("\n[AUTO] Running thread seeker...")
            result = subprocess.run(
                ["python3", str(Path.home() / "chronicle" / "bin" / "thread_seeker.py"), "--execute"],
                capture_output=True, text=True, timeout=120
            )
            print(result.stdout[-500:] if result.stdout else "(no output)")
            if result.returncode != 0 and result.stderr:
                print(f"Error: {result.stderr[-200:]}")

    return 0 if status == "active" else 1


if __name__ == "__main__":
    sys.exit(main())
