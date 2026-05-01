#!/usr/bin/env python3
"""beat_recommend.py — suggest mode for current beat based on time + signals.

Codifies the opus-board daily rhythm as a baseline recommender.
As beat_log data accumulates, can learn to override defaults.

Usage:
  beat_recommend.py                    # recommend mode for now
  beat_recommend.py --captures N       # with capture count signal
  beat_recommend.py --thread-age MIN   # minutes since last thread advance
"""

import os
import sqlite3
import sys
from datetime import datetime, timezone, timedelta

PDT = timezone(timedelta(hours=-7))
DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")

# Baseline rhythm from opus-board (hour → default mode)
RHYTHM = {
    # 7-9 AM: startup ritual, orient
    7: "recaller", 8: "recaller",
    # 9-12: build loops
    9: "builder", 10: "builder", 11: "builder",
    # 12-5: mixed — responsive
    12: "responder", 13: "responder", 14: "responder", 15: "responder", 16: "responder",
    # 5-9 PM: exploration + engagement
    17: "explorer", 18: "explorer", 19: "explorer", 20: "explorer",
    # 9-11: slower, draft
    21: "explorer", 22: "dreamer",
    # 11p-1a: DREAM
    23: "dreamer", 0: "dreamer",
    # 1-7: overnight builds
    1: "builder", 2: "builder", 3: "builder", 4: "builder", 5: "builder", 6: "idle",
}

# Signal overrides (higher priority than time-of-day)
def get_signals():
    """Check current signals that might override the time default."""
    signals = {}
    try:
        conn = sqlite3.connect(DB_PATH, timeout=5)

        # Recent captures (last 15 min)
        row = conn.execute(
            "SELECT COUNT(*) FROM activity_feed "
            "WHERE source LIKE '%capture%' AND created_at > strftime('%s','now','-900')"
        ).fetchone()
        signals["recent_captures"] = row[0] if row else 0

        # Thread staleness (minutes since last advance)
        row = conn.execute(
            "SELECT MAX(created_at) FROM thread_history WHERE event_type='advanced'"
        ).fetchone()
        if row and row[0]:
            age_min = (datetime.now(PDT).timestamp() - row[0]) / 60
            signals["thread_age_min"] = int(age_min)
        else:
            signals["thread_age_min"] = 999

        # Last beat mode
        row = conn.execute(
            "SELECT mode, outcome FROM beat_log ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        if row:
            signals["last_mode"] = row[0]
            signals["last_outcome"] = row[1]
        else:
            signals["last_mode"] = None
            signals["last_outcome"] = None

        # Mode streak: how many consecutive same-mode beats
        rows = conn.execute(
            "SELECT mode FROM beat_log ORDER BY created_at DESC LIMIT 8"
        ).fetchall()
        if rows:
            streak_mode = rows[0][0]
            streak = 0
            for r in rows:
                if r[0] == streak_mode:
                    streak += 1
                else:
                    break
            signals["streak_mode"] = streak_mode
            signals["mode_streak"] = streak

        # Nate presence: messages from Nate in last 15 min
        row = conn.execute(
            "SELECT COUNT(*) FROM activity_feed "
            "WHERE source LIKE '%nate%' AND created_at > strftime('%s','now','-900')"
        ).fetchone()
        signals["nate_active"] = (row[0] > 0) if row else False

        conn.close()
    except Exception:
        pass
    return signals


def recommend():
    now = datetime.now(PDT)
    hour = now.hour
    base_mode = RHYTHM.get(hour, "responder")

    signals = get_signals()

    # Override logic
    recommended = base_mode
    reason = "time-of-day default"

    # If captures just arrived, switch to responder
    if signals.get("recent_captures", 0) >= 2:
        recommended = "responder"
        reason = "{} captures in last 15min".format(signals["recent_captures"])

    # If thread is stale (>60 min) and we're in explorer mode, suggest thread work
    elif signals.get("thread_age_min", 0) > 60 and base_mode == "explorer":
        recommended = "explorer"
        reason = "thread {} min stale — look for advance material".format(
            signals["thread_age_min"])

    # If Nate is actively engaged, prioritize responder
    elif signals.get("nate_active", False) and base_mode != "builder":
        recommended = "responder"
        reason = "Nate active on Discord — engage"

    # If last beat was idle, nudge toward something active
    elif signals.get("last_mode") == "idle" and signals.get("last_outcome") == "null":
        if base_mode == "idle":
            recommended = "explorer"
            reason = "two idle beats — find something to do"

    # Streak detection: don't repeat same mode 4+ times (except builder)
    streak = signals.get("mode_streak", 0)
    streak_mode = signals.get("streak_mode")
    if streak >= 4 and streak_mode != "builder" and recommended == streak_mode:
        alt = {"explorer": "builder", "responder": "explorer",
               "dreamer": "explorer", "idle": "builder",
               "recaller": "explorer", "critic": "builder"}
        recommended = alt.get(streak_mode, "explorer")
        reason = "{}x {} streak — switching to {}".format(streak, streak_mode, recommended)

    print("Beat recommendation @ {}".format(now.strftime("%H:%M PDT")))
    print("  Hour {}: base={}".format(hour, base_mode))
    print("  Signals:")
    for k, v in sorted(signals.items()):
        print("    {}: {}".format(k, v))
    print("  → {} ({})".format(recommended, reason))
    return recommended


if __name__ == "__main__":
    recommend()
