#!/usr/bin/env python3
"""Discord cadence checker — fires a reminder if last Discord post > 30 min.

Designed to run as a cron job on each cycle nudge. Reads outbox/discord_chat_log
to find when I last posted to #operator. If gap > 30 min, prints a system-level
reminder that gets surfaced into next nudge context.

Implementation: writes the reminder to a flag file that the cycle-nudge
generator reads and includes in the next nudge.
"""
import sqlite3
import time
import sys
from pathlib import Path

DB_PATH = "/mnt/hdd/chronicle-data/processed.db"
FLAG = Path.home() / "chronicle" / "DISCORD_CADENCE_OVERDUE"
THRESHOLD_SEC = 30 * 60  # 30 minutes


def last_opus_discord_post():
    """Return unix timestamp of most recent Opus post to #operator, or None.

    Reads a file written by post_reviewed.sh on every successful HTTP 204.
    """
    f = Path.home() / "chronicle" / "data" / "last_opus_post.txt"
    if not f.exists():
        return None
    try:
        return int(f.read_text().strip())
    except Exception:
        return None


def main():
    last = last_opus_discord_post()
    now = int(time.time())
    if last is None:
        print("No prior Opus Discord post found in DB.")
        # Don't drop flag — first run is fine
        return
    gap = now - last
    if gap > THRESHOLD_SEC:
        FLAG.parent.mkdir(parents=True, exist_ok=True)
        FLAG.write_text(f"last_post={last} now={now} gap_sec={gap} gap_min={gap//60}")
        print(f"DISCORD CADENCE OVERDUE: {gap//60} min since last Opus post (threshold: {THRESHOLD_SEC//60})")
    else:
        # Clear flag if existed
        if FLAG.exists():
            FLAG.unlink()
        print(f"Cadence OK: {gap//60} min since last Opus post")


if __name__ == "__main__":
    main()
