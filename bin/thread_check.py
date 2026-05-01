#!/usr/bin/env python3
"""Check for unanswered thread challenges. Quick — runs in <1s.

Outputs new challenges since the last advance, or nothing if caught up.
Designed to be called frequently (every nudge, every poll cycle).
"""
import os
import sqlite3
import sys

DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")


def main():
    db = sqlite3.connect(DB_PATH, timeout=5)
    db.row_factory = sqlite3.Row

    thread = db.execute(
        "SELECT id, title FROM cognitive_threads WHERE status='active' LIMIT 1"
    ).fetchone()
    if not thread:
        db.close()
        return

    tid = thread["id"]

    # Get last advance timestamp
    last_advance = db.execute(
        "SELECT id, created_at FROM thread_history "
        "WHERE thread_id=? AND event_type='advanced' ORDER BY id DESC LIMIT 1",
        (tid,),
    ).fetchone()

    if not last_advance:
        db.close()
        return

    # Get challenges after last advance
    challenges = db.execute(
        "SELECT id, content, created_at FROM thread_history "
        "WHERE thread_id=? AND event_type='challenge' AND id > ? "
        "ORDER BY id ASC",
        (tid, last_advance["id"]),
    ).fetchall()

    db.close()

    if not challenges:
        return  # Caught up — no output

    for c in challenges:
        age_min = max(0, (int(__import__('time').time()) - c["created_at"]) // 60)
        print(f"[THREAD #{tid} CHALLENGE {c['id']}] ({age_min}m ago)")
        print(c["content"])
        print()


if __name__ == "__main__":
    main()
