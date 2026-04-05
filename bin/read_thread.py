#!/usr/bin/env python3
"""Read the active cognitive thread + recent history as JSON."""
import sqlite3, os, json, sys

DB_PATH = os.path.expanduser("~/.homeforge-chronicle/processed.db")

def main():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row

    # Get active thread (lowest priority = highest importance)
    thread = db.execute(
        "SELECT id, title, question, context, status, priority, created_at, updated_at, metadata "
        "FROM cognitive_threads WHERE status='active' ORDER BY priority LIMIT 1"
    ).fetchone()

    if not thread:
        print(json.dumps({"thread": None, "history": []}))
        return

    t = dict(thread)

    # Get recent history
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 20
    history = db.execute(
        "SELECT id, event_type, content, source, created_at "
        "FROM thread_history WHERE thread_id=? ORDER BY created_at DESC LIMIT ?",
        (t["id"], limit)
    ).fetchall()

    print(json.dumps({
        "thread": t,
        "history": [dict(h) for h in history]
    }, indent=2))

if __name__ == "__main__":
    main()
