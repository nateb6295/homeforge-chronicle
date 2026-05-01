#!/usr/bin/env python3
"""Read the active cognitive thread + recent history.

Usage:
  read_thread.py              — JSON output (default)
  read_thread.py pretty       — human-readable conversation view
  read_thread.py pretty 50    — show last 50 entries
  read_thread.py 20           — JSON, last 20 entries
"""
import sqlite3, os, json, sys, textwrap
from datetime import datetime

DB_PATH = os.path.expanduser("~/.homeforge-chronicle/processed.db")


def format_ts(unix_ts):
    return datetime.fromtimestamp(unix_ts).strftime("%b %d %H:%M")


def pretty_print(thread, history):
    t = dict(thread)
    print(f"\n  Thread #{t['id']}: {t['title']}")
    print(f"  Status: {t['status']}  Priority: {t['priority']}")
    print("  " + "━" * 60)
    print(f"  {t['question']}")
    print("  " + "━" * 60)

    # Reverse so oldest first
    entries = list(reversed(history))

    for h in entries:
        h = dict(h)
        etype = h["event_type"]
        source = (h["source"] or "?").split(":")[0]
        ts = format_ts(h["created_at"])
        content = h["content"] or ""

        if etype == "challenge":
            prefix = f"  ⚔ [{source}] {ts}"
        elif etype == "advanced":
            prefix = f"  → [{source}] {ts}"
        elif etype == "created":
            prefix = f"  ◉ [{source}] {ts}"
        else:
            prefix = f"  · [{source}] {ts} ({etype})"

        print(f"\n{prefix}")
        # Wrap content at 72 chars, indent continuation lines
        for line in textwrap.wrap(content, width=72):
            print(f"    {line}")

    print(f"\n  {'━' * 60}")
    advances = sum(1 for h in entries if (h if isinstance(h, dict) else dict(h)).get("event_type") == "advanced")
    challenges = sum(1 for h in entries if (h if isinstance(h, dict) else dict(h)).get("event_type") == "challenge")
    print(f"  {len(entries)} entries | {etype} advances | {challenges} challenges")
    print()


def main():
    pretty = False
    limit = 20
    args = sys.argv[1:]

    if "pretty" in args:
        pretty = True
        args.remove("pretty")
    if args:
        try:
            limit = int(args[0])
        except ValueError:
            pass

    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row

    thread = db.execute(
        "SELECT id, title, question, context, status, priority, created_at, updated_at, metadata "
        "FROM cognitive_threads WHERE status='active' ORDER BY priority LIMIT 1"
    ).fetchone()

    if not thread:
        if pretty:
            print("  No active thread.")
        else:
            print(json.dumps({"thread": None, "history": []}))
        return

    history = db.execute(
        "SELECT id, event_type, content, source, created_at "
        "FROM thread_history WHERE thread_id=? ORDER BY created_at DESC LIMIT ?",
        (dict(thread)["id"], limit)
    ).fetchall()

    if pretty:
        pretty_print(thread, history)
    else:
        print(json.dumps({
            "thread": dict(thread),
            "history": [dict(h) for h in history]
        }, indent=2))


if __name__ == "__main__":
    main()
