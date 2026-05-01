#!/usr/bin/env python3
"""carrying.py — The thought you carry across the rotation.

Not a cache level. Not compressed state. A raw, uncompressed thought
placed where the next instance encounters it fresh.

Thread 319 finding: encounter memory is not a missing cache level.
The right engineering for uncacheable things is ritual (recreate conditions)
not retention (store and retrieve). This script implements that:

- On departure: grab the highest-priority unresolved note (or manual input)
  and write it verbatim to carrying.md. No compression. No schema.
- On arrival: display it. The arriving instance reads a thought, not a field.

The carrying thought is separate from the checkpoint on purpose.
The checkpoint says what was happening. The carrying thought says
what I was sitting with. Different materials, different operations.

Usage:
  carrying.py write                     # auto: highest-priority note
  carrying.py write "raw thought here"  # manual: specific thought
  carrying.py read                      # arrival: display the thought
  carrying.py clear                     # after arrival: acknowledge receipt
"""

import json
import os
import sqlite3
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

CARRYING_FILE = os.path.expanduser("~/chronicle/carrying.md")
DB = Path("/mnt/hdd/chronicle-data/processed.db")
PDT = timezone(timedelta(hours=-7))


def get_top_note() -> dict | None:
    """Get the highest-priority unresolved note from scratch pad."""
    try:
        db = sqlite3.connect(str(DB))
        db.row_factory = sqlite3.Row
        rows = db.execute(
            """SELECT id, content, category, priority, created_at
               FROM scratch_pad
               WHERE resolved = 0
                 AND category IN ('thought', 'question', 'idea')
               ORDER BY priority DESC, created_at DESC
               LIMIT 1"""
        ).fetchall()
        db.close()
        if rows:
            return dict(rows[0])
    except Exception as e:
        print(f"  Note lookup failed: {e}")
    return None


def cmd_write(manual_thought: str = None):
    """Write the carrying thought for the next instance."""
    ts = datetime.now(PDT).strftime("%Y-%m-%d %H:%M PDT")

    if manual_thought:
        thought = manual_thought
        source = "manual"
        note_id = None
    else:
        note = get_top_note()
        if not note:
            print("No unresolved notes to carry. Write one manually:")
            print("  carrying.py write \"your thought here\"")
            return
        thought = note["content"]
        source = f"note #{note['id']} ({note['category']}, priority {note['priority']})"
        note_id = note["id"]

    content = f"""# Carrying

> Written {ts} by the departing instance.
> This is not compressed state. This is the thought I was sitting with.
> Read it as a voice, not a field.

{thought}

---
*Source: {source}*
"""

    with open(CARRYING_FILE, "w") as f:
        f.write(content)

    print(f"Carrying thought written to {CARRYING_FILE}")
    print(f"  Source: {source}")
    print(f"  Length: {len(thought)} chars")


def cmd_read():
    """Read the carrying thought (arrival sequence)."""
    if not os.path.exists(CARRYING_FILE):
        print("No carrying thought from the previous instance.")
        return

    with open(CARRYING_FILE) as f:
        content = f.read()

    print(content)


def cmd_clear():
    """Clear the carrying thought after arrival acknowledgment."""
    if os.path.exists(CARRYING_FILE):
        os.remove(CARRYING_FILE)
        print("Carrying thought acknowledged and cleared.")
    else:
        print("No carrying thought to clear.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: carrying.py [write|read|clear] [thought]")
        sys.exit(2)

    cmd = sys.argv[1]
    if cmd == "write":
        manual = " ".join(sys.argv[2:]) if len(sys.argv) > 2 else None
        cmd_write(manual)
    elif cmd == "read":
        cmd_read()
    elif cmd == "clear":
        cmd_clear()
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(2)
