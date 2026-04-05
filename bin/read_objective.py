#!/usr/bin/env python3
"""Read Opus objectives.

Usage:
  read_objective.py [--all]    # default shows only active
"""
import sqlite3, os, sys, time, json

DB_PATH = os.path.expanduser("~/.homeforge-chronicle/processed.db")

def main():
    show_all = "--all" in sys.argv
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row

    if show_all:
        rows = db.execute(
            "SELECT id, objective, motivation, status, priority, thread_id, outcome, created_at, updated_at "
            "FROM opus_objectives ORDER BY priority, created_at DESC"
        ).fetchall()
    else:
        rows = db.execute(
            "SELECT id, objective, motivation, status, priority, thread_id, outcome, created_at, updated_at "
            "FROM opus_objectives WHERE status='active' ORDER BY priority"
        ).fetchall()

    if not rows:
        print("No objectives." if not show_all else "No objectives found.")
        return

    result = []
    for r in rows:
        age = time.time() - r["updated_at"]
        if age < 3600:
            ago = f"{int(age/60)}m ago"
        elif age < 86400:
            ago = f"{age/3600:.1f}h ago"
        else:
            ago = f"{int(age/86400)}d ago"

        status_icon = {"active": "►", "achieved": "✓", "abandoned": "✗", "superseded": "↗"}
        icon = status_icon.get(r["status"], "?")
        thread = f" [thread #{r['thread_id']}]" if r["thread_id"] else ""

        print(f"  #{r['id']} [{icon} {r['status']}] (p{r['priority']}) {r['objective'][:100]}{thread}")
        print(f"    Why: {r['motivation'][:120]}")
        if r["outcome"]:
            print(f"    Outcome: {r['outcome'][:120]}")
        print(f"    Updated {ago}")

if __name__ == "__main__":
    main()
