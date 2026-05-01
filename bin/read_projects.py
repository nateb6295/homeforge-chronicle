#!/usr/bin/env python3
"""Read active projects. Projects are ongoing — there's always something to advance."""
import sqlite3
import os
import sys

DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")


def main():
    db = sqlite3.connect(DB_PATH)

    # Active projects
    active = db.execute(
        "SELECT id, name, description, priority, updated_at "
        "FROM projects WHERE status = 'active' "
        "ORDER BY priority, updated_at"
    ).fetchall()

    # Recent updates
    updates = {}
    for proj in active:
        rows = db.execute(
            "SELECT content, created_at FROM project_updates "
            "WHERE project_id = ? ORDER BY created_at DESC LIMIT 1",
            (proj[0],),
        ).fetchall()
        if rows:
            updates[proj[0]] = rows[0]

    if not active:
        print("No active projects. Create one with write_projects.py.")
        return

    print(f"{len(active)} active project(s):\n")
    for proj in active:
        pid, name, desc, priority, updated = proj
        print(f"  [{pid}] {name} (priority {priority})")
        if desc:
            # Show first 200 chars of description
            short = desc[:200] + ("..." if len(desc) > 200 else "")
            print(f"      {short}")
        if pid in updates:
            content, ts = updates[pid]
            from datetime import datetime
            age = datetime.now() - datetime.fromtimestamp(ts)
            hours = age.total_seconds() / 3600
            if hours < 1:
                age_str = f"{int(age.total_seconds()/60)}m ago"
            elif hours < 24:
                age_str = f"{hours:.1f}h ago"
            else:
                age_str = f"{hours/24:.1f}d ago"
            print(f"      Last update ({age_str}): {content[:150]}")
        else:
            print(f"      No updates yet.")
        print()

    # Also check if any are stale (no update in 48h)
    from datetime import datetime
    stale = []
    for proj in active:
        pid = proj[0]
        if pid in updates:
            ts = updates[pid][1]
            hours = (datetime.now() - datetime.fromtimestamp(ts)).total_seconds() / 3600
            if hours > 48:
                stale.append((proj[1], f"{hours/24:.0f}d"))
        else:
            stale.append((proj[1], "never updated"))

    if stale:
        print("Stale projects (no update in 48h+):")
        for name, age in stale:
            print(f"  ⚠ {name} — {age}")

    db.close()


if __name__ == "__main__":
    # Handle --all flag to include completed/paused
    if "--all" in sys.argv:
        db = sqlite3.connect(DB_PATH)
        rows = db.execute(
            "SELECT id, name, status, priority FROM projects ORDER BY status, priority"
        ).fetchall()
        for r in rows:
            print(f"  [{r[0]}] {r[1]} ({r[2]}, priority {r[3]})")
        db.close()
    else:
        main()
