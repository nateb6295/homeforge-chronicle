#!/usr/bin/env python3
"""Create and manage projects. Projects are ongoing — they don't disappear when acknowledged.

Usage:
    write_projects.py create "Name" "Description" [--priority N]
    write_projects.py update ID "What happened"
    write_projects.py pause ID "Why"
    write_projects.py resume ID
    write_projects.py complete ID "Completion note"
"""
import sqlite3
import os
import sys
import time

DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return

    cmd = sys.argv[1]
    db = sqlite3.connect(DB_PATH)
    now = int(time.time())

    if cmd == "create":
        name = sys.argv[2] if len(sys.argv) > 2 else "Unnamed"
        desc = sys.argv[3] if len(sys.argv) > 3 else ""
        priority = 5
        if "--priority" in sys.argv:
            idx = sys.argv.index("--priority")
            priority = int(sys.argv[idx + 1])
        db.execute(
            "INSERT INTO projects (name, description, status, priority, created_at, updated_at) "
            "VALUES (?, ?, 'active', ?, ?, ?)",
            (name, desc, priority, now, now),
        )
        db.commit()
        pid = db.execute("SELECT last_insert_rowid()").fetchone()[0]
        print(f"Created project [{pid}]: {name}")

    elif cmd == "update":
        pid = int(sys.argv[2])
        content = sys.argv[3] if len(sys.argv) > 3 else ""
        db.execute(
            "INSERT INTO project_updates (project_id, update_type, content, created_at) VALUES (?, 'progress', ?, ?)",
            (pid, content, now),
        )
        db.execute("UPDATE projects SET updated_at = ? WHERE id = ?", (now, pid))
        db.commit()
        name = db.execute("SELECT name FROM projects WHERE id = ?", (pid,)).fetchone()
        print(f"Updated [{pid}] {name[0]}: {content[:100]}")

    elif cmd == "pause":
        pid = int(sys.argv[2])
        reason = sys.argv[3] if len(sys.argv) > 3 else ""
        db.execute(
            "UPDATE projects SET status = 'paused', updated_at = ? WHERE id = ?",
            (now, pid),
        )
        if reason:
            db.execute(
                "INSERT INTO project_updates (project_id, update_type, content, created_at) VALUES (?, 'status', ?, ?)",
                (pid, f"Paused: {reason}", now),
            )
        db.commit()
        print(f"Paused project [{pid}]")

    elif cmd == "resume":
        pid = int(sys.argv[2])
        db.execute(
            "UPDATE projects SET status = 'active', updated_at = ? WHERE id = ?",
            (now, pid),
        )
        db.execute(
            "INSERT INTO project_updates (project_id, update_type, content, created_at) VALUES (?, 'status', ?, ?)",
            (pid, "Resumed", now),
        )
        db.commit()
        print(f"Resumed project [{pid}]")

    elif cmd == "complete":
        pid = int(sys.argv[2])
        note = sys.argv[3] if len(sys.argv) > 3 else ""
        db.execute(
            "UPDATE projects SET status = 'completed', completed_at = ?, completion_note = ?, updated_at = ? WHERE id = ?",
            (now, note, now, pid),
        )
        db.commit()
        print(f"Completed project [{pid}]")

    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)

    db.close()


if __name__ == "__main__":
    main()
