#!/usr/bin/env python3
"""Read code modification history.

Usage:
  read_modifications.py [--last N] [--status STATUS] [--agent AGENT]
"""
import sqlite3, os, sys, json, time

DB_PATH = os.path.expanduser("~/.homeforge-chronicle/processed.db")

def main():
    last = 10
    status_filter = None
    agent_filter = None

    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == "--last" and i + 1 < len(args):
            last = int(args[i + 1]); i += 2
        elif args[i] == "--status" and i + 1 < len(args):
            status_filter = args[i + 1]; i += 2
        elif args[i] == "--agent" and i + 1 < len(args):
            agent_filter = args[i + 1]; i += 2
        else:
            i += 1

    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row

    sql = "SELECT id, timestamp, target_file, modification_type, description, status, rollback_reason, initiated_by FROM code_modifications"
    conditions = []
    params = []

    if status_filter:
        conditions.append("status = ?")
        params.append(status_filter)
    if agent_filter:
        conditions.append("target_file LIKE ?")
        params.append(f"%{agent_filter}%")

    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
    sql += " ORDER BY timestamp DESC LIMIT ?"
    params.append(last)

    rows = db.execute(sql, params).fetchall()

    if not rows:
        print("No modifications found.")
        return

    print(f"{len(rows)} modifications:")
    for r in rows:
        age = time.time() - r["timestamp"]
        if age < 3600:
            ago = f"{int(age/60)}m ago"
        elif age < 86400:
            ago = f"{age/3600:.1f}h ago"
        else:
            ago = f"{int(age/86400)}d ago"

        status_icon = {"verified": "✓", "rolled_back": "✗", "failed": "✗", "applied": "~", "pending": "?"}
        icon = status_icon.get(r["status"], "?")

        basename = os.path.basename(r["target_file"])
        print(f"  #{r['id']} [{icon} {r['status']}] {r['modification_type']} {basename} ({ago})")
        print(f"    {r['description'][:120]}")
        if r["rollback_reason"]:
            print(f"    ROLLBACK: {r['rollback_reason'][:100]}")

if __name__ == "__main__":
    main()
