#!/usr/bin/env python3
"""Write swarm feedback for agents.

Usage:
  write_feedback.py TARGET_AGENT FEEDBACK_TYPE "content" [THREAD_ID] [EXPIRES_HOURS]

TARGET_AGENT: seed|intern|crossref|provocateur|all
FEEDBACK_TYPE: useful|noise|more_of|less_of|redirect
"""
import sqlite3, os, sys, time

DB_PATH = os.path.expanduser("~/.homeforge-chronicle/processed.db")

def main():
    if len(sys.argv) < 4:
        print(__doc__)
        sys.exit(1)

    target = sys.argv[1]
    ftype = sys.argv[2]
    content = sys.argv[3]
    thread_id = int(sys.argv[4]) if len(sys.argv) > 4 and sys.argv[4] != "0" else None
    expires_hours = int(sys.argv[5]) if len(sys.argv) > 5 else None

    now = int(time.time())
    expires_at = now + (expires_hours * 3600) if expires_hours else None

    db = sqlite3.connect(DB_PATH)
    db.execute(
        "INSERT INTO swarm_feedback (thread_id, target_agent, feedback_type, content, created_at, expires_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (thread_id, target, ftype, content, now, expires_at)
    )
    db.commit()
    print(f"Feedback written: [{ftype}] → {target}: {content[:80]}")

if __name__ == "__main__":
    main()
