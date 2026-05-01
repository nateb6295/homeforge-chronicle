#!/usr/bin/env python3
"""Read and acknowledge unacknowledged feedback for an agent.

Usage:
  read_feedback.py AGENT_NAME [--no-ack]
"""
import sqlite3, os, sys, time, json

DB_PATH = os.path.expanduser("~/.homeforge-chronicle/processed.db")

def main():
    if len(sys.argv) < 2:
        print("Usage: read_feedback.py AGENT_NAME [--no-ack]")
        sys.exit(1)

    agent = sys.argv[1]
    no_ack = "--no-ack" in sys.argv
    now = int(time.time())

    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row

    # Read unacknowledged, unexpired feedback
    rows = db.execute(
        "SELECT id, thread_id, feedback_type, content, created_at "
        "FROM swarm_feedback "
        "WHERE target_agent IN (?, 'all') AND acknowledged_at IS NULL "
        "AND (expires_at IS NULL OR expires_at > ?) "
        "ORDER BY created_at",
        (agent, now)
    ).fetchall()

    if not rows:
        print(json.dumps({"feedback": []}))
        return

    feedback = [dict(r) for r in rows]

    if not no_ack:
        for r in rows:
            db.execute(
                "UPDATE swarm_feedback SET acknowledged_by=?, acknowledged_at=? WHERE id=?",
                (agent, now, r["id"])
            )
        db.commit()

    print(json.dumps({"feedback": feedback}, indent=2))

if __name__ == "__main__":
    main()
