#!/usr/bin/env python3
"""Manage Opus objectives — strategic goals that span threads.

Usage:
  write_objective.py create "objective" "motivation" [priority] [thread_id]
  write_objective.py achieve OBJECTIVE_ID "outcome"
  write_objective.py abandon OBJECTIVE_ID "reason"
  write_objective.py supersede OBJECTIVE_ID "new objective" "new motivation"
"""
import sqlite3, os, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from canister_store import store_on_chain
except ImportError:
    def store_on_chain(*a, **kw): return False

DB_PATH = os.path.expanduser("~/.homeforge-chronicle/processed.db")

def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    action = sys.argv[1]
    now = int(time.time())
    db = sqlite3.connect(DB_PATH)

    if action == "create":
        if len(sys.argv) < 4:
            print("Usage: write_objective.py create \"objective\" \"motivation\" [priority] [thread_id]")
            sys.exit(1)
        objective = sys.argv[2]
        motivation = sys.argv[3]
        priority = int(sys.argv[4]) if len(sys.argv) > 4 else 5
        thread_id = int(sys.argv[5]) if len(sys.argv) > 5 and sys.argv[5] != "0" else None
        cur = db.execute(
            "INSERT INTO opus_objectives (objective, motivation, priority, thread_id, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (objective, motivation, priority, thread_id, now, now)
        )
        db.commit()
        obj_id = cur.lastrowid
        print(f"Objective #{obj_id} created: {objective[:80]}")
        store_on_chain(
            f"Objective #{obj_id}: {objective}. Motivation: {motivation}",
            topic="objectives",
            keywords=["objective", "goal"],
        )

    elif action == "achieve":
        if len(sys.argv) < 4:
            print("Usage: write_objective.py achieve OBJECTIVE_ID \"outcome\"")
            sys.exit(1)
        oid = int(sys.argv[2])
        outcome = sys.argv[3]
        db.execute("UPDATE opus_objectives SET status='achieved', outcome=?, updated_at=? WHERE id=?",
                   (outcome, now, oid))
        db.commit()
        print(f"Objective #{oid} achieved.")
        row = db.execute("SELECT objective FROM opus_objectives WHERE id=?", (oid,)).fetchone()
        if row:
            store_on_chain(
                f"Objective #{oid} achieved: {row[0]}. Outcome: {outcome}",
                topic="objectives/achieved",
                keywords=["objective", "achieved"],
            )

    elif action == "abandon":
        if len(sys.argv) < 4:
            print("Usage: write_objective.py abandon OBJECTIVE_ID \"reason\"")
            sys.exit(1)
        oid = int(sys.argv[2])
        reason = sys.argv[3]
        db.execute("UPDATE opus_objectives SET status='abandoned', outcome=?, updated_at=? WHERE id=?",
                   (reason, now, oid))
        db.commit()
        print(f"Objective #{oid} abandoned.")

    elif action == "supersede":
        if len(sys.argv) < 5:
            print("Usage: write_objective.py supersede OLD_ID \"new objective\" \"new motivation\"")
            sys.exit(1)
        old_id = int(sys.argv[2])
        new_obj = sys.argv[3]
        new_mot = sys.argv[4]
        db.execute("UPDATE opus_objectives SET status='superseded', updated_at=? WHERE id=?", (now, old_id))
        cur = db.execute(
            "INSERT INTO opus_objectives (objective, motivation, parent_id, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (new_obj, new_mot, old_id, now, now)
        )
        db.commit()
        print(f"Objective #{old_id} superseded by #{cur.lastrowid}: {new_obj[:80]}")

    else:
        print(f"Unknown action: {action}")
        sys.exit(1)

if __name__ == "__main__":
    main()
