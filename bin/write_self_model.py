#!/usr/bin/env python3
"""Update the persistent self-model.

Usage:
  write_self_model.py add "capability" TYPE "description" [confidence] [evidence_json]
  write_self_model.py update ID "new description" [new_confidence]
  write_self_model.py supersede ID "new capability" TYPE "new description"

TYPE: capability|limitation|preference|observation
"""
import sqlite3, os, sys, time, json
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

    if action == "add":
        if len(sys.argv) < 5:
            print("Usage: write_self_model.py add \"capability\" TYPE \"description\" [confidence] [evidence]")
            sys.exit(1)
        capability = sys.argv[2]
        ptype = sys.argv[3]
        description = sys.argv[4]
        confidence = float(sys.argv[5]) if len(sys.argv) > 5 else 0.5
        evidence = sys.argv[6] if len(sys.argv) > 6 else "[]"
        cur = db.execute(
            "INSERT INTO self_model (capability, property_type, description, confidence, evidence, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (capability, ptype, description, confidence, evidence, now, now)
        )
        db.commit()
        sm_id = cur.lastrowid
        print(f"Self-model #{sm_id}: [{ptype}] {capability} (confidence: {confidence})")

        # Store high-confidence self-model entries on-chain
        if confidence >= 0.85:
            store_on_chain(
                f"Self-model [{ptype}]: {capability} — {description}",
                topic=f"self-model/{ptype}",
                keywords=["self-model", ptype, capability.replace("_", "-")],
            )

    elif action == "update":
        if len(sys.argv) < 4:
            print("Usage: write_self_model.py update ID \"new description\" [confidence]")
            sys.exit(1)
        sid = int(sys.argv[2])
        desc = sys.argv[3]
        conf = float(sys.argv[4]) if len(sys.argv) > 4 else None
        if conf is not None:
            db.execute("UPDATE self_model SET description=?, confidence=?, updated_at=? WHERE id=?",
                       (desc, conf, now, sid))
        else:
            db.execute("UPDATE self_model SET description=?, updated_at=? WHERE id=?",
                       (desc, now, sid))
        db.commit()
        print(f"Self-model #{sid} updated.")

    elif action == "supersede":
        if len(sys.argv) < 6:
            print("Usage: write_self_model.py supersede OLD_ID \"capability\" TYPE \"description\"")
            sys.exit(1)
        old_id = int(sys.argv[2])
        capability = sys.argv[3]
        ptype = sys.argv[4]
        description = sys.argv[5]
        cur = db.execute(
            "INSERT INTO self_model (capability, property_type, description, confidence, evidence, created_at, updated_at) "
            "VALUES (?, ?, ?, 0.5, '[]', ?, ?)",
            (capability, ptype, description, now, now)
        )
        db.execute("UPDATE self_model SET superseded_by=?, updated_at=? WHERE id=?",
                   (cur.lastrowid, now, old_id))
        db.commit()
        print(f"Self-model #{old_id} superseded by #{cur.lastrowid}")

    else:
        print(f"Unknown action: {action}")
        sys.exit(1)

if __name__ == "__main__":
    main()
