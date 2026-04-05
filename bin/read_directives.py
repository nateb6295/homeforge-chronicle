#!/usr/bin/env python3
import sqlite3, os
db = sqlite3.connect(os.path.expanduser("~/.homeforge-chronicle/processed.db"))
rows = db.execute("SELECT id, priority, content FROM directives WHERE acknowledged_at IS NULL ORDER BY priority, id").fetchall()
if not rows:
    print("No pending directives.")
else:
    print(f"{len(rows)} pending directives:")
    for r in rows:
        print(f"  [priority {r[1]}] #{r[0]}: {r[2]}")
