#!/usr/bin/env python3
import sqlite3, os, sys, time
if len(sys.argv) < 2:
    print("Usage: write_directive.py \"content\" [priority]")
    sys.exit(1)
content = sys.argv[1]
priority = int(sys.argv[2]) if len(sys.argv) > 2 else 5
db = sqlite3.connect(os.path.expanduser("~/.homeforge-chronicle/processed.db"))
db.execute("INSERT INTO directives (source, content, priority, created_at) VALUES (?, ?, ?, ?)",
    ("cycle:opus", content, priority, int(time.time())))
db.commit()
print(f"Directive written (priority {priority}): {content[:80]}")
