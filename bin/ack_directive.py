#!/usr/bin/env python3
import sqlite3, os, sys, time
if len(sys.argv) < 2:
    print("Usage: ack_directive.py DIRECTIVE_ID")
    sys.exit(1)
directive_id = int(sys.argv[1])
cycle_ts = time.strftime("%Y%m%d_%H%M")
db = sqlite3.connect(os.path.expanduser("~/.homeforge-chronicle/processed.db"))
db.execute("UPDATE directives SET acknowledged_by=?, acknowledged_at=? WHERE id=?",
    (f"cycle:{cycle_ts}", int(time.time()), directive_id))
db.commit()
print(f"Directive #{directive_id} acknowledged by cycle:{cycle_ts}")
