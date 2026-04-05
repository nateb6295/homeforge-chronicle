#!/usr/bin/env python3
"""Read the persistent self-model.

Usage:
  read_self_model.py [--type TYPE] [--all] [--starvation]
"""
import sqlite3, os, sys, time

DB_PATH = os.path.expanduser("~/.homeforge-chronicle/processed.db")

def main():
    type_filter = None
    show_all = "--all" in sys.argv
    show_starvation = "--starvation" in sys.argv

    args = sys.argv[1:]
    for i, a in enumerate(args):
        if a == "--type" and i + 1 < len(args):
            type_filter = args[i + 1]

    db = sqlite3.connect(DB_PATH, timeout=30)
    db.row_factory = sqlite3.Row

    sql = "SELECT * FROM self_model WHERE superseded_by IS NULL"
    params = []
    if type_filter:
        sql += " AND property_type = ?"
        params.append(type_filter)
    sql += " ORDER BY property_type, confidence DESC"

    rows = db.execute(sql, params).fetchall()

    if not rows:
        print("Self-model is empty. Start learning.")
        return

    # Track access — stamp every active entry as accessed now
    now = int(time.time())
    accessed_ids = [r["id"] for r in rows]
    for rid in accessed_ids:
        db.execute("UPDATE self_model SET last_accessed = ? WHERE id = ?", (now, rid))
    db.commit()

    current_type = None
    for r in rows:
        if r["property_type"] != current_type:
            current_type = r["property_type"]
            print(f"\n  [{current_type.upper()}]")

        conf_bar = "█" * int(r["confidence"] * 10) + "░" * (10 - int(r["confidence"] * 10))
        print(f"    #{r['id']} {conf_bar} {r['confidence']:.1f} | {r['capability']}")
        print(f"       {r['description'][:150]}")

    # Starvation report
    if show_starvation:
        print("\n  [ACCESS STARVATION REPORT]")
        total = db.execute("SELECT COUNT(*) FROM self_model WHERE superseded_by IS NULL").fetchone()[0]
        never = db.execute("SELECT COUNT(*) FROM self_model WHERE superseded_by IS NULL AND last_accessed IS NULL").fetchone()[0]
        day_ago = now - 86400
        week_ago = now - 604800
        accessed_24h = db.execute("SELECT COUNT(*) FROM self_model WHERE superseded_by IS NULL AND last_accessed > ?", (day_ago,)).fetchone()[0]
        accessed_7d = db.execute("SELECT COUNT(*) FROM self_model WHERE superseded_by IS NULL AND last_accessed > ?", (week_ago,)).fetchone()[0]
        print(f"    Total active entries: {total}")
        print(f"    Never accessed:       {never}")
        print(f"    Accessed (24h):       {accessed_24h} ({100*accessed_24h/max(total,1):.0f}%)")
        print(f"    Accessed (7d):        {accessed_7d} ({100*accessed_7d/max(total,1):.0f}%)")
        print(f"    Starvation rate:      {100*(total-accessed_7d)/max(total,1):.0f}% (entries not touched in 7d)")

        # Most neglected entries
        neglected = db.execute(
            "SELECT id, capability, last_accessed FROM self_model "
            "WHERE superseded_by IS NULL ORDER BY COALESCE(last_accessed, 0) ASC LIMIT 5"
        ).fetchall()
        if neglected:
            print("    Most neglected:")
            for n in neglected:
                age = "never" if not n["last_accessed"] else f"{(now - n['last_accessed']) // 3600}h ago"
                print(f"      #{n['id']} ({age}) — {n['capability']}")

    # Always print summary line
    total = db.execute("SELECT COUNT(*) FROM self_model WHERE superseded_by IS NULL").fetchone()[0]
    print(f"\n  Showed {len(accessed_ids)}/{total} entries. Use --starvation for access report.")

if __name__ == "__main__":
    main()
