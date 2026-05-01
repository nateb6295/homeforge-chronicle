#!/usr/bin/env python3
"""Track synthesis lineage — how ideas flow through the mesh.

Usage:
  lineage.py log OUTPUT_TYPE OUTPUT_ID AGENT INPUT_TYPE:INPUT_ID[:DESC] [...]
  lineage.py trace OUTPUT_TYPE OUTPUT_ID [--depth N]
  lineage.py agent AGENT_NAME [--limit N]

Examples:
  lineage.py log thread_advance 6142 opus voice:5157:"Gemma perception" challenge:6139:"Godel" brief:131503:"Tsao generative brain"
  lineage.py trace thread_advance 6142 --depth 3
  lineage.py agent opus --limit 10
"""
import sqlite3, os, sys, time

DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")


def log_lineage(db, output_type, output_id, agent, inputs):
    """Log synthesis lineage edges."""
    now = int(time.time())
    logged = 0
    for inp in inputs:
        parts = inp.split(":", 2)
        input_type = parts[0]
        input_id = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
        desc = parts[2] if len(parts) > 2 else None
        db.execute(
            "INSERT INTO synthesis_lineage (output_type, output_id, input_type, input_id, "
            "input_description, agent, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (output_type, output_id, input_type, input_id, desc, agent, now)
        )
        logged += 1
    db.commit()
    print(f"Logged {logged} lineage edges for {output_type}#{output_id} (agent: {agent})")


def trace_lineage(db, output_type, output_id, depth=2, indent=0):
    """Trace the lineage of a synthesis output recursively."""
    prefix = "  " * indent
    rows = db.execute(
        "SELECT input_type, input_id, input_description, agent FROM synthesis_lineage "
        "WHERE output_type=? AND output_id=?",
        (output_type, output_id)
    ).fetchall()

    if not rows and indent == 0:
        print(f"No lineage recorded for {output_type}#{output_id}")
        return

    for r in rows:
        input_type, input_id, desc, agent = r
        label = desc or ""
        id_str = f"#{input_id}" if input_id else ""
        print(f"{prefix}<- [{input_type}{id_str}] {label} (via {agent})")
        if depth > 1 and input_id:
            trace_lineage(db, input_type, input_id, depth - 1, indent + 1)


def agent_lineage(db, agent, limit=10):
    """Show recent synthesis activity for an agent."""
    rows = db.execute(
        "SELECT output_type, output_id, COUNT(*) as input_count, created_at "
        "FROM synthesis_lineage WHERE agent=? GROUP BY output_type, output_id "
        "ORDER BY created_at DESC LIMIT ?",
        (agent, limit)
    ).fetchall()

    if not rows:
        print(f"No lineage recorded for agent '{agent}'")
        return

    print(f"Recent synthesis by {agent}:")
    for r in rows:
        ts = time.strftime("%Y-%m-%d %H:%M", time.localtime(r[3]))
        print(f"  {r[0]}#{r[1]} ({r[2]} inputs) — {ts}")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    action = sys.argv[1]
    db = sqlite3.connect(DB_PATH)

    if action == "log" and len(sys.argv) >= 6:
        output_type = sys.argv[2]
        output_id = int(sys.argv[3])
        agent = sys.argv[4]
        inputs = sys.argv[5:]
        log_lineage(db, output_type, output_id, agent, inputs)

    elif action == "trace" and len(sys.argv) >= 4:
        output_type = sys.argv[2]
        output_id = int(sys.argv[3])
        depth = 2
        if "--depth" in sys.argv:
            idx = sys.argv.index("--depth")
            depth = int(sys.argv[idx + 1])
        trace_lineage(db, output_type, output_id, depth)

    elif action == "agent" and len(sys.argv) >= 3:
        agent = sys.argv[2]
        limit = 10
        if "--limit" in sys.argv:
            idx = sys.argv.index("--limit")
            limit = int(sys.argv[idx + 1])
        agent_lineage(db, agent, limit)

    else:
        print(__doc__)
        sys.exit(1)

    db.close()


if __name__ == "__main__":
    main()
