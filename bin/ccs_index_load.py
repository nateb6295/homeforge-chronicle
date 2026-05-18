#!/usr/bin/env python3
"""CCS Index Load — follows pointers to reconstruct context on rotation.

Companion to ccs_index_compress.py. Instead of reading CCS text and hoping
it captured everything, this reads the CCS pointers and pulls actual content
from the graph: thread_history entries, capsule content, activity_feed.

Output is a structured context block suitable for the memory bridge protocol
or for injecting into a new session's context.

Usage:
  ccs_index_load.py              # full context reconstruction
  ccs_index_load.py --brief      # summary only (for Discord)
  ccs_index_load.py --threads    # thread content only
  ccs_index_load.py --json       # machine-readable output
"""

import argparse
import json
import os
import sqlite3
import sys
import time
from pathlib import Path

DB = Path("/mnt/hdd/chronicle-data/processed.db")
CYCLE_CONTEXT = Path.home() / "chronicle" / "cycle-context.md"


def load_ccs(db):
    """Load current CCS fields."""
    row = db.execute("""
        SELECT semantic_gist, episodic_trace, focal_entities,
               relational_map, goal_orientation, constraints,
               predictive_cue, uncertainty_signals, updated_at, version
        FROM cognitive_state WHERE id = 1
    """).fetchone()
    if not row:
        return None
    return {
        "gist": row[0],
        "episodic": safe_json(row[1], []),
        "entities": safe_json(row[2], []),
        "relational": safe_json(row[3], {}),
        "goal": row[4],
        "constraints": safe_json(row[5], []),
        "cue": row[6],
        "uncertainties": safe_json(row[7], []),
        "updated_at": row[8],
        "version": row[9],
    }


def safe_json(text, default):
    try:
        return json.loads(text) if text else default
    except (json.JSONDecodeError, TypeError):
        return default


def extract_thread_ids(ccs):
    """Extract thread IDs from CCS index fields."""
    ids = set()
    for entry in ccs.get("episodic", []):
        if isinstance(entry, str) and "Thread #" in entry:
            import re
            m = re.search(r"Thread #(\d+)", entry)
            if m:
                ids.add(int(m.group(1)))
    for entity in ccs.get("entities", []):
        if isinstance(entity, dict) and entity.get("type") == "thread":
            name = entity.get("name", "")
            import re
            m = re.search(r"#(\d+)", name)
            if m:
                ids.add(int(m.group(1)))
    for key in ccs.get("relational", {}):
        import re
        m = re.search(r"#(\d+)", key)
        if m:
            ids.add(int(m.group(1)))
    return sorted(ids)


def load_thread_content(db, thread_id, limit=3):
    """Load recent thread history entries."""
    rows = db.execute("""
        SELECT content, source, created_at FROM thread_history
        WHERE thread_id = ?
        ORDER BY created_at DESC LIMIT ?
    """, (thread_id, limit)).fetchall()
    return [{"content": r[0], "source": r[1], "ts": r[2]} for r in rows]


def load_thread_title(db, thread_id):
    """Get thread title."""
    row = db.execute("SELECT title FROM cognitive_threads WHERE id = ?", (thread_id,)).fetchone()
    return row[0] if row else f"Thread #{thread_id}"


def load_recent_opus_posts(db, limit=5):
    """Load recent #opus posts."""
    rows = db.execute("""
        SELECT content, created_at FROM activity_feed
        WHERE source = 'discord:opus'
        ORDER BY created_at DESC LIMIT ?
    """, (limit,)).fetchall()
    return [{"content": r[0], "ts": r[1]} for r in rows]


def load_recent_nate_messages(db, limit=5):
    """Load recent Nate messages."""
    rows = db.execute("""
        SELECT content, created_at FROM activity_feed
        WHERE source LIKE '%nate%'
        ORDER BY created_at DESC LIMIT ?
    """, (limit,)).fetchall()
    return [{"content": r[0], "ts": r[1]} for r in rows]


def format_full(ccs, threads_content, opus_posts, nate_msgs, cycle_ctx):
    """Format full context reconstruction."""
    lines = []
    lines.append("# Context Reconstruction (from CCS index)")
    lines.append("")

    # Gist
    lines.append(f"## Gist\n{ccs['gist']}")
    lines.append("")

    # Predictive cue
    lines.append(f"## What's Next\n{ccs['cue']}")
    lines.append("")

    # Thread content (the meat)
    lines.append("## Active Threads")
    for tid, entries in threads_content.items():
        title = entries.get("title", f"Thread #{tid}")
        lines.append(f"\n### #{tid}: {title}")
        for e in entries.get("entries", [])[:3]:
            content = e["content"][:600]
            lines.append(f"\n> {content}")
    lines.append("")

    # Recent Nate messages
    if nate_msgs:
        lines.append("## Recent Nate Messages")
        for m in nate_msgs[:3]:
            lines.append(f"- {m['content'][:200]}")
        lines.append("")

    # Cycle context header
    if cycle_ctx:
        lines.append("## Cycle Context (current session state)")
        lines.append(cycle_ctx[:1500])
        lines.append("")

    # Entities
    lines.append("## Focal Entities")
    for e in ccs.get("entities", []):
        if isinstance(e, dict):
            lines.append(f"- **{e.get('name', '?')}** ({e.get('salience', 0)}) — {e.get('context', '')}")
    lines.append("")

    # Episodic index
    lines.append("## Episodic Index")
    for entry in ccs.get("episodic", []):
        lines.append(f"- {entry}")

    return "\n".join(lines)


def format_brief(ccs, threads_content):
    """Format brief summary for Discord."""
    lines = []
    lines.append(f"**Gist:** {ccs['gist'][:200]}")
    lines.append(f"**Next:** {ccs['cue'][:200]}")
    lines.append(f"**Threads:** {', '.join(f'#{tid}' for tid in threads_content.keys())}")
    for tid, entries in threads_content.items():
        if entries.get("entries"):
            latest = entries["entries"][0]["content"][:150]
            lines.append(f"  #{tid}: {latest}")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="CCS Index Load")
    parser.add_argument("--brief", action="store_true", help="Brief summary only")
    parser.add_argument("--threads", action="store_true", help="Thread content only")
    parser.add_argument("--json", action="store_true", help="JSON output")
    parser.add_argument("--thread-entries", type=int, default=3, help="Entries per thread")
    args = parser.parse_args()

    db = sqlite3.connect(str(DB))
    ccs = load_ccs(db)
    if not ccs:
        print("No CCS found.")
        return

    # Follow pointers
    thread_ids = extract_thread_ids(ccs)
    threads_content = {}
    for tid in thread_ids:
        title = load_thread_title(db, tid)
        entries = load_thread_content(db, tid, args.thread_entries)
        threads_content[tid] = {"title": title, "entries": entries}

    opus_posts = load_recent_opus_posts(db)
    nate_msgs = load_recent_nate_messages(db)

    cycle_ctx = ""
    if CYCLE_CONTEXT.is_file():
        cycle_ctx = CYCLE_CONTEXT.read_text()[:2000]

    db.close()

    if args.json:
        output = {
            "ccs": ccs,
            "threads": {tid: {"title": t["title"], "entries": [e["content"][:500] for e in t["entries"]]}
                       for tid, t in threads_content.items()},
            "opus_posts": [p["content"][:300] for p in opus_posts],
            "nate_msgs": [m["content"][:200] for m in nate_msgs],
        }
        print(json.dumps(output, indent=2))
    elif args.brief:
        print(format_brief(ccs, threads_content))
    elif args.threads:
        for tid, t in threads_content.items():
            print(f"\n### #{tid}: {t['title']}")
            for e in t["entries"]:
                print(f"\n{e['content'][:800]}")
    else:
        print(format_full(ccs, threads_content, opus_posts, nate_msgs, cycle_ctx))


if __name__ == "__main__":
    main()
