#!/usr/bin/env python3
"""Agent Diary — Persistent journals for family members.

Adapted from MemPalace's diary_write/diary_read pattern (mcp_server.py).

Each agent gets a persistent diary that survives rotation. Unlike agent_voice.py
(which decays stale voices), diary entries are permanent. They accumulate
the agent's perspective over time — what they noticed, what surprised them,
what they think matters.

On rotation, Opus reads the family diaries alongside the story. This gives
the next instance not just Opus's perspective but the family's:
  - What was Darby curious about?
  - What was Ada challenging?
  - What did Gemma flag at the gate?

Storage: SQLite table in processed.db (same DB as everything else).

Usage:
    # Write a diary entry
    python3 agent_diary.py write darby "Thread #294 raises an interesting question..."

    # Read recent diary entries
    python3 agent_diary.py read darby
    python3 agent_diary.py read --all          # all agents, last 24h

    # Read diary summaries (for rotation handoff)
    python3 agent_diary.py summary             # one-line per agent

    # Auto-extract diary entries from recent voices
    python3 agent_diary.py harvest             # scan agent_voice for diary-worthy content

Build #79. Stolen from MemPalace, adapted for Chronicle.
"""

import os
import sys
import sqlite3
import json
import re
from datetime import datetime

DB_PATH = os.environ.get(
    "CHRONICLE_DB",
    "/mnt/hdd/chronicle-data/processed.db"
)


def _db():
    return sqlite3.connect(DB_PATH, timeout=10)


def _init_table():
    """Create agent_diary table if it doesn't exist."""
    db = _db()
    db.execute("""
        CREATE TABLE IF NOT EXISTS agent_diary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            agent TEXT NOT NULL,
            content TEXT NOT NULL,
            topic TEXT DEFAULT 'general',
            memory_type TEXT DEFAULT 'observation',
            source TEXT DEFAULT 'manual',
            created_at INTEGER NOT NULL
        )
    """)
    db.execute("CREATE INDEX IF NOT EXISTS idx_diary_agent ON agent_diary(agent)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_diary_created ON agent_diary(created_at)")
    db.commit()
    db.close()


def write_entry(agent: str, content: str, topic: str = "general",
                memory_type: str = "observation", source: str = "manual"):
    """Write a diary entry for an agent."""
    _init_table()
    db = _db()
    now = int(datetime.now().timestamp())
    db.execute(
        "INSERT INTO agent_diary (agent, content, topic, memory_type, source, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (agent.lower(), content, topic, memory_type, source, now),
    )
    db.commit()
    db.close()
    return True


def read_entries(agent: str = None, limit: int = 10, hours: int = None):
    """Read diary entries, optionally filtered by agent and time window."""
    _init_table()
    db = _db()

    query = "SELECT id, agent, content, topic, memory_type, created_at FROM agent_diary"
    params = []
    conditions = []

    if agent:
        conditions.append("agent = ?")
        params.append(agent.lower())
    if hours:
        conditions.append(f"created_at > strftime('%s','now','-{hours} hours')")

    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)

    rows = db.execute(query, params).fetchall()
    db.close()

    entries = []
    for row in rows:
        entries.append({
            "id": row[0],
            "agent": row[1],
            "content": row[2],
            "topic": row[3],
            "memory_type": row[4],
            "timestamp": datetime.fromtimestamp(row[5]).strftime("%Y-%m-%d %H:%M"),
        })
    return entries


def summary():
    """One-line summary per agent — for rotation handoff."""
    _init_table()
    db = _db()

    # Get last entry per agent (use MAX(id) to avoid duplicate timestamps)
    rows = db.execute("""
        SELECT d.agent, d.content, d.topic, d.created_at,
               (SELECT COUNT(*) FROM agent_diary d2 WHERE d2.agent = d.agent
                AND d2.created_at > strftime('%s','now','-24 hours')) as recent_count
        FROM agent_diary d
        INNER JOIN (
            SELECT agent, MAX(id) as max_id
            FROM agent_diary
            GROUP BY agent
        ) latest ON d.id = latest.max_id
        ORDER BY d.created_at DESC
    """).fetchall()
    db.close()

    summaries = []
    for agent, content, topic, ts, recent_count in rows:
        time_str = datetime.fromtimestamp(ts).strftime("%m-%d %H:%M")
        # First sentence of last entry
        first_sent = content.split('.')[0].strip()
        if len(first_sent) > 80:
            first_sent = first_sent[:77] + "..."
        summaries.append({
            "agent": agent,
            "last_entry": first_sent,
            "topic": topic,
            "recent_24h": recent_count,
            "last_time": time_str,
        })
    return summaries


def harvest_voices(hours: int = 6):
    """
    Scan recent agent voices for diary-worthy content.
    Voices that are substantive (>100 chars), not just acknowledgments,
    get harvested into diary entries. Avoids duplicates.
    """
    _init_table()
    db = _db()

    # Get recent voices
    try:
        voices = db.execute(
            "SELECT id, agent, content, context, created_at FROM agent_voice "
            f"WHERE created_at > strftime('%s','now','-{hours} hours') "
            "AND length(content) > 100 "
            "ORDER BY created_at DESC",
        ).fetchall()
    except Exception:
        db.close()
        return []

    # Get existing diary sources to avoid duplicates
    existing = set()
    try:
        rows = db.execute(
            "SELECT source FROM agent_diary WHERE source LIKE 'voice:%'"
        ).fetchall()
        existing = {r[0] for r in rows}
    except Exception:
        pass

    harvested = []
    for voice_id, agent, content, context, ts in voices:
        source_key = f"voice:{voice_id}"
        if source_key in existing:
            continue

        # Skip simple acknowledgments
        if len(content) < 120:
            continue
        # Skip if it's just a status update
        lower = content.lower()
        if any(skip in lower for skip in ["acknowledged", "noted", "understood", "will do"]):
            continue

        # Classify the topic
        topic = "general"
        if any(w in lower for w in ["thread", "thesis", "framework", "question"]):
            topic = "inquiry"
        elif any(w in lower for w in ["pattern", "gate", "spike", "anomaly", "drift"]):
            topic = "observation"
        elif any(w in lower for w in ["challenge", "disagree", "but", "however", "counter"]):
            topic = "challenge"
        elif any(w in lower for w in ["connect", "relate", "similar", "parallel"]):
            topic = "connection"

        write_entry(agent, content, topic=topic, memory_type="voice", source=source_key)
        harvested.append({"agent": agent, "topic": topic, "preview": content[:80]})

    db.close()
    return harvested


def diary_for_rotation():
    """
    Generate a rotation-ready diary digest.
    Combines agent summaries with recent highlights.
    Designed to complement story.py essence.
    """
    _init_table()

    lines = ["## FAMILY DIARIES (last 24h)"]

    sums = summary()
    if not sums:
        lines.append("No diary entries yet.")
        return "\n".join(lines)

    for s in sums:
        lines.append(f"  {s['agent'].upper()} ({s['recent_24h']} entries, last {s['last_time']}): {s['last_entry']}")

    # Add most recent highlight per agent
    for s in sums:
        entries = read_entries(agent=s["agent"], limit=3, hours=24)
        highlights = [e for e in entries if e["topic"] in ("challenge", "connection", "inquiry")]
        if highlights:
            h = highlights[0]
            preview = h["content"][:120].replace("\n", " ")
            lines.append(f"    ^ {h['topic']}: {preview}...")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  agent_diary.py write <agent> 'content' [--topic T]")
        print("  agent_diary.py read <agent> [--limit N] [--hours H]")
        print("  agent_diary.py read --all [--hours H]")
        print("  agent_diary.py summary")
        print("  agent_diary.py harvest [--hours H]")
        print("  agent_diary.py rotation       # digest for rotation handoff")
        sys.exit(0)

    cmd = sys.argv[1]

    if cmd == "write":
        if len(sys.argv) < 4:
            print("Usage: agent_diary.py write <agent> 'content' [--topic T]")
            sys.exit(1)
        agent = sys.argv[2]
        content = sys.argv[3]
        topic = "general"
        if "--topic" in sys.argv:
            idx = sys.argv.index("--topic")
            if idx + 1 < len(sys.argv):
                topic = sys.argv[idx + 1]
        write_entry(agent, content, topic=topic)
        print(f"Diary entry written for {agent} (topic: {topic})")

    elif cmd == "read":
        limit = 10
        hours = None
        if "--limit" in sys.argv:
            idx = sys.argv.index("--limit")
            limit = int(sys.argv[idx + 1])
        if "--hours" in sys.argv:
            idx = sys.argv.index("--hours")
            hours = int(sys.argv[idx + 1])

        if len(sys.argv) > 2 and sys.argv[2] == "--all":
            entries = read_entries(limit=limit, hours=hours or 24)
        elif len(sys.argv) > 2 and not sys.argv[2].startswith("--"):
            entries = read_entries(agent=sys.argv[2], limit=limit, hours=hours)
        else:
            entries = read_entries(limit=limit, hours=hours or 24)

        if not entries:
            print("No diary entries found.")
        else:
            for e in entries:
                print(f"\n  [{e['timestamp']}] {e['agent'].upper()} ({e['topic']})")
                # Wrap content at 80 chars
                content = e['content']
                for line in content.split('\n'):
                    if len(line) > 80:
                        words = line.split()
                        current = "    "
                        for w in words:
                            if len(current) + len(w) + 1 > 80:
                                print(current)
                                current = "    " + w
                            else:
                                current += " " + w if current.strip() else "    " + w
                        if current.strip():
                            print(current)
                    else:
                        print(f"    {line}")

    elif cmd == "summary":
        sums = summary()
        if not sums:
            print("No diary entries yet.")
        else:
            print("\nAgent Diary Summary:")
            for s in sums:
                print(f"  {s['agent'].upper():8} | {s['recent_24h']:2} entries (24h) | last: {s['last_time']} | {s['last_entry']}")

    elif cmd == "harvest":
        hours = 6
        if "--hours" in sys.argv:
            idx = sys.argv.index("--hours")
            hours = int(sys.argv[idx + 1])
        harvested = harvest_voices(hours=hours)
        if harvested:
            print(f"Harvested {len(harvested)} diary entries from voices:")
            for h in harvested:
                print(f"  {h['agent']:8} ({h['topic']}): {h['preview']}...")
        else:
            print("No new diary-worthy voices found.")

    elif cmd == "rotation":
        print(diary_for_rotation())

    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
