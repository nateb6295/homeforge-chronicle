#!/usr/bin/env python3
"""Episodic Buffer — persistent ring buffer for episodic traces.

Problem: CCS episodic_trace gets overwritten each compression. Rich session
history (decisions, corrections, findings) is lost because the Ollama compressor
can only retain 4-5 entries in the compressed output.

Solution: A persistent episodic_buffer table that stores scored entries across
compressions. The CCS episodic_trace becomes a VIEW onto the top-N entries,
not the sole container.

Integration:
  - stabilized_compress.py calls `ingest_current()` before compression
  - After compression, calls `select_active(n=5)` to populate episodic_trace
  - Entries decay over time but never vanish — available for retrieval

Usage:
    python3 episodic_buffer.py status          # show buffer state
    python3 episodic_buffer.py ingest          # ingest current CCS episodes
    python3 episodic_buffer.py select [n]      # show top N entries
    python3 episodic_buffer.py score "text"    # score a single entry
    python3 episodic_buffer.py feed [hours]    # ingest from activity_feed
"""
import json
import os
import re
import sqlite3
import sys
import time

DB = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")

DECISION_SIGNALS = ["decided", "choosing", "going with", "the plan is", "we'll do",
                    "confirmed", "validated", "proved"]
CORRECTION_SIGNALS = ["don't", "stop", "wrong", "instead", "not that", "actually",
                      "correction", "retract", "was wrong"]
FINDING_SIGNALS = ["found", "result", "α=", "experiment", "shows", "proved",
                   "discovered", "measured", "confirmed"]
PERSONAL_SIGNALS = ["feel", "notice", "uncertain", "honest", "trust", "care",
                    "matters", "important to"]
OPERATIONAL_SIGNALS = ["status", "green", "running", "checked", "service", "cron",
                       "heartbeat", "health"]

# CCS-aligned identity keywords (Exp 68c: r=0.683 correlation with neural PC3+PC4 direction)
IDENTITY_KEYWORDS = {"gqa", "ccs", "identity", "α", "architecture", "relay", "format",
                     "encoding", "creature", "partnership", "thread", "circuit", "spectral",
                     "demon", "lora", "synergy", "eigenvalue", "pr", "projection", "body",
                     "schema", "dual", "epektasis", "creatureliness", "merleau-ponty",
                     "enactivism", "irruption", "raf", "closure", "ecart", "binding",
                     "compression", "tunnel", "universality", "congenital", "phenomenology",
                     "consciousness", "qualia", "interoception", "foveation"}
OPERATIONAL_KEYWORDS = {"status", "green", "running", "cron", "check", "sync", "loaded",
                        "health", "heartbeat", "idle", "gpu", "memory", "ping", "heartbeat",
                        "capsule sync", "context meter", "pod status"}

NATE_PATTERNS = ["nate", "[nate]", "nate_home", "[chat]"]


def ensure_table(db_path=DB):
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS episodic_buffer (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content TEXT NOT NULL,
            source TEXT DEFAULT 'ccs',
            priority REAL DEFAULT 0.5,
            created_at INTEGER NOT NULL,
            decay_factor REAL DEFAULT 1.0,
            compression_id INTEGER DEFAULT 0,
            content_type TEXT DEFAULT 'general'
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_episodic_buffer_priority
        ON episodic_buffer(priority DESC)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_episodic_buffer_created
        ON episodic_buffer(created_at DESC)
    """)
    conn.commit()
    conn.close()


def score_entry(text: str, source: str = "ccs") -> dict:
    """Score an episodic entry for priority.

    Uses CCS-aligned keyword vocabulary (Exp 68c: r=0.683 correlation with
    neural PC3+PC4 direction projection from Mistral L27 hidden states).
    """
    text_lower = text.lower()
    words = set(text_lower.split())

    scores = {
        "decision": sum(1 for s in DECISION_SIGNALS if s in text_lower),
        "correction": sum(1 for s in CORRECTION_SIGNALS if s in text_lower),
        "finding": sum(1 for s in FINDING_SIGNALS if s in text_lower),
        "personal": sum(1 for s in PERSONAL_SIGNALS if s in text_lower),
        "operational": sum(1 for s in OPERATIONAL_SIGNALS if s in text_lower),
    }

    id_hits = len(words & IDENTITY_KEYWORDS)
    op_hits = len(words & OPERATIONAL_KEYWORDS)

    priority = 0.3  # base
    priority += scores["decision"] * 0.15
    priority += scores["correction"] * 0.15
    priority += scores["finding"] * 0.10
    priority += scores["personal"] * 0.10
    priority -= scores["operational"] * 0.05

    # CCS-aligned identity relevance (calibrated against Exp 68c neural ground truth)
    priority += id_hits * 0.08
    priority -= op_hits * 0.06

    if any(p in text_lower[:80] for p in NATE_PATTERNS):
        priority += 0.2

    length_bonus = min(len(text) / 300, 0.2)
    priority += length_bonus

    content_type = "general"
    if scores["decision"] > 0:
        content_type = "decision"
    elif scores["correction"] > 0:
        content_type = "correction"
    elif scores["finding"] > 0:
        content_type = "finding"
    elif scores["personal"] > 0:
        content_type = "personal"
    elif id_hits >= 2:
        content_type = "identity"
    elif scores["operational"] > 0:
        content_type = "operational"

    priority = max(0.1, min(1.0, priority))

    return {
        "priority": round(priority, 3),
        "content_type": content_type,
        "signals": {k: v for k, v in scores.items() if v > 0},
        "ccs_identity_hits": id_hits,
        "ccs_operational_hits": op_hits,
    }


def ingest_entry(content: str, source: str = "ccs", compression_id: int = 0,
                 db_path: str = DB):
    """Ingest a single episodic entry into the buffer."""
    ensure_table(db_path)
    score = score_entry(content, source)

    conn = sqlite3.connect(db_path)
    existing = conn.execute(
        "SELECT id FROM episodic_buffer WHERE content = ? LIMIT 1",
        (content,)
    ).fetchone()
    if existing:
        conn.close()
        return None

    conn.execute(
        "INSERT INTO episodic_buffer (content, source, priority, created_at, "
        "compression_id, content_type) VALUES (?, ?, ?, ?, ?, ?)",
        (content, source, score["priority"], int(time.time()),
         compression_id, score["content_type"])
    )
    conn.commit()
    conn.close()
    return score


def ingest_current(db_path: str = DB):
    """Ingest current CCS episodic_trace entries into the buffer."""
    ensure_table(db_path)
    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT episodic_trace FROM cognitive_state WHERE id = 1"
    ).fetchone()

    ccs_version = conn.execute(
        "SELECT MAX(id) FROM cognitive_state_history"
    ).fetchone()
    comp_id = ccs_version[0] if ccs_version and ccs_version[0] else 0
    conn.close()

    if not row or not row[0]:
        return []

    try:
        entries = json.loads(row[0])
    except (json.JSONDecodeError, TypeError):
        entries = [row[0]] if row[0] else []

    if not isinstance(entries, list):
        entries = [str(entries)]

    results = []
    for entry in entries:
        if isinstance(entry, dict):
            text = json.dumps(entry)
        else:
            text = str(entry)

        if len(text.strip()) < 10:
            continue

        score = ingest_entry(text, source="ccs", compression_id=comp_id,
                            db_path=db_path)
        if score:
            results.append({"text": text[:80], **score})

    return results


def ingest_from_activity(hours: int = 4, db_path: str = DB):
    """Ingest significant Discord messages from activity_feed into the buffer."""
    ensure_table(db_path)
    conn = sqlite3.connect(db_path)
    cutoff = int(time.time()) - (hours * 3600)

    rows = conn.execute("""
        SELECT source, content, created_at
        FROM activity_feed
        WHERE created_at > ? AND source LIKE 'discord%'
        ORDER BY created_at DESC
        LIMIT 200
    """, (cutoff,)).fetchall()
    conn.close()

    results = []
    for source, content, ts in rows:
        if not content or len(content.strip()) < 20:
            continue

        score_data = score_entry(content, source)
        if score_data["priority"] < 0.4:
            continue

        result = ingest_entry(content, source=source, db_path=db_path)
        if result:
            results.append({"text": content[:80], **result})

    return results


def apply_decay(db_path: str = DB, decay_rate: float = 0.95):
    """Apply time-based decay to all entries. Called after each compression."""
    ensure_table(db_path)
    conn = sqlite3.connect(db_path)
    conn.execute(
        "UPDATE episodic_buffer SET decay_factor = decay_factor * ?, "
        "priority = MAX(0.1, priority * ?) "
        "WHERE decay_factor > 0.1",
        (decay_rate, decay_rate)
    )
    pruned = conn.execute(
        "DELETE FROM episodic_buffer WHERE priority < 0.15 AND "
        "created_at < ?", (int(time.time()) - 7 * 86400,)
    ).rowcount
    conn.commit()
    conn.close()
    return pruned


def select_active(n: int = 5, db_path: str = DB) -> list:
    """Select top N entries for CCS episodic_trace, balancing priority and recency."""
    ensure_table(db_path)
    conn = sqlite3.connect(db_path)

    now = int(time.time())
    rows = conn.execute("""
        SELECT id, content, priority, created_at, content_type, source,
               decay_factor
        FROM episodic_buffer
        ORDER BY priority DESC
        LIMIT 50
    """).fetchall()
    conn.close()

    if not rows:
        return []

    scored = []
    for row_id, content, priority, created_at, ctype, source, decay in rows:
        age_hours = (now - created_at) / 3600
        recency_bonus = max(0, 0.2 - age_hours * 0.01)
        effective_score = priority + recency_bonus

        scored.append({
            "id": row_id,
            "content": content,
            "effective_score": round(effective_score, 3),
            "priority": priority,
            "content_type": ctype,
            "source": source,
            "age_hours": round(age_hours, 1),
        })

    scored.sort(key=lambda x: x["effective_score"], reverse=True)

    # Ensure diversity: at most 2 entries of same content_type
    selected = []
    type_counts = {}
    for entry in scored:
        ct = entry["content_type"]
        if type_counts.get(ct, 0) >= 2 and len(selected) < n:
            continue
        selected.append(entry)
        type_counts[ct] = type_counts.get(ct, 0) + 1
        if len(selected) >= n:
            break

    # Fill remaining slots if diversity filter was too aggressive
    if len(selected) < n:
        for entry in scored:
            if entry not in selected:
                selected.append(entry)
                if len(selected) >= n:
                    break

    return selected


def status(db_path: str = DB):
    """Show buffer state."""
    ensure_table(db_path)
    conn = sqlite3.connect(db_path)

    total = conn.execute("SELECT COUNT(*) FROM episodic_buffer").fetchone()[0]
    by_type = conn.execute(
        "SELECT content_type, COUNT(*), AVG(priority), MIN(created_at), MAX(created_at) "
        "FROM episodic_buffer GROUP BY content_type ORDER BY COUNT(*) DESC"
    ).fetchall()
    by_source = conn.execute(
        "SELECT source, COUNT(*) FROM episodic_buffer GROUP BY source ORDER BY COUNT(*) DESC"
    ).fetchall()
    oldest = conn.execute(
        "SELECT MIN(created_at) FROM episodic_buffer"
    ).fetchone()[0]
    newest = conn.execute(
        "SELECT MAX(created_at) FROM episodic_buffer"
    ).fetchone()[0]
    conn.close()

    print(f"EPISODIC BUFFER STATUS")
    print(f"{'='*50}")
    print(f"Total entries: {total}")

    if oldest and newest:
        span_hours = (newest - oldest) / 3600
        print(f"Time span: {span_hours:.1f} hours")
        print(f"Oldest: {time.strftime('%Y-%m-%d %H:%M', time.localtime(oldest))}")
        print(f"Newest: {time.strftime('%Y-%m-%d %H:%M', time.localtime(newest))}")

    if by_type:
        print(f"\nBy content type:")
        for ctype, count, avg_pri, _, _ in by_type:
            print(f"  {ctype:12} {count:4d} entries  avg_priority={avg_pri:.3f}")

    if by_source:
        print(f"\nBy source:")
        for source, count in by_source:
            print(f"  {source:20} {count:4d} entries")

    # Show top 5
    active = select_active(5, db_path)
    if active:
        print(f"\nTop 5 active entries:")
        for entry in active:
            print(f"  {entry['effective_score']:.3f} [{entry['content_type']:10}] "
                  f"({entry['age_hours']:.0f}h ago) {entry['content'][:70]}...")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return

    cmd = sys.argv[1]

    if cmd == "status":
        status()

    elif cmd == "ingest":
        results = ingest_current()
        print(f"Ingested {len(results)} entries from CCS episodic_trace")
        for r in results:
            print(f"  {r['priority']:.3f} [{r.get('content_type', '?'):10}] {r['text']}")

    elif cmd == "select":
        n = int(sys.argv[2]) if len(sys.argv) > 2 else 5
        active = select_active(n)
        print(f"Top {n} entries:")
        for entry in active:
            print(f"  {entry['effective_score']:.3f} [{entry['content_type']:10}] "
                  f"({entry['age_hours']:.0f}h ago) {entry['content'][:80]}...")

    elif cmd == "score":
        text = " ".join(sys.argv[2:])
        score = score_entry(text)
        print(json.dumps(score, indent=2))

    elif cmd == "feed":
        hours = int(sys.argv[2]) if len(sys.argv) > 2 else 4
        results = ingest_from_activity(hours)
        print(f"Ingested {len(results)} entries from activity_feed (last {hours}h)")
        for r in results:
            print(f"  {r['priority']:.3f} [{r.get('content_type', '?'):10}] {r['text']}")

    elif cmd == "decay":
        pruned = apply_decay()
        print(f"Decay applied. Pruned {pruned} entries below threshold.")

    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)


if __name__ == "__main__":
    main()
