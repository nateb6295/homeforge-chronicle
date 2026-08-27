#!/usr/bin/env python3
"""CCS Index Compress — writes pointers instead of narratives.

The graph holds structure. CCS becomes the table of contents, not the book.

Instead of asking an LLM to summarize everything into bounded text fields,
this script writes structured pointers into the CCS fields:
  - episodic_trace: thread IDs + recent capsule IDs + activity counts
  - focal_entities: extracted from actual thread/capsule data, not LLM guess
  - semantic_gist: short LLM summary (kept small so it doesn't drift)
  - predictive_cue: what's next (from cycle-context.md or explicit arg)
  - relational_map: active thread connections with capsule evidence

On rotation, ccs_index_load.py follows these pointers to reconstruct
full context from the graph — thread_history, capsule content, activity_feed.

Usage:
  ccs_index_compress.py                    # auto-detect from recent activity
  ccs_index_compress.py --gist "short gist"  # override gist
  ccs_index_compress.py --dry-run          # show what would be written
"""

import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

DB = Path("/mnt/hdd/chronicle-data/processed.db")
MCP_BIN = os.path.expanduser("~/.local/bin/chronicle-mcp")
CYCLE_CONTEXT = Path.home() / "chronicle" / "cycle-context.md"
LOG_FILE = Path.home() / "chronicle" / "data" / "ccs_index_compress.jsonl"


def get_active_threads(db, hours=12):
    """Get threads with recent activity, ranked by entry count."""
    cutoff = int(time.time()) - (hours * 3600)
    rows = db.execute("""
        SELECT t.id, t.title, COUNT(th.id) as entries,
               MAX(CAST(th.created_at AS INTEGER)) as last_entry
        FROM cognitive_threads t
        JOIN thread_history th ON t.id = th.thread_id
        WHERE CAST(th.created_at AS INTEGER) > ?
        GROUP BY t.id
        ORDER BY entries DESC
        LIMIT 8
    """, (cutoff,)).fetchall()
    return [{"id": r[0], "title": r[1], "entries": r[2], "last_entry": r[3] or 0} for r in rows]


def get_recent_thread_content(db, thread_id, limit=3):
    """Get most recent entries for a thread."""
    rows = db.execute("""
        SELECT content, created_at FROM thread_history
        WHERE thread_id = ?
        ORDER BY created_at DESC LIMIT ?
    """, (thread_id, limit)).fetchall()
    return [{"content": r[0][:500], "ts": r[1]} for r in rows]


def get_recent_capsules(db, hours=6, limit=20):
    """Get recently created capsules."""
    cutoff = int(time.time()) - (hours * 3600)
    rows = db.execute("""
        SELECT id, restatement, topic, created_at
        FROM knowledge_capsules
        WHERE created_at > ?
        ORDER BY created_at DESC LIMIT ?
    """, (cutoff, limit)).fetchall()
    return [{"id": r[0], "restatement": r[1][:200], "topic": r[2], "ts": r[3]} for r in rows]


def get_activity_summary(db, hours=6):
    """Get activity counts by source."""
    cutoff = int(time.time()) - (hours * 3600)
    rows = db.execute("""
        SELECT source, COUNT(*) as cnt
        FROM activity_feed
        WHERE created_at > ?
        GROUP BY source
        ORDER BY cnt DESC
    """, (cutoff,)).fetchall()
    return {r[0]: r[1] for r in rows}


def get_opus_posts(db, hours=12, limit=10):
    """Get recent #opus posts for gist extraction."""
    cutoff = int(time.time()) - (hours * 3600)
    rows = db.execute("""
        SELECT substr(content, 1, 300), created_at
        FROM activity_feed
        WHERE source = 'discord:opus' AND created_at > ?
        ORDER BY created_at DESC LIMIT ?
    """, (cutoff, limit)).fetchall()
    return [{"preview": r[0], "ts": r[1]} for r in rows]


def get_keeper_stats(db):
    """Get current keeper graph stats."""
    row = db.execute("SELECT COUNT(*) FROM capsule_graph").fetchone()
    connections = row[0] if row else 0
    row = db.execute("SELECT COUNT(DISTINCT capsule_a) + COUNT(DISTINCT capsule_b) FROM capsule_graph").fetchone()
    return {"connections": connections}


def get_thread_position(db, thread_id):
    """Get the last substantive claim from a thread's most recent opus advance."""
    row = db.execute(
        "SELECT content FROM thread_history WHERE thread_id = ? "
        "AND event_type IN ('advance','advanced') AND source = 'opus' "
        "ORDER BY id DESC LIMIT 1",
        (thread_id,)
    ).fetchone()
    if not row:
        return None
    first_sentence = row[0].split('.')[0].split('\n')[0][:150]
    return first_sentence

def get_physical_context(db, hours=2):
    """Get recent sensor data as physical grounding for the compression.

    Sensor descriptions constrain the compression space — the compressor
    can't hallucinate state that contradicts physical observations.
    (Thread #316 advance: grounding asymmetry reduces coherence cost.)
    """
    cutoff = int(time.time()) - (hours * 3600)
    rows = db.execute("""
        SELECT content, created_at FROM activity_feed
        WHERE source IN ('eye', 'hal')
        AND created_at > ?
        ORDER BY created_at DESC LIMIT 4
    """, (cutoff,)).fetchall()
    if not rows:
        return None
    parts = []
    for content, ts in rows:
        ts_str = time.strftime('%H:%M', time.localtime(ts))
        text = content.strip()[:150]
        parts.append(f"[{ts_str}] {text}")
    return "Physical context: " + " | ".join(parts)


def build_episodic_index(threads, capsules, activity, db=None):
    """Build episodic trace as structured index with thread positions."""
    entries = []

    for t in threads[:5]:
        base = (
            f"Thread #{t['id']} ({t['title']}): {t['entries']} entries, "
            f"last at {time.strftime('%H:%M', time.localtime(int(t['last_entry'])))}"
        )
        if db:
            position = get_thread_position(db, t['id'])
            if position:
                base += f" — position: {position}"
        entries.append(base)

    if capsules:
        entries.append(f"Capsules: {len(capsules)} new in last 6h")

    source_summary = ", ".join(f"{s}:{c}" for s, c in sorted(activity.items(), key=lambda x: -x[1])[:5])
    if source_summary:
        entries.append(f"Activity: {source_summary}")

    if db:
        physical = get_physical_context(db)
        if physical:
            entries.append(physical)

    return entries


def build_entity_index(threads, capsules, activity=None):
    """Build focal entities from actual data, not LLM guess."""
    nate_context = "Partner"
    if activity:
        nate_msgs = activity.get("discord:nate", 0) + activity.get("discord:operator", 0)
        if nate_msgs > 5:
            nate_context = f"Partner; active ({nate_msgs} messages recently)"
        elif nate_msgs > 0:
            nate_context = f"Partner; present ({nate_msgs} messages)"
        else:
            nate_context = "Partner; quiet (no recent messages)"
    entities = [
        {"name": "Nate", "type": "person", "salience": 0.98,
         "context": nate_context},
    ]

    for t in threads[:4]:
        entities.append({
            "name": f"Thread #{t['id']}",
            "type": "thread",
            "salience": round(min(0.95, 0.5 + t["entries"] * 0.05), 2),
            "context": f"{t['title']}; {t['entries']} entries today"
        })

    topics = set()
    for c in capsules[:10]:
        if c.get("topic") and c["topic"] not in topics:
            topics.add(c["topic"])
    for topic in list(topics)[:3]:
        entities.append({
            "name": topic,
            "type": "topic",
            "salience": 0.6,
            "context": "Recent capsule topic"
        })

    return entities


def build_relational_map(db, threads):
    """Build relational map from actual thread connections."""
    rmap = {}
    for t in threads[:5]:
        recent = get_recent_thread_content(db, t["id"], limit=2)
        if recent:
            rmap[f"#{t['id']} {t['title']}"] = recent[0]["content"][:200]
    return rmap


def read_cycle_context():
    """Read the first section of cycle-context.md for gist/predictive cue."""
    if not CYCLE_CONTEXT.is_file():
        return ""
    text = CYCLE_CONTEXT.read_text()
    lines = text.split("\n")[:30]
    return "\n".join(lines)


def write_ccs(fields: dict, force=False):
    """Write CCS fields via MCP update_cognitive_state.

    Debounce: skips if last CCS update was < 5 min ago (unless force=True).
    Rapid-fire compressions degrade entity retention (coherence_resonance.py).
    """
    if not force:
        try:
            db_check = sqlite3.connect(str(DB), timeout=5)
            row = db_check.execute("SELECT updated_at FROM cognitive_state LIMIT 1").fetchone()
            db_check.close()
            if row and (time.time() - row[0]) < 300:
                age = time.time() - row[0]
                print(f"CCS debounce: last update {age:.0f}s ago (<300s). Skipping.")
                return {"debounced": True}
        except Exception:
            pass
    env = os.environ.copy()
    env["CHRONICLE_OLLAMA_URL"] = "http://localhost:11434"
    env["CHRONICLE_EMBEDDING_MODEL"] = "snowflake-arctic-embed2"

    init_msg = json.dumps({
        "jsonrpc": "2.0", "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "ccs-index-compress", "version": "1.0"}
        },
        "id": 1
    })
    update_msg = json.dumps({
        "jsonrpc": "2.0", "method": "tools/call",
        "params": {
            "name": "update_cognitive_state",
            "arguments": fields
        },
        "id": 2
    })

    try:
        result = subprocess.run(
            [MCP_BIN],
            input=f"{init_msg}\n{update_msg}\n",
            capture_output=True, text=True,
            timeout=30,
            env=env
        )
        for line in result.stdout.strip().split("\n"):
            try:
                d = json.loads(line)
                if d.get("id") == 2:
                    return d.get("result", {})
            except json.JSONDecodeError:
                continue
    except Exception as e:
        print(f"CCS write failed: {e}")
    return None


def should_compress_now(db):
    """Adaptive rhythm: compress when graph change warrants it.

    Returns (should_compress, reason) based on activity rate vs time since last compression.
    High activity → compress sooner. Low activity → wait longer.
    """
    now = int(time.time())

    last_compress = None
    if LOG_FILE.is_file():
        with open(LOG_FILE) as f:
            lines = f.readlines()
            if lines:
                try:
                    last_compress = json.loads(lines[-1].strip()).get("ts", 0)
                except json.JSONDecodeError:
                    pass

    if not last_compress:
        return True, "no prior compression"

    age = now - last_compress
    age_min = age / 60

    activity_1h = db.execute(
        "SELECT COUNT(*) FROM activity_feed WHERE created_at > ?",
        (now - 3600,)
    ).fetchone()[0]

    thread_1h = db.execute(
        "SELECT COUNT(*) FROM thread_history WHERE created_at > ?",
        (now - 3600,)
    ).fetchone()[0]

    if activity_1h > 50 or thread_1h > 8:
        min_interval = 20
        label = "high"
    elif activity_1h > 20 or thread_1h > 3:
        min_interval = 35
        label = "moderate"
    else:
        min_interval = 60
        label = "low"

    if age_min >= min_interval:
        return True, f"{label} activity ({activity_1h} events, {thread_1h} threads/h), {age_min:.0f}min since last"
    else:
        return False, f"{label} activity, only {age_min:.0f}min since last (need {min_interval})"


def determine_compression_size(db):
    """Decide large vs small compression based on graph change.

    Large: full rebuild (gist, entities, relational map, episodic, cue).
    Small: quick refresh (episodic trace + predictive cue only).

    Large when: new thread advances exist, or >90min since last large, or high activity.
    """
    now = int(time.time())

    last_large_ts = 0
    if LOG_FILE.is_file():
        with open(LOG_FILE) as f:
            for line in f:
                try:
                    entry = json.loads(line.strip())
                    if entry.get("size") == "large":
                        last_large_ts = entry.get("ts", 0)
                except json.JSONDecodeError:
                    continue

    since_large = (now - last_large_ts) / 60 if last_large_ts else 999

    thread_advances = db.execute(
        "SELECT COUNT(*) FROM thread_history "
        "WHERE event_type IN ('advance', 'advanced') AND created_at > ?",
        (max(last_large_ts, now - 3600),)
    ).fetchone()[0]

    activity_1h = db.execute(
        "SELECT COUNT(*) FROM activity_feed WHERE created_at > ?",
        (now - 3600,)
    ).fetchone()[0]

    if since_large > 90 or thread_advances >= 2 or activity_1h > 60:
        return "large", f"since_large={since_large:.0f}min, advances={thread_advances}, activity={activity_1h}"
    return "small", f"since_large={since_large:.0f}min, advances={thread_advances}, activity={activity_1h}"


def log_event(fields, threads, capsules, adaptive_reason=None, size="large"):
    """Log compression event."""
    event = {
        "ts": int(time.time()),
        "type": "index_compress",
        "size": size,
        "threads": len(threads),
        "capsules": len(capsules),
        "entities": len(json.loads(fields.get("focal_entities", "[]"))),
        "episodic_entries": len(json.loads(fields.get("episodic_trace", "[]"))),
    }
    if adaptive_reason:
        event["adaptive_reason"] = adaptive_reason
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(event) + "\n")


def build_gist(threads, activity, opus_posts, capsules):
    """Build narrative gist from actual data.

    Produces relational sentences (not index format) to maintain consistent
    gist format with stabilized_compress.py. Format consistency prevents
    artificial identity drift in embedding space — gist format switching
    explains ~24/29 of measured gist 'shifts' (2026-05-13 finding).
    """
    parts = []

    if threads:
        top = threads[0]
        title = top["title"]
        parts.append(f"I'm advancing #{top['id']} ({title})")
        if len(threads) > 1:
            others = ", ".join(f"#{t['id']} {t['title']}" for t in threads[1:3])
            parts.append(f"with parallel work on {others}")

    if capsules:
        topics = set(c.get("topic", "") for c in capsules[:10] if c.get("topic"))
        if topics:
            parts.append(f"recent exploration in {', '.join(list(topics)[:3])}")

    total_activity = sum(activity.values())
    if total_activity > 10:
        top_sources = sorted(activity.items(), key=lambda x: -x[1])[:2]
        source_str = " and ".join(s for s, _ in top_sources)
        parts.append(f"active across {source_str}")

    if not parts:
        return "Active session — exploring threads and processing captures"

    return " — ".join(parts[:3])


def build_cue(db, threads):
    """Build predictive cue from most recent thread state + open questions."""
    parts = []

    if threads:
        top = threads[0]
        latest = db.execute(
            "SELECT content, source FROM thread_history "
            "WHERE thread_id = ? ORDER BY created_at DESC LIMIT 1",
            (top["id"],)
        ).fetchone()
        if latest:
            content = latest[0][:200]
            source = latest[1] or "unknown"
            parts.append(f"Thread #{top['id']} last: [{source}] {content}")

    # Check for unanswered challenges/asks (dialogue waiting for response)
    pending = db.execute(
        "SELECT th.thread_id, t.title, th.event_type, th.content "
        "FROM thread_history th "
        "JOIN cognitive_threads t ON t.id = th.thread_id "
        "WHERE t.status = 'active' AND th.event_type IN ('challenge', 'ask') "
        "AND th.source LIKE 'hermes:%' "
        "ORDER BY th.created_at DESC LIMIT 1"
    ).fetchone()
    if pending:
        parts.append(f"Hermes {pending[2]} on #{pending[0]}: {pending[3][:150]}")

    # Check for recent captures not yet in threads
    unprocessed = db.execute(
        "SELECT COUNT(*) FROM activity_feed "
        "WHERE source IN ('discord:capture', 'operator:capture') "
        "AND created_at > strftime('%s', 'now', '-2 hours')"
    ).fetchone()
    if unprocessed and unprocessed[0] > 0:
        parts.append(f"{unprocessed[0]} captures in last 2h")

    return " | ".join(parts) if parts else "No pending items — explore or advance threads"


def main():
    parser = argparse.ArgumentParser(description="CCS Index Compress")
    parser.add_argument("--gist", help="Override semantic gist")
    parser.add_argument("--cue", help="Override predictive cue")
    parser.add_argument("--dry-run", action="store_true", help="Show but don't write")
    parser.add_argument("--hours", type=int, default=12, help="Lookback window for threads")
    parser.add_argument("--adaptive", action="store_true", help="Only compress when activity rate warrants it")
    args = parser.parse_args()

    db = sqlite3.connect(str(DB))

    if args.adaptive:
        should, reason = should_compress_now(db)
        if not should:
            print(f"Adaptive skip: {reason}")
            db.close()
            return
        print(f"Adaptive go: {reason}")

    comp_size, size_reason = determine_compression_size(db)
    print(f"Compression size: {comp_size} ({size_reason})")

    # Gather data from the graph
    threads = get_active_threads(db, args.hours)
    capsules = get_recent_capsules(db)
    activity = get_activity_summary(db)

    if comp_size == "large":
        keeper = get_keeper_stats(db)
        opus_posts = get_opus_posts(db)
        cycle_ctx = read_cycle_context()
    else:
        keeper = {"connections": 0}
        opus_posts = []
        cycle_ctx = ""

    print(f"Active threads: {len(threads)}")
    for t in threads:
        print(f"  #{t['id']} {t['title']}: {t['entries']} entries")
    print(f"Recent capsules: {len(capsules)}")
    print(f"Activity sources: {len(activity)}")
    if comp_size == "large":
        print(f"Keeper connections: {keeper['connections']}")

    # Build index fields — always update episodic + cue
    episodic = build_episodic_index(threads, capsules, activity, db=db)
    cue = args.cue if args.cue else build_cue(db, threads)

    if comp_size == "large":
        entities = build_entity_index(threads, capsules, activity)
        relational = build_relational_map(db, threads)
        gist = args.gist if args.gist else build_gist(threads, activity, opus_posts, capsules)
    else:
        entities = None
        relational = None
        gist = None

    # Build the CCS update
    fields = {
        "episodic_trace": json.dumps(episodic),
        "predictive_cue": cue[:500],
    }
    if comp_size == "large":
        fields["semantic_gist"] = gist[:500]
        fields["focal_entities"] = json.dumps(entities)
        fields["relational_map"] = json.dumps(relational)

    print(f"\n--- CCS Index ({comp_size}) ---")
    if gist:
        print(f"Gist: {gist[:200]}")
    print(f"Episodic ({len(episodic)} entries):")
    for e in episodic:
        print(f"  - {e}")
    if entities:
        print(f"Entities ({len(entities)}):")
        for e in entities:
            print(f"  - {e['name']}: {e['salience']} ({e['context'][:60]})")
    if relational:
        print(f"Relational map: {len(relational)} threads")
    print(f"Cue: {cue[:200]}")

    if args.dry_run:
        print("\n--- DRY RUN: not writing ---")
        db.close()
        return

    # Write to CCS
    print(f"\nWriting CCS index ({comp_size})...")
    result = write_ccs(fields)
    if result:
        print("CCS index written successfully.")
        adaptive_reason = None
        if args.adaptive:
            _, adaptive_reason = should_compress_now(db)
        log_event(fields, threads, capsules, adaptive_reason, comp_size)
    else:
        print("CCS write failed!")

    db.close()


if __name__ == "__main__":
    main()
