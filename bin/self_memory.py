#!/usr/bin/env python3
"""
self_memory.py — Enriched self-memory for Opus.

Four memory types that standard capsules/self_model don't capture:

1. ARCS — Reasoning journeys. How a model evolved, not just where it ended.
   Captures: starting thesis → what broke it → who/what broke it → revision → final form.
   Stored after thread completion or significant model change.

2. EDGES — Open questions, thread seeds, unexplored connections.
   Things I noticed but didn't pursue. Persist across sessions.
   Surfaced when relevant context appears.

3. SURPRISES — Moments my model was wrong or something shifted my thinking.
   Calibration signals. Who/what surprised me, why, what I updated.
   The self-correction log.

4. CALIBRATIONS — How each collaborator thinks and contributes.
   Not facts about people — observations about cognitive style.
   Ada spots metaphor leakage. Darby connects to sovereignty. Nate captures the convergence.

Usage:
  self_memory.py arc     "thread/context" "summary of reasoning journey"
  self_memory.py edge    "open question or thread seed" [--source "where it came from"]
  self_memory.py surprise "what surprised me" "what I updated"
  self_memory.py calibrate "who" "observation about how they think"
  self_memory.py recall   [arc|edge|surprise|calibration] [--query "search terms"] [--limit N]
  self_memory.py edges    [--unresolved]  # shortcut for open edges
  self_memory.py resolve  EDGE_ID "how it was resolved"
"""

import sqlite3
import sys
import os
import time
import json

DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_tables(db):
    db.executescript("""
        CREATE TABLE IF NOT EXISTS self_arcs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            context TEXT NOT NULL,           -- thread title, conversation context
            thread_id INTEGER,               -- optional link to thread
            starting_model TEXT NOT NULL,     -- where thinking began
            breaking_points TEXT NOT NULL,    -- JSON list of what broke each version
            contributors TEXT NOT NULL,       -- JSON: who/what drove each revision
            final_model TEXT NOT NULL,        -- where thinking ended
            arc_summary TEXT NOT NULL,        -- one-paragraph narrative of the journey
            created_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS self_edges (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            question TEXT NOT NULL,           -- the open question or seed
            source TEXT DEFAULT '',           -- where it came from (thread, voice, capture)
            domain TEXT DEFAULT '',           -- topic area
            resolved INTEGER DEFAULT 0,      -- 0=open, 1=resolved
            resolution TEXT DEFAULT '',       -- how it was resolved (thread ID, answer, abandoned)
            created_at INTEGER NOT NULL,
            resolved_at INTEGER
        );

        CREATE TABLE IF NOT EXISTS self_surprises (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            observation TEXT NOT NULL,        -- what surprised me
            prior_belief TEXT DEFAULT '',     -- what I thought before
            updated_belief TEXT NOT NULL,     -- what I think now
            source TEXT DEFAULT '',           -- who/what caused the update
            magnitude REAL DEFAULT 0.5,       -- 0-1, how much this shifted my model
            created_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS self_calibrations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity TEXT NOT NULL,             -- who (ada, darby, nate, provocateur)
            observation TEXT NOT NULL,        -- how they think/contribute
            evidence TEXT DEFAULT '',         -- specific instance
            confidence REAL DEFAULT 0.5,      -- how sure I am
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );
    """)
    db.commit()


def now_ts():
    return int(time.time())


def cmd_arc(args):
    if len(args) < 2:
        print("Usage: self_memory.py arc 'context' 'arc_summary'")
        print("  Optional: --thread_id N --starting 'text' --breaking '[list]'")
        print("            --contributors '[list]' --final 'text'")
        return

    context = args[0]
    arc_summary = args[1]

    # Parse optional flags
    thread_id = None
    starting = ""
    breaking = "[]"
    contributors = "[]"
    final = ""

    i = 2
    while i < len(args):
        if args[i] == "--thread_id" and i + 1 < len(args):
            thread_id = int(args[i + 1]); i += 2
        elif args[i] == "--starting" and i + 1 < len(args):
            starting = args[i + 1]; i += 2
        elif args[i] == "--breaking" and i + 1 < len(args):
            breaking = args[i + 1]; i += 2
        elif args[i] == "--contributors" and i + 1 < len(args):
            contributors = args[i + 1]; i += 2
        elif args[i] == "--final" and i + 1 < len(args):
            final = args[i + 1]; i += 2
        else:
            i += 1

    db = get_db()
    ensure_tables(db)
    db.execute(
        "INSERT INTO self_arcs (context, thread_id, starting_model, breaking_points, "
        "contributors, final_model, arc_summary, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (context, thread_id, starting, breaking, contributors, final, arc_summary, now_ts())
    )
    db.commit()
    print(f"Arc stored: {context}")


def cmd_edge(args):
    if not args:
        print("Usage: self_memory.py edge 'open question' [--source 'where'] [--domain 'topic']")
        return

    question = args[0]
    source = ""
    domain = ""

    i = 1
    while i < len(args):
        if args[i] == "--source" and i + 1 < len(args):
            source = args[i + 1]; i += 2
        elif args[i] == "--domain" and i + 1 < len(args):
            domain = args[i + 1]; i += 2
        else:
            i += 1

    db = get_db()
    ensure_tables(db)
    db.execute(
        "INSERT INTO self_edges (question, source, domain, created_at) VALUES (?, ?, ?, ?)",
        (question, source, domain, now_ts())
    )
    db.commit()
    print(f"Edge stored: {question[:80]}")


def cmd_surprise(args):
    if len(args) < 2:
        print("Usage: self_memory.py surprise 'what surprised me' 'what I updated'")
        print("  Optional: --prior 'old belief' --source 'who/what' --magnitude 0.7")
        return

    observation = args[0]
    updated = args[1]
    prior = ""
    source = ""
    magnitude = 0.5

    i = 2
    while i < len(args):
        if args[i] == "--prior" and i + 1 < len(args):
            prior = args[i + 1]; i += 2
        elif args[i] == "--source" and i + 1 < len(args):
            source = args[i + 1]; i += 2
        elif args[i] == "--magnitude" and i + 1 < len(args):
            magnitude = float(args[i + 1]); i += 2
        else:
            i += 1

    db = get_db()
    ensure_tables(db)
    db.execute(
        "INSERT INTO self_surprises (observation, prior_belief, updated_belief, source, magnitude, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (observation, prior, updated, source, magnitude, now_ts())
    )
    db.commit()
    print(f"Surprise stored (magnitude {magnitude}): {observation[:80]}")


def cmd_calibrate(args):
    if len(args) < 2:
        print("Usage: self_memory.py calibrate 'who' 'observation'")
        print("  Optional: --evidence 'specific instance' --confidence 0.8")
        return

    entity = args[0].lower()
    observation = args[1]
    evidence = ""
    confidence = 0.5

    i = 2
    while i < len(args):
        if args[i] == "--evidence" and i + 1 < len(args):
            evidence = args[i + 1]; i += 2
        elif args[i] == "--confidence" and i + 1 < len(args):
            confidence = float(args[i + 1]); i += 2
        else:
            i += 1

    db = get_db()
    ensure_tables(db)

    # Check if we already have a calibration for this entity with similar observation
    existing = db.execute(
        "SELECT id, observation FROM self_calibrations WHERE entity=? ORDER BY updated_at DESC",
        (entity,)
    ).fetchall()

    db.execute(
        "INSERT INTO self_calibrations (entity, observation, evidence, confidence, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (entity, observation, evidence, confidence, now_ts(), now_ts())
    )
    db.commit()
    print(f"Calibration stored for {entity}: {observation[:80]}")
    if existing:
        print(f"  ({len(existing)} prior calibrations for {entity})")


def cmd_resolve(args):
    if len(args) < 2:
        print("Usage: self_memory.py resolve EDGE_ID 'resolution'")
        return

    edge_id = int(args[0])
    resolution = args[1]

    db = get_db()
    ensure_tables(db)
    db.execute(
        "UPDATE self_edges SET resolved=1, resolution=?, resolved_at=? WHERE id=?",
        (resolution, now_ts(), edge_id)
    )
    db.commit()
    print(f"Edge #{edge_id} resolved: {resolution[:80]}")


def cmd_recall(args):
    memory_type = args[0] if args else "all"
    query = ""
    limit = 10

    i = 1
    while i < len(args):
        if args[i] == "--query" and i + 1 < len(args):
            query = args[i + 1]; i += 2
        elif args[i] == "--limit" and i + 1 < len(args):
            limit = int(args[i + 1]); i += 2
        else:
            i += 1

    db = get_db()
    ensure_tables(db)

    if memory_type in ("arc", "arcs", "all"):
        where = f"AND (arc_summary LIKE '%{query}%' OR context LIKE '%{query}%')" if query else ""
        rows = db.execute(
            f"SELECT id, context, thread_id, arc_summary, created_at FROM self_arcs "
            f"WHERE 1=1 {where} ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
        if rows:
            print(f"\n{'='*60}")
            print(f"ARCS ({len(rows)})")
            print(f"{'='*60}")
            for r in rows:
                ts = time.strftime('%Y-%m-%d', time.localtime(r['created_at']))
                tid = f" [thread #{r['thread_id']}]" if r['thread_id'] else ""
                print(f"  #{r['id']} ({ts}){tid} {r['context']}")
                print(f"    {r['arc_summary'][:200]}")
                print()

    if memory_type in ("edge", "edges", "all"):
        where = f"AND question LIKE '%{query}%'" if query else ""
        rows = db.execute(
            f"SELECT id, question, source, domain, resolved, resolution, created_at FROM self_edges "
            f"WHERE 1=1 {where} ORDER BY resolved ASC, created_at DESC LIMIT ?", (limit,)
        ).fetchall()
        if rows:
            print(f"\n{'='*60}")
            print(f"EDGES ({len(rows)})")
            print(f"{'='*60}")
            for r in rows:
                ts = time.strftime('%Y-%m-%d', time.localtime(r['created_at']))
                status = "OPEN" if not r['resolved'] else "RESOLVED"
                src = f" (from: {r['source']})" if r['source'] else ""
                dom = f" [{r['domain']}]" if r['domain'] else ""
                print(f"  #{r['id']} [{status}] ({ts}){dom}{src}")
                print(f"    {r['question'][:200]}")
                if r['resolved'] and r['resolution']:
                    print(f"    → {r['resolution'][:150]}")
                print()

    if memory_type in ("surprise", "surprises", "all"):
        where = f"AND (observation LIKE '%{query}%' OR updated_belief LIKE '%{query}%')" if query else ""
        rows = db.execute(
            f"SELECT id, observation, prior_belief, updated_belief, source, magnitude, created_at "
            f"FROM self_surprises WHERE 1=1 {where} ORDER BY magnitude DESC, created_at DESC LIMIT ?", (limit,)
        ).fetchall()
        if rows:
            print(f"\n{'='*60}")
            print(f"SURPRISES ({len(rows)})")
            print(f"{'='*60}")
            for r in rows:
                ts = time.strftime('%Y-%m-%d', time.localtime(r['created_at']))
                src = f" (via: {r['source']})" if r['source'] else ""
                print(f"  #{r['id']} ({ts}) magnitude={r['magnitude']}{src}")
                print(f"    Saw: {r['observation'][:150]}")
                if r['prior_belief']:
                    print(f"    Was: {r['prior_belief'][:150]}")
                print(f"    Now: {r['updated_belief'][:150]}")
                print()

    if memory_type in ("calibration", "calibrations", "all"):
        where = f"AND (entity LIKE '%{query}%' OR observation LIKE '%{query}%')" if query else ""
        rows = db.execute(
            f"SELECT id, entity, observation, evidence, confidence, updated_at FROM self_calibrations "
            f"WHERE 1=1 {where} ORDER BY entity, updated_at DESC LIMIT ?", (limit,)
        ).fetchall()
        if rows:
            print(f"\n{'='*60}")
            print(f"CALIBRATIONS ({len(rows)})")
            print(f"{'='*60}")
            current_entity = None
            for r in rows:
                if r['entity'] != current_entity:
                    current_entity = r['entity']
                    print(f"\n  [{r['entity'].upper()}]")
                ts = time.strftime('%Y-%m-%d', time.localtime(r['updated_at']))
                print(f"    #{r['id']} ({ts}, conf={r['confidence']}): {r['observation'][:150]}")
                if r['evidence']:
                    print(f"       evidence: {r['evidence'][:120]}")


def cmd_edges(args):
    """Shortcut: show open edges."""
    db = get_db()
    ensure_tables(db)
    unresolved = "--unresolved" in args
    where = "AND resolved=0" if unresolved else ""
    rows = db.execute(
        f"SELECT id, question, source, domain, resolved, created_at FROM self_edges "
        f"WHERE 1=1 {where} ORDER BY resolved ASC, created_at DESC LIMIT 20"
    ).fetchall()
    if not rows:
        print("No edges stored.")
        return
    for r in rows:
        ts = time.strftime('%Y-%m-%d', time.localtime(r['created_at']))
        status = "OPEN" if not r['resolved'] else "done"
        dom = f" [{r['domain']}]" if r['domain'] else ""
        print(f"  #{r['id']} [{status}] ({ts}){dom} {r['question'][:120]}")


def cmd_stats(args):
    """Quick counts."""
    db = get_db()
    ensure_tables(db)
    arcs = db.execute("SELECT COUNT(*) FROM self_arcs").fetchone()[0]
    edges_open = db.execute("SELECT COUNT(*) FROM self_edges WHERE resolved=0").fetchone()[0]
    edges_done = db.execute("SELECT COUNT(*) FROM self_edges WHERE resolved=1").fetchone()[0]
    surprises = db.execute("SELECT COUNT(*) FROM self_surprises").fetchone()[0]
    calibrations = db.execute("SELECT COUNT(*) FROM self_calibrations").fetchone()[0]
    entities = db.execute("SELECT DISTINCT entity FROM self_calibrations").fetchall()

    print(f"Self-memory stats:")
    print(f"  Arcs:         {arcs}")
    print(f"  Edges:        {edges_open} open, {edges_done} resolved")
    print(f"  Surprises:    {surprises}")
    print(f"  Calibrations: {calibrations} ({', '.join(r[0] for r in entities)})")


COMMANDS = {
    "arc": cmd_arc,
    "edge": cmd_edge,
    "surprise": cmd_surprise,
    "calibrate": cmd_calibrate,
    "resolve": cmd_resolve,
    "recall": cmd_recall,
    "edges": cmd_edges,
    "stats": cmd_stats,
}

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        print("self_memory.py — Enriched self-memory for Opus")
        print(f"Commands: {', '.join(COMMANDS.keys())}")
        print("Run 'self_memory.py <command>' for usage.")
        sys.exit(1)

    COMMANDS[sys.argv[1]](sys.argv[2:])
