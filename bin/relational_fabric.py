#!/usr/bin/env python3
"""Relational Fabric — persistent relational layer for CCS.

The problem: CCS relational_map gets damaged on every compression (12% probe score).
Relational structure changes at tectonic speed (Δ=0.292) so damage persists for weeks.

The fix: store relational edges in a persistent table. CCS holds pointers (edge IDs +
activation strength), not content. Compression updates which edges are active, but
can never damage edge content because it's not in CCS.

Architecture:
  thread_edges (persistent, append-mostly)
    ↑
  CCS.active_edges (pointers + salience)
    ↑
  hydrate() → reconstructs full relational context at load time

Edge lifecycle:
  - Created when a thread advance references another thread, or during compression
  - Strength refreshed when CCS references the edge
  - Strength decays slowly when unreferenced (but edge persists)
  - Edges are geological — never deleted, only deprecated
"""
import json
import re
import sqlite3
import time
from typing import Optional

DB = "/mnt/hdd/chronicle-data/processed.db"


def get_db():
    db = sqlite3.connect(DB)
    db.row_factory = sqlite3.Row
    return db


def init_table(db=None):
    close = False
    if db is None:
        db = get_db()
        close = True

    db.execute("""
        CREATE TABLE IF NOT EXISTS thread_edges (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_type TEXT NOT NULL,       -- 'thread', 'concept', 'entity'
            source_id TEXT NOT NULL,          -- thread number or concept name
            target_type TEXT NOT NULL,
            target_id TEXT NOT NULL,
            rel_type TEXT NOT NULL,           -- 'supports', 'challenges', 'extends', 'parallels', 'derives_from', 'contradicts'
            arc_name TEXT,                    -- human-readable arc name (e.g. 'test-failure-to-redesign arc')
            description TEXT NOT NULL,        -- full relational description — the texture compression keeps losing
            strength REAL DEFAULT 1.0,        -- decays when unreferenced, refreshed when active
            created_at INTEGER NOT NULL,
            last_referenced INTEGER NOT NULL,
            reference_count INTEGER DEFAULT 1,
            deprecated INTEGER DEFAULT 0      -- soft delete
        )
    """)

    db.execute("""
        CREATE INDEX IF NOT EXISTS idx_thread_edges_source
        ON thread_edges(source_type, source_id)
    """)
    db.execute("""
        CREATE INDEX IF NOT EXISTS idx_thread_edges_target
        ON thread_edges(target_type, target_id)
    """)
    db.execute("""
        CREATE INDEX IF NOT EXISTS idx_thread_edges_strength
        ON thread_edges(strength DESC)
    """)

    db.commit()
    if close:
        db.close()


def create_edge(source_type, source_id, target_type, target_id, rel_type,
                description, arc_name=None, strength=1.0, db=None):
    close = False
    if db is None:
        db = get_db()
        close = True

    now = int(time.time())
    cursor = db.execute("""
        INSERT INTO thread_edges
        (source_type, source_id, target_type, target_id, rel_type,
         arc_name, description, strength, created_at, last_referenced)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (source_type, str(source_id), target_type, str(target_id),
          rel_type, arc_name, description, strength, now, now))
    db.commit()
    edge_id = cursor.lastrowid

    if close:
        db.close()
    return edge_id


def refresh_edge(edge_id, db=None):
    close = False
    if db is None:
        db = get_db()
        close = True

    now = int(time.time())
    db.execute("""
        UPDATE thread_edges
        SET last_referenced = ?, reference_count = reference_count + 1,
            strength = MIN(1.0, strength + 0.1)
        WHERE id = ?
    """, (now, edge_id))
    db.commit()

    if close:
        db.close()


def decay_edges(decay_rate=0.02, min_strength=0.1, db=None):
    close = False
    if db is None:
        db = get_db()
        close = True

    db.execute("""
        UPDATE thread_edges
        SET strength = MAX(?, strength - ?)
        WHERE deprecated = 0 AND strength > ?
    """, (min_strength, decay_rate, min_strength))
    db.commit()

    if close:
        db.close()


def get_active_edges(limit=20, min_strength=0.3, db=None):
    close = False
    if db is None:
        db = get_db()
        close = True

    rows = db.execute("""
        SELECT * FROM thread_edges
        WHERE deprecated = 0 AND strength >= ?
        ORDER BY strength DESC, last_referenced DESC
        LIMIT ?
    """, (min_strength, limit)).fetchall()

    edges = [dict(r) for r in rows]
    if close:
        db.close()
    return edges


def get_edges_for_thread(thread_id, db=None):
    close = False
    if db is None:
        db = get_db()
        close = True

    tid = str(thread_id)
    rows = db.execute("""
        SELECT * FROM thread_edges
        WHERE deprecated = 0
          AND ((source_type = 'thread' AND source_id = ?)
            OR (target_type = 'thread' AND target_id = ?))
        ORDER BY strength DESC
    """, (tid, tid)).fetchall()

    edges = [dict(r) for r in rows]
    if close:
        db.close()
    return edges


def hydrate(active_edge_ids=None, limit=20, min_strength=0.3):
    """Resolve edge IDs into full relational context.

    If active_edge_ids provided, fetch those specific edges.
    Otherwise, fetch top edges by strength.

    Returns a dict compatible with CCS relational_map format,
    plus the raw edge data for downstream use.
    """
    db = get_db()

    if active_edge_ids:
        placeholders = ",".join("?" for _ in active_edge_ids)
        rows = db.execute(f"""
            SELECT * FROM thread_edges
            WHERE id IN ({placeholders}) AND deprecated = 0
            ORDER BY strength DESC
        """, active_edge_ids).fetchall()
        for eid in active_edge_ids:
            refresh_edge(eid, db=db)
    else:
        rows = db.execute("""
            SELECT * FROM thread_edges
            WHERE deprecated = 0 AND strength >= ?
            ORDER BY strength DESC, last_referenced DESC
            LIMIT ?
        """, (min_strength, limit)).fetchall()

    edges = [dict(r) for r in rows]

    relational_map = {}
    for edge in edges:
        key = edge.get("arc_name") or f"{edge['source_id']}→{edge['target_id']}"
        relational_map[key] = edge["description"]

    db.close()

    return {
        "relational_map": relational_map,
        "edges": edges,
        "edge_ids": [e["id"] for e in edges],
    }


def extract_thread_references(text):
    """Find thread references in text like #320, #322, Thread #315."""
    return list(set(re.findall(r'#(\d{3,})', text)))


def auto_create_edges_from_advance(thread_id, content, source="opus", db=None):
    """When a thread advance references other threads, create edges automatically."""
    refs = extract_thread_references(content)
    refs = [r for r in refs if r != str(thread_id)]

    if not refs:
        return []

    close = False
    if db is None:
        db = get_db()
        close = True

    edge_ids = []
    for ref in refs:
        rel_type = "extends"
        if any(w in content.lower() for w in ["challenge", "contradict", "against", "wrong"]):
            rel_type = "challenges"
        elif any(w in content.lower() for w in ["parallel", "analog", "similar", "same"]):
            rel_type = "parallels"
        elif any(w in content.lower() for w in ["support", "confirm", "evidence", "back"]):
            rel_type = "supports"
        elif any(w in content.lower() for w in ["derive", "from", "because", "since"]):
            rel_type = "derives_from"

        snippet = content[:250].replace("\n", " ")
        eid = create_edge(
            "thread", str(thread_id),
            "thread", ref,
            rel_type, snippet,
            arc_name=f"#{thread_id}→#{ref}",
            db=db,
        )
        edge_ids.append(eid)

    if close:
        db.close()
    return edge_ids


def migrate_relational_map(db=None):
    """Migrate current CCS relational_map entries into thread_edges."""
    close = False
    if db is None:
        db = get_db()
        close = True

    row = db.execute("""
        SELECT snapshot FROM cognitive_state_history ORDER BY id DESC LIMIT 1
    """).fetchone()

    if not row:
        print("No CCS snapshot found.")
        if close:
            db.close()
        return []

    ccs = json.loads(row["snapshot"])
    rmap = ccs.get("relational_map", {})

    if not rmap:
        print("No relational_map in current CCS.")
        if close:
            db.close()
        return []

    edge_ids = []
    for arc_name, description in rmap.items():
        thread_refs = extract_thread_references(description)

        if len(thread_refs) >= 2:
            source_id = thread_refs[0]
            target_id = thread_refs[1]
        elif len(thread_refs) == 1:
            source_id = thread_refs[0]
            target_id = "general"
        else:
            source_id = "general"
            target_id = "general"

        eid = create_edge(
            "thread", source_id,
            "thread", target_id,
            "extends", description,
            arc_name=arc_name,
            strength=0.9,
            db=db,
        )
        edge_ids.append(eid)
        print(f"  Migrated: {arc_name} → edge {eid}")

    if close:
        db.close()
    return edge_ids


def mine_thread_history(db=None, limit=200):
    """Mine recent thread_history for cross-thread references and create edges."""
    close = False
    if db is None:
        db = get_db()
        close = True

    rows = db.execute("""
        SELECT thread_id, event_type, content, source, created_at
        FROM thread_history
        WHERE event_type IN ('advance', 'tangent', 'design')
        ORDER BY created_at DESC
        LIMIT ?
    """, (limit,)).fetchall()

    created = 0
    seen = set()
    for r in rows:
        tid = str(r["thread_id"])
        refs = extract_thread_references(r["content"])
        refs = [ref for ref in refs if ref != tid]

        for ref in refs:
            pair = tuple(sorted([tid, ref]))
            if pair in seen:
                continue
            seen.add(pair)

            existing = db.execute("""
                SELECT id FROM thread_edges
                WHERE source_type = 'thread' AND source_id = ?
                  AND target_type = 'thread' AND target_id = ?
                  AND deprecated = 0
                LIMIT 1
            """, (tid, ref)).fetchone()

            if existing:
                refresh_edge(existing["id"], db=db)
                continue

            snippet = r["content"][:250].replace("\n", " ")
            create_edge(
                "thread", tid,
                "thread", ref,
                "extends", snippet,
                arc_name=f"#{tid}→#{ref} ({r['event_type']})",
                db=db,
            )
            created += 1

    if close:
        db.close()
    return created


def status():
    """Print current fabric status."""
    db = get_db()
    init_table(db)

    total = db.execute("SELECT COUNT(*) FROM thread_edges").fetchone()[0]
    active = db.execute("SELECT COUNT(*) FROM thread_edges WHERE deprecated = 0 AND strength >= 0.3").fetchone()[0]
    strong = db.execute("SELECT COUNT(*) FROM thread_edges WHERE strength >= 0.8").fetchone()[0]

    print(f"Relational Fabric Status:")
    print(f"  Total edges:  {total}")
    print(f"  Active (≥0.3): {active}")
    print(f"  Strong (≥0.8): {strong}")

    if total > 0:
        print(f"\nTop edges by strength:")
        for edge in get_active_edges(limit=10, min_strength=0.0, db=db):
            src = f"{edge['source_type']}:{edge['source_id']}"
            tgt = f"{edge['target_type']}:{edge['target_id']}"
            name = edge.get("arc_name", "")
            print(f"  [{edge['strength']:.2f}] {src} --[{edge['rel_type']}]--> {tgt}")
            if name:
                print(f"         {name}")
            print(f"         {edge['description'][:120]}...")

    db.close()


if __name__ == "__main__":
    import sys
    db = get_db()
    init_table(db)
    db.close()

    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == "migrate":
            print("Migrating CCS relational_map → thread_edges...")
            ids = migrate_relational_map()
            print(f"Migrated {len(ids)} arcs.")
            print("\nMining thread_history for cross-references...")
            mined = mine_thread_history()
            print(f"Created {mined} new edges from thread history.")
        elif cmd == "status":
            status()
        elif cmd == "hydrate":
            result = hydrate()
            print(json.dumps(result["relational_map"], indent=2))
            print(f"\n{len(result['edges'])} edges hydrated.")
        elif cmd == "decay":
            decay_edges()
            print("Decay applied.")
        else:
            print(f"Unknown command: {cmd}")
            print("Usage: relational_fabric.py [migrate|status|hydrate|decay]")
    else:
        status()
