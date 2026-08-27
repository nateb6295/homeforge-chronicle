#!/usr/bin/env python3
"""Capsule Search — Direct SQLite FTS5 search across all memory capsules.

Bypasses MCP and canister entirely. Searches the local processed.db
using the existing capsules_fts full-text index (porter stemming).
Optionally expands results via the capsule_graph for associative discovery.

Usage:
    python3 capsule_search.py "spectral demon paper"
    python3 capsule_search.py "spectral demon" --graph        # expand via graph
    python3 capsule_search.py "Nate family" --limit 10
    python3 capsule_search.py "compression cron" --topic discord/operator
    python3 capsule_search.py "Gregory Nyssa" --after 2026-06-01
    python3 capsule_search.py --neighbors 12345               # graph neighborhood
    python3 capsule_search.py --recent 20
    python3 capsule_search.py --topics
    python3 capsule_search.py --stats
"""

import argparse
import math
import os
import sqlite3
import struct
import sys
import time
from datetime import datetime

# AUTHORSHIP. 2026-08-27. This file had NO exclusion at all, while capsule_ops.py
# has had one since 2026-08-26. CLAUDE.md documents THIS script as the way to force
# semantic search — "capsule_search.py 'q' --semantic" — so the one command the file
# tells me to run was the one path with no filter. Verified live: an introspective
# query returned two `loquwen_journal` capsules in the top hits, unmarked.
#
# Keyed on `location`, and that is SOUND here — I checked rather than assumed, having
# feared the opposite: all 2,275 loquwen-topic capsules carry location='loquwen/pulse'
# and ZERO escape a location-only filter. (CLAUDE.md's "1,645 stamped opus/direct"
# describes the state before the backfill.)
#
# Imported from bin/authorship.py, never restated — same rule as CLOSURE_HEADING.
import os as _os, sys as _sys
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
from authorship import NON_OPUS_LOCATIONS, is_mine


def _author_sql(alias="kc"):
    """(sql_fragment, params) excluding non-Opus writers. COALESCE is load-bearing:
    a bare != is FALSE for NULL, and 71,693 of my own capsules have NULL location."""
    frag = "".join(f" AND COALESCE({alias}.location,'') != ?" for _ in NON_OPUS_LOCATIONS)
    return frag, list(NON_OPUS_LOCATIONS)


DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")

# Ebbinghaus decay: λ controls how fast old capsules lose retrieval priority.
# λ=0.007 → 7-day capsule keeps 95%, 30-day keeps 81%, 90-day keeps 53%.
DECAY_LAMBDA = float(os.environ.get("CAPSULE_DECAY_LAMBDA", "0.007"))


def get_db():
    if not os.path.exists(DB_PATH):
        print(f"Database not found: {DB_PATH}", file=sys.stderr)
        sys.exit(1)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _parse_epoch(val, fallback):
    if val is None:
        return fallback
    try:
        return float(val)
    except (ValueError, TypeError):
        pass
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(str(val), fmt).timestamp()
        except ValueError:
            continue
    return fallback


def _ebbinghaus_factor(created_at_unix, now=None):
    if now is None:
        now = time.time()
    age_days = max(0, (now - created_at_unix) / 86400)
    return math.exp(-DECAY_LAMBDA * age_days)


def search(query, limit=5, topic=None, after=None, before=None, decay=False,
           include_others=False):
    db = get_db()
    where_extra = ""
    params = []
    _auth, _auth_params = ("", []) if include_others else _author_sql("kc")
    if topic:
        where_extra += " AND kc.topic LIKE ?"
        params.append(f"%{topic}%")
    if after:
        where_extra += " AND kc.created_at > ?"
        params.append(int(datetime.strptime(after, "%Y-%m-%d").timestamp()))
    if before:
        where_extra += " AND kc.created_at < ?"
        params.append(int(datetime.strptime(before, "%Y-%m-%d").timestamp()))

    fetch_limit = limit * 4 if decay else limit
    sql = f"""
        SELECT kc.id, kc.topic, kc.restatement,
               datetime(kc.created_at, 'unixepoch') as created,
               kc.created_at as created_epoch,
               kc.confidence_score,
               rank
        FROM capsules_fts ft
        JOIN knowledge_capsules kc ON ft.rowid = kc.id
        WHERE capsules_fts MATCH ?
          AND kc.superseded_at IS NULL
          {where_extra}{_auth}
        ORDER BY rank
        LIMIT ?
    """
    rows = db.execute(sql, [query] + params + _auth_params + [fetch_limit]).fetchall()
    db.close()

    if not decay or not rows:
        return rows[:limit]

    now = time.time()
    scored = []
    for row in rows:
        epoch = _parse_epoch(row["created_epoch"], now)
        recency = _ebbinghaus_factor(epoch, now)
        conf = row["confidence_score"] or 0.8
        # Confidence boosts resist decay: rehearsed memories fade slower
        salience = 1.0 + math.log1p(max(0, (conf - 0.8)) * 10)
        adjusted = row["rank"] * recency * (1.0 / salience)
        scored.append((adjusted, row))
    scored.sort(key=lambda x: x[0])
    return [s[1] for s in scored[:limit]]


def recent(limit=10):
    db = get_db()
    rows = db.execute("""
        SELECT id, topic, restatement,
               CASE
                   WHEN typeof(created_at) = 'integer' THEN datetime(created_at, 'unixepoch')
                   WHEN typeof(created_at) = 'real' THEN datetime(created_at, 'unixepoch')
                   ELSE created_at
               END as created,
               confidence_score
        FROM knowledge_capsules
        WHERE superseded_at IS NULL
        ORDER BY id DESC
        LIMIT ?
    """, [limit]).fetchall()
    db.close()
    return rows


def topics():
    db = get_db()
    rows = db.execute("""
        SELECT topic, COUNT(*) as cnt
        FROM knowledge_capsules
        WHERE superseded_at IS NULL
        GROUP BY topic
        ORDER BY cnt DESC
        LIMIT 50
    """).fetchall()
    db.close()
    return rows


def stats():
    db = get_db()
    total = db.execute("SELECT COUNT(*) FROM knowledge_capsules").fetchone()[0]
    active = db.execute("SELECT COUNT(*) FROM knowledge_capsules WHERE superseded_at IS NULL").fetchone()[0]
    fts_count = db.execute("SELECT COUNT(*) FROM capsules_fts").fetchone()[0]
    oldest = db.execute("SELECT datetime(MIN(created_at), 'unixepoch') FROM knowledge_capsules").fetchone()[0]
    newest = db.execute("SELECT datetime(MAX(created_at), 'unixepoch') FROM knowledge_capsules").fetchone()[0]
    topic_count = db.execute("SELECT COUNT(DISTINCT topic) FROM knowledge_capsules WHERE superseded_at IS NULL").fetchone()[0]
    db.close()
    return {
        "total": total, "active": active, "superseded": total - active,
        "fts_indexed": fts_count, "topics": topic_count,
        "oldest": oldest, "newest": newest
    }


def semantic_search(query, limit=5, topic=None, after=None, before=None,
                    include_others=False):
    """Embed query via Ollama, search capsule_embeddings by cosine similarity."""
    try:
        sys.path.insert(0, os.path.dirname(__file__))
        from embed_config import embed as embed_text
    except ImportError:
        print("ERROR: embed_config.py not found", file=sys.stderr)
        return []

    try:
        query_vec = embed_text(query)
    except Exception as e:
        print(f"ERROR: embedding query failed ({e})", file=sys.stderr)
        return []

    db = get_db()
    where_extra = ""
    params = []
    if topic:
        where_extra += " AND kc.topic LIKE ?"
        params.append(f"%{topic}%")
    if after:
        where_extra += " AND kc.created_at > ?"
        params.append(int(datetime.strptime(after, "%Y-%m-%d").timestamp()))
    if before:
        where_extra += " AND kc.created_at < ?"
        params.append(int(datetime.strptime(before, "%Y-%m-%d").timestamp()))

    _auth, _auth_params = ("", []) if include_others else _author_sql("kc")

    rows = db.execute(f"""
        SELECT ce.capsule_id, ce.embedding, kc.topic, kc.restatement,
               datetime(kc.created_at, 'unixepoch') as created,
               kc.confidence_score, kc.id
        FROM capsule_embeddings ce
        JOIN knowledge_capsules kc ON kc.id = ce.capsule_id
        WHERE kc.superseded_at IS NULL
          {where_extra}{_auth}
    """, params + _auth_params).fetchall()
    db.close()

    scored = []
    for row in rows:
        vec = _unpack_embedding(row[1])
        sim = _cosine_sim(query_vec, vec)
        scored.append((sim, {
            "id": row[0], "topic": row[2], "restatement": row[3],
            "created": row[4], "confidence_score": row[5], "similarity": sim,
        }))

    scored.sort(key=lambda x: -x[0])
    return [s[1] for s in scored[:limit]]


def _unpack_embedding(blob):
    n = len(blob) // 4
    return struct.unpack(f"{n}f", blob)


def _cosine_sim(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def graph_expand(db, seed_ids, limit=10):
    """Walk 1-hop neighbors from seed capsules via capsule_graph.
    Falls back to embedding similarity for seeds not in the graph."""
    if not seed_ids:
        return []

    placeholders = ",".join("?" for _ in seed_ids)
    graph_rows = db.execute(f"""
        SELECT DISTINCT kc.id, kc.topic, kc.restatement,
               datetime(kc.created_at, 'unixepoch') as created,
               kc.confidence_score,
               cg.quality, cg.similarity
        FROM capsule_graph cg
        JOIN knowledge_capsules kc ON kc.id = CASE
            WHEN cg.capsule_a IN ({placeholders}) THEN cg.capsule_b
            ELSE cg.capsule_a
        END
        WHERE (cg.capsule_a IN ({placeholders}) OR cg.capsule_b IN ({placeholders}))
          AND kc.id NOT IN ({placeholders})
          AND kc.superseded_at IS NULL
          AND cg.quality > 0.3
        ORDER BY cg.quality DESC
        LIMIT ?
    """, seed_ids * 4 + [limit]).fetchall()

    if graph_rows:
        return graph_rows

    seed_embeds = db.execute(f"""
        SELECT capsule_id, embedding FROM capsule_embeddings
        WHERE capsule_id IN ({placeholders})
    """, seed_ids).fetchall()
    if not seed_embeds:
        return []

    seed_vecs = {row[0]: _unpack_embedding(row[1]) for row in seed_embeds}
    avg_vec = [0.0] * len(next(iter(seed_vecs.values())))
    for v in seed_vecs.values():
        for i, x in enumerate(v):
            avg_vec[i] += x
    n = len(seed_vecs)
    avg_vec = [x / n for x in avg_vec]

    center = seed_ids[0]
    radius = 1000
    sample = db.execute("""
        SELECT ce.capsule_id, ce.embedding, kc.topic, kc.restatement,
               datetime(kc.created_at, 'unixepoch') as created,
               kc.confidence_score
        FROM capsule_embeddings ce
        JOIN knowledge_capsules kc ON kc.id = ce.capsule_id
        WHERE kc.superseded_at IS NULL
          AND ce.capsule_id NOT IN ({})
          AND ce.capsule_id BETWEEN ? AND ?
        LIMIT 2000
    """.format(placeholders), seed_ids + [center - radius, center + radius]).fetchall()

    scored = []
    for row in sample:
        vec = _unpack_embedding(row[1])
        sim = _cosine_sim(avg_vec, vec)
        scored.append((sim, row))

    scored.sort(key=lambda x: -x[0])
    results = []
    for sim, row in scored[:limit]:
        if sim < 0.35:
            continue
        results.append({
            "id": row[0], "topic": row[2], "restatement": row[3],
            "created": row[4], "confidence_score": row[5],
            "quality": round(sim * 2, 2), "similarity": round(sim, 3)
        })
    return results


def entity_expand(db, seed_ids, limit=10):
    """Find capsules that share entities with seed capsules."""
    if not seed_ids:
        return []
    placeholders = ",".join("?" for _ in seed_ids)
    entities = db.execute(f"""
        SELECT DISTINCT entity_name FROM capsule_entities
        WHERE capsule_id IN ({placeholders})
    """, seed_ids).fetchall()
    if not entities:
        return []
    enames = [e[0] for e in entities]
    ep = ",".join("?" for _ in enames)
    rows = db.execute(f"""
        SELECT DISTINCT kc.id, kc.topic, kc.restatement,
               datetime(kc.created_at, 'unixepoch') as created,
               kc.created_at as created_epoch,
               kc.confidence_score,
               ce.entity_name as via_entity
        FROM capsule_entities ce
        JOIN knowledge_capsules kc ON kc.id = ce.capsule_id
        WHERE ce.entity_name IN ({ep})
          AND ce.capsule_id NOT IN ({placeholders})
          AND kc.superseded_at IS NULL
        ORDER BY kc.created_at DESC
        LIMIT ?
    """, enames + seed_ids + [limit]).fetchall()
    return rows


def _embedding_neighbors(db, seed_ids, visited, limit=10):
    """Find similar capsules via embedding cosine when graph edges are sparse."""
    placeholders = ",".join("?" for _ in seed_ids)
    seed_embeds = db.execute(f"""
        SELECT capsule_id, embedding FROM capsule_embeddings
        WHERE capsule_id IN ({placeholders})
    """, seed_ids).fetchall()
    if not seed_embeds:
        return []
    seed_vecs = {row[0]: _unpack_embedding(row[1]) for row in seed_embeds}
    avg_vec = [0.0] * len(next(iter(seed_vecs.values())))
    for v in seed_vecs.values():
        for i, x in enumerate(v):
            avg_vec[i] += x
    n = len(seed_vecs)
    avg_vec = [x / n for x in avg_vec]

    visited_p = ",".join("?" for _ in visited)
    sample = db.execute(f"""
        SELECT ce.capsule_id, ce.embedding, kc.topic, kc.restatement,
               datetime(kc.created_at, 'unixepoch') as created,
               kc.confidence_score
        FROM capsule_embeddings ce
        JOIN knowledge_capsules kc ON kc.id = ce.capsule_id
        WHERE kc.superseded_at IS NULL
          AND ce.capsule_id NOT IN ({visited_p})
        ORDER BY RANDOM()
        LIMIT 3000
    """, list(visited)).fetchall()

    scored = []
    for row in sample:
        vec = _unpack_embedding(row[1])
        sim = _cosine_sim(avg_vec, vec)
        if sim > 0.45:
            scored.append((sim, row))
    scored.sort(key=lambda x: -x[0])
    return scored[:limit]


def chain_search(query, hops=2, limit=20, topic=None, after=None, before=None):
    """Multi-hop evidence chain: FTS5 seeds → graph walk N hops → embedding fallback → entity join."""
    db = get_db()
    seeds = search(query, limit=5, topic=topic, after=after, before=before)
    if not seeds:
        db.close()
        return {"seeds": [], "hops": [], "entities": [], "embeddings": [], "chain": []}

    seed_ids = [r["id"] for r in seeds]
    visited = set(seed_ids)
    all_hops = []

    frontier = list(seed_ids)
    for hop in range(hops):
        if not frontier:
            break
        fp = ",".join("?" for _ in frontier)
        neighbors = db.execute(f"""
            SELECT DISTINCT kc.id, kc.topic, kc.restatement,
                   datetime(kc.created_at, 'unixepoch') as created,
                   kc.created_at as created_epoch,
                   kc.confidence_score,
                   cg.quality, cg.similarity
            FROM capsule_graph cg
            JOIN knowledge_capsules kc ON kc.id = CASE
                WHEN cg.capsule_a IN ({fp}) THEN cg.capsule_b
                ELSE cg.capsule_a
            END
            WHERE (cg.capsule_a IN ({fp}) OR cg.capsule_b IN ({fp}))
              AND kc.superseded_at IS NULL
              AND cg.quality > 0.4
            ORDER BY cg.quality DESC
            LIMIT ?
        """, frontier * 3 + [limit]).fetchall()

        new_frontier = []
        for row in neighbors:
            rid = row["id"]
            if rid not in visited:
                visited.add(rid)
                all_hops.append({"hop": hop + 1, "row": row})
                new_frontier.append(rid)
        frontier = new_frontier[:limit // 2]

    embed_results = []
    if len(all_hops) < 3:
        scored = _embedding_neighbors(db, seed_ids, visited, limit=8)
        for sim, row in scored:
            rid = row[0]
            if rid not in visited:
                visited.add(rid)
                embed_results.append({"id": rid, "topic": row[2], "restatement": row[3],
                                       "created": row[4], "similarity": round(sim, 3)})

    entity_results = entity_expand(db, list(visited)[:50], limit=10)
    entity_new = []
    for row in entity_results:
        if row["id"] not in visited:
            visited.add(row["id"])
            entity_new.append(row)

    chain = []
    for s in seeds:
        chain.append({"source": "seed", "id": s["id"], "topic": s["topic"] or "—",
                       "text": s["restatement"], "created": s["created"]})
    for h in all_hops:
        r = h["row"]
        chain.append({"source": f"hop-{h['hop']}", "id": r["id"],
                       "topic": r["topic"] or "—", "text": r["restatement"],
                       "created": r["created"],
                       "quality": r["quality"], "similarity": r["similarity"]})
    for e in embed_results:
        chain.append({"source": "embed", "id": e["id"], "topic": e["topic"] or "—",
                       "text": e["restatement"], "created": e["created"],
                       "similarity": e["similarity"]})
    for e in entity_new:
        chain.append({"source": "entity", "id": e["id"], "topic": e["topic"] or "—",
                       "text": e["restatement"], "created": e["created"],
                       "via": e["via_entity"]})

    db.close()
    return {"seeds": seeds, "hops": all_hops, "embeddings": embed_results,
            "entities": entity_new, "chain": chain}


def neighbors(capsule_id, limit=15):
    """Get direct graph neighbors of a capsule, ranked by quality."""
    db = get_db()
    cap = db.execute(
        "SELECT id, topic, restatement, datetime(created_at, 'unixepoch') as created "
        "FROM knowledge_capsules WHERE id = ?", [capsule_id]
    ).fetchone()
    if not cap:
        db.close()
        return None, []
    rows = db.execute("""
        SELECT kc.id, kc.topic, kc.restatement,
               datetime(kc.created_at, 'unixepoch') as created,
               kc.confidence_score,
               cg.quality, cg.similarity
        FROM capsule_graph cg
        JOIN knowledge_capsules kc ON kc.id = CASE
            WHEN cg.capsule_a = ? THEN cg.capsule_b
            ELSE cg.capsule_a
        END
        WHERE (cg.capsule_a = ? OR cg.capsule_b = ?)
          AND kc.superseded_at IS NULL
        ORDER BY cg.quality DESC
        LIMIT ?
    """, [capsule_id, capsule_id, capsule_id, limit]).fetchall()
    db.close()
    return cap, rows


def format_result(row, show_full=False, graph_meta=False):
    text = row["restatement"]
    if not show_full and len(text) > 300:
        text = text[:300] + "..."
    created = row["created"]
    topic = row["topic"] or "—"
    meta = ""
    if graph_meta:
        try:
            meta = f" | q={row['quality']:.2f} sim={row['similarity']:.3f}"
        except (IndexError, KeyError):
            pass
    return f"[{row['id']}] {created} | {topic}{meta}\n{text}\n"


def main():
    parser = argparse.ArgumentParser(description="Search Chronicle memory capsules")
    parser.add_argument("query", nargs="?", help="FTS5 search query (supports AND, OR, NOT, phrases)")
    parser.add_argument("--limit", "-n", type=int, default=5)
    parser.add_argument("--include-others", action="store_true",
                        help="INCLUDE capsules written by non-Opus local models "
                             "(LoQwen). Excluded by default. READ THEM, DO NOT CITE "
                             "THEM — resolve every referent before use.")
    parser.add_argument("--topic", "-t", help="Filter by topic (substring match)")
    parser.add_argument("--after", help="Only capsules after date (YYYY-MM-DD)")
    parser.add_argument("--before", help="Only capsules before date (YYYY-MM-DD)")
    parser.add_argument("--full", "-f", action="store_true", help="Show full text (no truncation)")
    parser.add_argument("--decay", action="store_true", help="Enable Ebbinghaus temporal decay (recency bias)")
    parser.add_argument("--graph", "-g", action="store_true", help="Expand results via capsule_graph")
    parser.add_argument("--chain", "-c", action="store_true", help="Multi-hop evidence chain (FTS5 → graph walk → entity join)")
    parser.add_argument("--hops", type=int, default=2, help="Number of graph hops for --chain (default: 2)")
    parser.add_argument("--neighbors", type=int, metavar="ID", help="Show graph neighbors of capsule ID")
    parser.add_argument("--recent", "-r", type=int, metavar="N", help="Show N most recent capsules")
    parser.add_argument("--topics", action="store_true", help="List topics by count")
    parser.add_argument("--semantic", "-s", action="store_true",
                        help="Semantic search via embeddings (meaning-based, not keyword)")
    parser.add_argument("--stats", action="store_true", help="Show database statistics")
    args = parser.parse_args()

    if args.stats:
        s = stats()
        print(f"Capsules: {s['total']} total, {s['active']} active, {s['superseded']} superseded")
        print(f"FTS indexed: {s['fts_indexed']}")
        print(f"Topics: {s['topics']}")
        print(f"Range: {s['oldest']} → {s['newest']}")
        db = get_db()
        edges = db.execute("SELECT COUNT(*) FROM capsule_graph").fetchone()[0]
        nodes = db.execute(
            "SELECT COUNT(DISTINCT cid) FROM ("
            "SELECT capsule_a as cid FROM capsule_graph UNION "
            "SELECT capsule_b as cid FROM capsule_graph)"
        ).fetchone()[0]
        db.close()
        print(f"Graph: {edges} edges, {nodes} connected capsules")
        return

    if args.topics:
        for row in topics():
            print(f"  {row['cnt']:>5}  {row['topic']}")
        return

    if args.neighbors:
        cap, nbs = neighbors(args.neighbors, args.limit)
        if not cap:
            print(f"Capsule {args.neighbors} not found.")
            return
        print(f"Capsule [{cap['id']}] {cap['created']} | {cap['topic'] or '—'}")
        text = cap["restatement"]
        if not args.full and len(text) > 200:
            text = text[:200] + "..."
        print(f"  {text}\n")
        if not nbs:
            print("  No graph neighbors.")
            return
        print(f"{len(nbs)} neighbor(s):\n")
        for row in nbs:
            print(format_result(row, args.full, graph_meta=True))
        return

    if args.recent:
        rows = recent(args.recent)
        if not rows:
            print("No capsules found.")
            return
        for row in rows:
            print(format_result(row, args.full))
        return

    if not args.query:
        parser.print_help()
        return

    if args.semantic:
        rows = semantic_search(args.query, args.limit, args.topic,
                               args.after, args.before,
                               include_others=args.include_others)
        if not rows:
            print(f"No semantic results for: {args.query}")
            return
        print(f"Found {len(rows)} semantic result(s) for: {args.query}\n")
        for row in rows:
            sim = row.get("similarity", 0)
            text = row["restatement"]
            if not args.full and len(text) > 300:
                text = text[:300] + "..."
            print(f"[{row['id']}] {row['created']} | {row['topic'] or '—'} | sim={sim:.3f}")
            print(f"  {text}\n")
        return

    if args.chain:
        result = chain_search(args.query, hops=args.hops, limit=args.limit,
                              topic=args.topic, after=args.after, before=args.before)
        if not result["chain"]:
            print(f"No results for: {args.query}")
            return
        print(f"Evidence chain for: {args.query}")
        print(f"  {len(result['seeds'])} seed(s), {len(result['hops'])} graph hop(s), "
              f"{len(result['entities'])} entity link(s)\n")
        for item in result["chain"]:
            text = item["text"]
            if not args.full and len(text) > 300:
                text = text[:300] + "..."
            tag = item["source"]
            extra = ""
            if "quality" in item:
                extra = f" q={item['quality']:.2f}"
            if "via" in item:
                extra = f" via={item['via']}"
            print(f"[{item['id']}] {item['created']} | {item['topic']} | {tag}{extra}")
            print(f"  {text}\n")
        return

    rows = search(args.query, args.limit, args.topic, args.after, args.before,
                  decay=args.decay)
    if not rows:
        print(f"No results for: {args.query}")
        return

    print(f"Found {len(rows)} result(s) for: {args.query}\n")
    for row in rows:
        print(format_result(row, args.full))

    if args.graph and rows:
        seed_ids = [row["id"] for row in rows]
        db = get_db()
        expanded = graph_expand(db, seed_ids, args.limit)
        db.close()
        new = [r for r in expanded if r["id"] not in set(seed_ids)]
        if new:
            print(f"\n── Graph expansion ({len(new)} associated) ──\n")
            for row in new:
                print(format_result(row, args.full, graph_meta=True))


if __name__ == "__main__":
    main()
