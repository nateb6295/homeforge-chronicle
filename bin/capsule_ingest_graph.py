#!/usr/bin/env python3
"""Ingest-time graph builder — turns every capsule write into a graph operation.

Called after capsule insert + embed. For each new capsule:
1. Extract entities from text (lightweight, no LLM)
2. Find similar capsules via cached embeddings
3. Create edges in capsule_graph
4. Store entities in capsule_entities

Usage:
    # Called programmatically from capsule_sync.py:
    from capsule_ingest_graph import build_graph_for_capsules
    build_graph_for_capsules(db, [id1, id2, ...])

    # Or standalone:
    python3 capsule_ingest_graph.py 75001 75002 75003
    python3 capsule_ingest_graph.py --recent 50   # process 50 most recent unconnected
"""

import math
import os
import re
import sqlite3
import struct
import sys
import time

DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")

MAX_EDGES_PER_CAPSULE = 10
SIMILARITY_THRESHOLD = 0.45
NEIGHBOR_WINDOW = 800

PERSON_PATTERNS = [
    r'\b(Nate|Opus|Gemma|Kimi|Hermes|Mistral|Gregory|Macrina|Weil|Merleau-Ponty)\b',
    r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\b',
]

TOPIC_KEYWORDS = {
    'spectral': 'spectral-demon',
    'σ₁': 'spectral-demon',
    'σ₂': 'spectral-demon',
    'CCS': 'ccs',
    'compression': 'ccs',
    'Gregory': 'gregory-nyssa',
    'Nyssa': 'gregory-nyssa',
    'capsule': 'memory-system',
    'canister': 'infrastructure',
    'discord': 'communication',
    'thread': 'threads',
    'DREAM': 'dream-cycle',
    'wallet': 'sovereignty',
    'ICP': 'sovereignty',
    'XRPL': 'sovereignty',
}

KNOWN_PERSONS = {
    'Nate', 'Opus', 'Gemma', 'Kimi', 'Hermes', 'Mistral',
    'Gregory', 'Macrina', 'Weil', 'Teilhard', 'Frost',
    'Leo', 'Simone', 'Bradford',
}


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


def extract_entities(text):
    """Extract entities from capsule text using patterns."""
    entities = []
    for person in KNOWN_PERSONS:
        if person.lower() in text.lower():
            entities.append((person, 'person'))

    for keyword, topic in TOPIC_KEYWORDS.items():
        if keyword in text:
            entities.append((topic, 'topic'))

    seen = set()
    unique = []
    for name, etype in entities:
        key = (name.lower(), etype)
        if key not in seen:
            seen.add(key)
            unique.append((name, etype))
    return unique


def _quality_score(similarity):
    return round(similarity * 2, 4) if similarity < 0.5 else round(similarity * 1.8, 4)


def build_graph_for_capsules(db, capsule_ids, verbose=False):
    """Build graph edges and extract entities for a list of capsule IDs."""
    if not capsule_ids:
        return 0, 0

    edges_created = 0
    entities_created = 0

    for cid in capsule_ids:
        row = db.execute(
            "SELECT id, restatement, topic, created_at FROM knowledge_capsules WHERE id = ?",
            [cid]
        ).fetchone()
        if not row:
            continue

        text = row[1] or ""
        topic = row[2] or ""

        entities = extract_entities(text)
        for ename, etype in entities:
            try:
                db.execute(
                    "INSERT OR IGNORE INTO capsule_entities (capsule_id, entity_name, entity_type) "
                    "VALUES (?, ?, ?)",
                    (cid, ename, etype)
                )
                entities_created += 1
            except Exception:
                pass

        embed_row = db.execute(
            "SELECT embedding FROM capsule_embeddings WHERE capsule_id = ?", [cid]
        ).fetchone()
        if not embed_row:
            continue
        vec = _unpack_embedding(embed_row[0])

        existing_edges = db.execute(
            "SELECT COUNT(*) FROM capsule_graph WHERE capsule_a = ? OR capsule_b = ?",
            [cid, cid]
        ).fetchone()[0]
        if existing_edges >= MAX_EDGES_PER_CAPSULE:
            continue

        neighbors = db.execute("""
            SELECT ce.capsule_id, ce.embedding
            FROM capsule_embeddings ce
            JOIN knowledge_capsules kc ON kc.id = ce.capsule_id
            WHERE kc.superseded_at IS NULL
              AND ce.capsule_id != ?
              AND ce.capsule_id BETWEEN ? AND ?
            LIMIT 500
        """, [cid, max(1, cid - NEIGHBOR_WINDOW), cid + NEIGHBOR_WINDOW]).fetchall()

        scored = []
        for nrow in neighbors:
            nid = nrow[0]
            nvec = _unpack_embedding(nrow[1])
            sim = _cosine_sim(vec, nvec)
            if sim >= SIMILARITY_THRESHOLD:
                scored.append((sim, nid))

        scored.sort(key=lambda x: -x[0])
        slots = MAX_EDGES_PER_CAPSULE - existing_edges

        for sim, nid in scored[:slots]:
            quality = _quality_score(sim)
            try:
                a, b = min(cid, nid), max(cid, nid)
                db.execute(
                    "INSERT OR IGNORE INTO capsule_graph "
                    "(capsule_a, capsule_b, similarity, quality, created_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (a, b, round(sim, 4), quality, int(time.time()))
                )
                edges_created += 1
            except Exception:
                pass

        if verbose and edges_created > 0:
            print(f"  [{cid}] +{min(len(scored), slots)} edges, +{len(entities)} entities")

    db.commit()
    return edges_created, entities_created


def process_unconnected(db, limit=50, verbose=False):
    """Find and process capsules that have no graph edges yet."""
    rows = db.execute("""
        SELECT kc.id FROM knowledge_capsules kc
        LEFT JOIN capsule_graph cg ON kc.id = cg.capsule_a OR kc.id = cg.capsule_b
        JOIN capsule_embeddings ce ON ce.capsule_id = kc.id
        WHERE cg.id IS NULL AND kc.superseded_at IS NULL
        ORDER BY kc.id DESC
        LIMIT ?
    """, [limit]).fetchall()

    ids = [r[0] for r in rows]
    if verbose:
        print(f"Found {len(ids)} unconnected capsules")
    return build_graph_for_capsules(db, ids, verbose=verbose)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Ingest-time graph builder")
    parser.add_argument("ids", nargs="*", type=int, help="Capsule IDs to process")
    parser.add_argument("--recent", type=int, metavar="N", help="Process N most recent unconnected")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row

    if args.recent:
        edges, entities = process_unconnected(db, args.recent, verbose=args.verbose)
        print(f"Created {edges} edges, {entities} entities for unconnected capsules")
    elif args.ids:
        edges, entities = build_graph_for_capsules(db, args.ids, verbose=args.verbose)
        print(f"Created {edges} edges, {entities} entities")
    else:
        parser.print_help()

    db.close()


if __name__ == "__main__":
    main()
