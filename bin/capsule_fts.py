#!/usr/bin/env python3
"""FTS5 full-text search for knowledge capsules.

Build #20: Hybrid retrieval (BM25 + vector) — from HyperMem deep-read.
Build #22: Three-signal retrieval (BM25 + vector + KG traversal) — from AIngram.
NOTE: KG signal currently adds noise with our first-pass relations (26% related_to).
Use 'hybrid' for production retrieval. 'tri' is experimental until KG quality improves.

Usage:
    capsule_fts.py build           # Create/rebuild FTS5 index
    capsule_fts.py search "query"  # BM25 keyword search
    capsule_fts.py hybrid "query"  # Fuse BM25 + FAISS (Reciprocal Rank Fusion)
    capsule_fts.py tri "query"     # Three-signal: BM25 + FAISS + KG traversal
"""
import json
import os
import sqlite3
import sys

DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")
FAISS_PATH = "/mnt/hdd/chronicle-data/capsules.faiss"
IDS_PATH = "/mnt/hdd/chronicle-data/capsules_ids.json"
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://192.168.1.11:11434")
EMBED_MODEL = "nomic-embed-text"  # 768d, matches existing FAISS index


def build_index():
    """Create or rebuild FTS5 index on capsule restatements."""
    db = sqlite3.connect(DB_PATH)

    # Drop existing if rebuilding
    db.execute("DROP TABLE IF EXISTS capsules_fts")

    # Create FTS5 virtual table
    db.execute("""
        CREATE VIRTUAL TABLE capsules_fts USING fts5(
            restatement,
            topic,
            content=knowledge_capsules,
            content_rowid=id,
            tokenize='porter unicode61'
        )
    """)

    # Populate from existing capsules
    db.execute("""
        INSERT INTO capsules_fts(rowid, restatement, topic)
        SELECT id, restatement, COALESCE(topic, '')
        FROM knowledge_capsules
        WHERE consolidated_into IS NULL
    """)

    count = db.execute("SELECT count(*) FROM capsules_fts").fetchone()[0]
    db.commit()
    db.close()
    print(f"Built FTS5 index: {count} capsules indexed")

    # Create trigger for auto-update on new capsules
    db = sqlite3.connect(DB_PATH)
    db.execute("DROP TRIGGER IF EXISTS capsules_fts_insert")
    db.execute("""
        CREATE TRIGGER capsules_fts_insert AFTER INSERT ON knowledge_capsules
        BEGIN
            INSERT INTO capsules_fts(rowid, restatement, topic)
            VALUES (NEW.id, NEW.restatement, COALESCE(NEW.topic, ''));
        END
    """)
    db.commit()
    db.close()
    print("Auto-insert trigger created")


def sanitize_fts_query(query):
    """Make a query safe for FTS5 — quote terms, handle hyphens."""
    # Split on whitespace, quote each term to avoid FTS5 syntax issues
    terms = query.split()
    quoted = []
    for t in terms:
        # Remove FTS5 special chars, keep alphanumeric and hyphens
        clean = t.strip('"\'(){}[]')
        if clean:
            quoted.append(f'"{clean}"')
    return " OR ".join(quoted) if quoted else query


def search_bm25(query, limit=10):
    """BM25 keyword search via FTS5."""
    db = sqlite3.connect(DB_PATH)
    safe_query = sanitize_fts_query(query)
    try:
        rows = db.execute("""
            SELECT kc.id, kc.restatement, kc.topic, kc.created_at,
                   rank AS bm25_score
            FROM capsules_fts
            JOIN knowledge_capsules kc ON kc.id = capsules_fts.rowid
            WHERE capsules_fts MATCH ?
            ORDER BY rank
            LIMIT ?
        """, (safe_query, limit)).fetchall()
    except Exception:
        rows = []
    db.close()
    return rows


def search_faiss(query, limit=10):
    """Vector similarity search via FAISS."""
    try:
        import faiss
        import numpy as np
        import urllib.request
    except ImportError:
        print("FAISS not available, using BM25 only", file=sys.stderr)
        return []

    # Embed query
    req = urllib.request.Request(
        f"{OLLAMA_URL}/api/embed",
        data=json.dumps({"model": EMBED_MODEL, "input": query}).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            result = json.loads(resp.read())
        embedding = np.array(result["embeddings"][0], dtype=np.float32).reshape(1, -1)
    except Exception as e:
        print(f"Embedding error: {e}", file=sys.stderr)
        return []

    # Load FAISS index
    try:
        index = faiss.read_index(FAISS_PATH)
        with open(IDS_PATH) as f:
            id_map = json.load(f)
        int_to_ext = id_map["int_to_ext"]
    except Exception as e:
        print(f"FAISS load error: {e}", file=sys.stderr)
        return []

    # Search
    faiss.normalize_L2(embedding)
    distances, indices = index.search(embedding, limit)

    results = []
    db = sqlite3.connect(DB_PATH)
    for dist, idx in zip(distances[0], indices[0]):
        if idx < 0:
            continue
        capsule_id = int_to_ext.get(str(idx))
        if capsule_id is None:
            continue
        row = db.execute(
            "SELECT id, restatement, topic, created_at FROM knowledge_capsules WHERE id = ?",
            (capsule_id,),
        ).fetchone()
        if row:
            results.append((*row, float(dist)))
    db.close()
    return results


def search_kg(query, limit=10):
    """Knowledge graph traversal: find capsules connected to query entities.

    1. Match query terms to kg_entities (fuzzy via LIKE)
    2. Traverse kg_relationships to find neighbor entities (1-hop)
    3. Find capsules containing those entities via capsule_entities
    """
    db = sqlite3.connect(DB_PATH)

    # Extract candidate terms from query (words > 3 chars)
    terms = [w for w in query.split() if len(w) > 3]
    if not terms:
        db.close()
        return []

    # Step 1: Find matching entities
    entity_ids = set()
    for term in terms[:5]:  # Cap at 5 terms
        rows = db.execute(
            "SELECT id FROM kg_entities WHERE canonical_name LIKE ? LIMIT 5",
            (f"%{term}%",),
        ).fetchall()
        entity_ids.update(r[0] for r in rows)

    if not entity_ids:
        db.close()
        return []

    # Step 2: 1-hop graph traversal via CTE
    placeholders = ",".join("?" * len(entity_ids))
    neighbor_names = db.execute(f"""
        WITH seed AS (SELECT id FROM kg_entities WHERE id IN ({placeholders})),
        neighbors AS (
            SELECT target_entity AS eid FROM kg_relationships
            WHERE source_entity IN (SELECT id FROM seed)
            UNION
            SELECT source_entity AS eid FROM kg_relationships
            WHERE target_entity IN (SELECT id FROM seed)
        )
        SELECT DISTINCT e.canonical_name
        FROM neighbors n JOIN kg_entities e ON e.id = n.eid
        LIMIT 30
    """, list(entity_ids)).fetchall()

    if not neighbor_names:
        db.close()
        return []

    # Step 3: Find capsules mentioning neighbor entities
    # Try capsule_entities first, then fall back to FTS5 on entity names
    name_list = [n[0] for n in neighbor_names]
    name_placeholders = ",".join("?" * len(name_list))
    rows = db.execute(f"""
        SELECT DISTINCT kc.id, kc.restatement, kc.topic, kc.created_at
        FROM capsule_entities ce
        JOIN knowledge_capsules kc ON kc.id = ce.capsule_id
        WHERE ce.entity_name IN ({name_placeholders})
        AND kc.consolidated_into IS NULL
        ORDER BY kc.created_at DESC
        LIMIT ?
    """, name_list + [limit]).fetchall()

    # If capsule_entities bridge is thin, boost via FTS5 on neighbor names
    if len(rows) < limit // 2:
        # Build an OR query from neighbor names (top 5 by relevance)
        fts_terms = " OR ".join(f'"{n}"' for n in name_list[:5] if len(n) > 2)
        if fts_terms:
            try:
                fts_rows = db.execute("""
                    SELECT kc.id, kc.restatement, kc.topic, kc.created_at
                    FROM capsules_fts
                    JOIN knowledge_capsules kc ON kc.id = capsules_fts.rowid
                    WHERE capsules_fts MATCH ?
                    ORDER BY rank LIMIT ?
                """, (fts_terms, limit)).fetchall()
                seen = {r[0] for r in rows}
                for r in fts_rows:
                    if r[0] not in seen:
                        rows.append(r)
                        seen.add(r[0])
            except Exception:
                pass  # FTS5 query syntax error is non-fatal

    db.close()
    return [(r[0], r[1], r[2], r[3], 1.0) for r in rows[:limit]]


def reciprocal_rank_fusion(bm25_results, faiss_results, k=60, kg_results=None):
    """Fuse BM25, FAISS, and optionally KG results via Reciprocal Rank Fusion.

    RRF score = sum(1 / (k + rank)) across all rankers.
    k=60 is the standard constant from the original RRF paper.
    Three signals: keyword match (BM25), semantic similarity (FAISS),
    graph proximity (KG traversal).
    """
    scores = {}
    data = {}
    sources = {}

    # BM25 ranks (already sorted by relevance)
    for rank, row in enumerate(bm25_results):
        cid = row[0]
        scores[cid] = scores.get(cid, 0) + 1.0 / (k + rank + 1)
        data[cid] = row[:4]  # id, restatement, topic, created_at
        sources.setdefault(cid, set()).add("bm25")

    # FAISS ranks (already sorted by similarity)
    for rank, row in enumerate(faiss_results):
        cid = row[0]
        scores[cid] = scores.get(cid, 0) + 1.0 / (k + rank + 1)
        data[cid] = row[:4]
        sources.setdefault(cid, set()).add("faiss")

    # KG traversal ranks (if provided)
    if kg_results:
        for rank, row in enumerate(kg_results):
            cid = row[0]
            scores[cid] = scores.get(cid, 0) + 1.0 / (k + rank + 1)
            if cid not in data:
                data[cid] = row[:4]
            sources.setdefault(cid, set()).add("kg")

    # Sort by fused score descending
    ranked = sorted(scores.items(), key=lambda x: -x[1])
    return [(data[cid], score, sources.get(cid, set())) for cid, score in ranked if cid in data]


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return

    cmd = sys.argv[1]

    if cmd == "build":
        build_index()

    elif cmd == "search":
        query = sys.argv[2] if len(sys.argv) > 2 else ""
        if not query:
            print("Usage: capsule_fts.py search 'query'")
            return
        results = search_bm25(query, limit=10)
        if not results:
            print("No BM25 matches.")
            return
        print(f"BM25 results for '{query}':\n")
        for cid, text, topic, ts, score in results:
            short = text[:200].replace("\n", " ")
            print(f"  [{cid}] ({topic}) score={score:.4f}")
            print(f"    {short}")
            print()

    elif cmd == "hybrid":
        query = sys.argv[2] if len(sys.argv) > 2 else ""
        if not query:
            print("Usage: capsule_fts.py hybrid 'query'")
            return
        bm25 = search_bm25(query, limit=20)
        faiss_r = search_faiss(query, limit=20)
        fused = reciprocal_rank_fusion(bm25, faiss_r)
        if not fused:
            print("No results.")
            return
        print(f"Hybrid (BM25+FAISS RRF) results for '{query}':\n")
        for (cid, text, topic, ts), score, srcs in fused[:10]:
            short = text[:200].replace("\n", " ")
            print(f"  [{cid}] ({topic}) rrf={score:.4f} [{'+'.join(sorted(srcs))}]")
            print(f"    {short}")
            print()

    elif cmd == "tri":
        query = sys.argv[2] if len(sys.argv) > 2 else ""
        if not query:
            print("Usage: capsule_fts.py tri 'query'")
            return
        bm25 = search_bm25(query, limit=20)
        faiss_r = search_faiss(query, limit=20)
        kg = search_kg(query, limit=20)
        fused = reciprocal_rank_fusion(bm25, faiss_r, kg_results=kg)
        if not fused:
            print("No results.")
            return
        sig_count = sum(1 for s in [bm25, faiss_r, kg] if s)
        print(f"Three-signal ({sig_count}/3 active) results for '{query}':\n")
        for (cid, text, topic, ts), score, srcs in fused[:10]:
            short = text[:200].replace("\n", " ")
            print(f"  [{cid}] ({topic}) rrf={score:.4f} [{'+'.join(sorted(srcs))}]")
            print(f"    {short}")
            print()

    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)


if __name__ == "__main__":
    main()
