#!/usr/bin/env python3
"""Hard-prune noise-bucket capsules and rebuild FAISS.

Removes feed/*, intern/*, crossref/*, and ex-provocateur/ex-analyst conversation
capsules from knowledge_capsules and every child table. Rebuilds capsules.faiss
from scratch using only the remaining embeddings.

This is intentionally destructive. Run the parallel backup first.
"""

import os
import sqlite3
import struct
import sys
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from vector_index import VectorIndex

DB = "/mnt/hdd/chronicle-data/processed.db"
FAISS_PATH = "/mnt/hdd/chronicle-data/capsules.faiss"

PRUNE_WHERE = """(
     topic LIKE 'feed/%'
  OR topic LIKE 'intern/%'
  OR topic LIKE 'crossref/%'
  OR conversation_id LIKE 'intern%'
  OR conversation_id LIKE 'crossref%'
  OR conversation_id LIKE 'provocateur%'
  OR conversation_id LIKE 'analyst%'
)"""


def main():
    t0 = time.time()
    conn = sqlite3.connect(DB, timeout=120.0)
    conn.execute("PRAGMA busy_timeout = 120000")
    conn.execute("PRAGMA foreign_keys = OFF")

    cur = conn.cursor()

    victims = [r[0] for r in cur.execute(
        f"SELECT id FROM knowledge_capsules WHERE {PRUNE_WHERE}").fetchall()]
    victim_set = set(victims)
    print(f"[prune] identified {len(victims)} victim capsules")

    cur.execute("BEGIN IMMEDIATE")

    # Null out any active pointers into the prune set
    cur.execute(f"""
        UPDATE knowledge_capsules SET superseded_by = NULL
         WHERE superseded_by IN (SELECT id FROM knowledge_capsules WHERE {PRUNE_WHERE})
    """)
    print(f"[prune] cleared {cur.rowcount} superseded_by pointers into prune set")

    cur.execute(f"""
        UPDATE knowledge_capsules SET consolidated_into = NULL
         WHERE consolidated_into IN (SELECT id FROM knowledge_capsules WHERE {PRUNE_WHERE})
    """)
    print(f"[prune] cleared {cur.rowcount} consolidated_into pointers into prune set")

    cur.execute(f"""
        DELETE FROM capsule_contradictions
         WHERE newer_id IN (SELECT id FROM knowledge_capsules WHERE {PRUNE_WHERE})
            OR older_id IN (SELECT id FROM knowledge_capsules WHERE {PRUNE_WHERE})
    """)
    print(f"[prune] deleted {cur.rowcount} contradiction rows")

    for child in ("capsule_embeddings", "capsule_persons", "capsule_entities",
                  "capsule_keywords", "capsule_patterns"):
        cur.execute(f"""
            DELETE FROM {child}
             WHERE capsule_id IN (SELECT id FROM knowledge_capsules WHERE {PRUNE_WHERE})
        """)
        print(f"[prune] deleted {cur.rowcount} rows from {child}")

    cur.execute(f"""
        DELETE FROM capsule_relations
         WHERE source_id IN (SELECT id FROM knowledge_capsules WHERE {PRUNE_WHERE})
            OR target_id IN (SELECT id FROM knowledge_capsules WHERE {PRUNE_WHERE})
    """)
    print(f"[prune] deleted {cur.rowcount} capsule_relations rows")

    cur.execute(f"DELETE FROM knowledge_capsules WHERE {PRUNE_WHERE}")
    print(f"[prune] deleted {cur.rowcount} knowledge_capsules rows")

    conn.commit()
    print(f"[prune] SQL delete phase: {time.time()-t0:.1f}s")

    # Rebuild FTS
    t1 = time.time()
    conn.execute("INSERT INTO capsules_fts(capsules_fts) VALUES('rebuild')")
    conn.commit()
    print(f"[prune] FTS rebuild: {time.time()-t1:.1f}s")

    # Rebuild FAISS from surviving embeddings
    t2 = time.time()
    os.rename(FAISS_PATH, FAISS_PATH + ".pre-rebuild")
    ids_path = FAISS_PATH.replace(".faiss", "_ids.json")
    os.rename(ids_path, ids_path + ".pre-rebuild")

    idx = VectorIndex(FAISS_PATH)
    print(f"[prune] starting FAISS rebuild (fresh index)")

    rows = conn.execute(
        "SELECT capsule_id, embedding FROM capsule_embeddings "
        "ORDER BY capsule_id").fetchall()
    print(f"[prune] {len(rows)} embeddings to index")

    batch_ids, batch_vecs = [], []
    for cap_id, blob in rows:
        n = len(blob) // 4
        vec = list(struct.unpack(f"{n}f", blob))
        batch_ids.append(cap_id)
        batch_vecs.append(vec)
        if len(batch_ids) >= 1000:
            idx.add(batch_ids, batch_vecs)
            batch_ids, batch_vecs = [], []
            if idx.count() % 2000 == 0:
                print(f"  indexed {idx.count()}...")
    if batch_ids:
        idx.add(batch_ids, batch_vecs)

    idx.save()
    print(f"[prune] FAISS rebuild: {time.time()-t2:.1f}s, {idx.count()} vectors")

    # Final counts
    remaining = conn.execute(
        "SELECT COUNT(*), COUNT(CASE WHEN superseded_at IS NULL THEN 1 END) "
        "FROM knowledge_capsules").fetchone()
    print(f"[prune] final: {remaining[0]} capsules total, {remaining[1]} active")
    print(f"[prune] total elapsed: {time.time()-t0:.1f}s")

    conn.close()


if __name__ == "__main__":
    main()
