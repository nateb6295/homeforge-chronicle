#!/usr/bin/env python3
"""keeper_connect.py — Event-driven bounded connection graph for Keeper capsules.

Replaces the retired compost timer. All compute happens locally via Ollama
embeddings; Keeper canister is storage only.

Design:
  - Event-driven: process NEW capsules against a neighbor window, not all-pairs
  - Bounded writes: max 15 connections per capsule, weakest displaced
  - Landmark capsules: top 50 by centrality, force-reconnected on wider cadence
  - Era buckets: every ~500 capsules, cross-era sampling prevents temporal gulags
  - Quality scoring matches Keeper's Rust logic exactly

Usage:
  keeper_connect.py run [--batch N]     # Process N unconnected capsules (default 20)
  keeper_connect.py backfill [--batch N] # Backfill from oldest unconnected
  keeper_connect.py landmarks           # Recompute and reconnect landmark capsules
  keeper_connect.py status              # Connection graph health
  keeper_connect.py sync                # Push local connections to Keeper canister
"""
import json
import math
import os
import random
import sqlite3
import struct
import sys
import time

import requests

DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")
OLLAMA_URL = "http://192.168.1.11:11434"
EMBED_MODEL = "mxbai-embed-large"
EMBED_DIM = 1024

NEIGHBOR_WINDOW = 500
MAX_CONNECTIONS_PER_CAPSULE = 15
LANDMARK_COUNT = 50
ERA_SIZE = 500
CROSS_ERA_SAMPLES = 2
CONNECTION_THRESHOLD = 0.50
QUALITY_PRUNE_FLOOR = 0.15

BELL_CENTER = 0.55
BELL_WIDTH = 0.20


def _db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""CREATE TABLE IF NOT EXISTS capsule_embeddings (
        capsule_id INTEGER PRIMARY KEY,
        embedding BLOB NOT NULL,
        model TEXT NOT NULL DEFAULT 'mxbai-embed-large',
        created_at INTEGER NOT NULL
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS capsule_graph (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        capsule_a INTEGER NOT NULL,
        capsule_b INTEGER NOT NULL,
        similarity REAL NOT NULL,
        quality REAL NOT NULL,
        relationship TEXT DEFAULT '',
        era_a INTEGER,
        era_b INTEGER,
        created_at INTEGER NOT NULL,
        UNIQUE(capsule_a, capsule_b)
    )""")
    conn.execute("""CREATE INDEX IF NOT EXISTS idx_cg_a ON capsule_graph(capsule_a)""")
    conn.execute("""CREATE INDEX IF NOT EXISTS idx_cg_b ON capsule_graph(capsule_b)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS capsule_landmarks (
        capsule_id INTEGER PRIMARY KEY,
        connection_count INTEGER NOT NULL,
        avg_quality REAL,
        era INTEGER,
        updated_at INTEGER NOT NULL
    )""")
    conn.commit()
    return conn


def embed_text(text):
    resp = requests.post(
        f"{OLLAMA_URL}/api/embed",
        json={"model": EMBED_MODEL, "input": text[:500]},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["embeddings"][0]


def pack_embedding(vec):
    return struct.pack(f"{len(vec)}f", *vec)


def unpack_embedding(blob):
    n = len(blob) // 4
    return struct.unpack(f"{n}f", blob)


def cosine_sim(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def get_capsule_era(capsule_id, era_boundaries):
    for i, (start, end) in enumerate(era_boundaries):
        if start <= capsule_id <= end:
            return i
    return len(era_boundaries)


def compute_era_boundaries(db):
    rows = db.execute(
        "SELECT id FROM knowledge_capsules ORDER BY id ASC"
    ).fetchall()
    ids = [r[0] for r in rows]
    if not ids:
        return []
    boundaries = []
    for i in range(0, len(ids), ERA_SIZE):
        chunk = ids[i:i + ERA_SIZE]
        boundaries.append((chunk[0], chunk[-1]))
    return boundaries


def compute_quality(topic_a, topic_b, similarity, age_days, conf_a, conf_b):
    if topic_a and topic_b:
        if topic_a == topic_b:
            topic_diversity = 0.5
        else:
            fam_a = topic_a.split("/")[0]
            fam_b = topic_b.split("/")[0]
            topic_diversity = 1.0 if fam_a == fam_b else 1.5
    else:
        topic_diversity = 1.0

    z = (similarity - BELL_CENTER) / BELL_WIDTH
    similarity_bell = math.exp(-z * z)

    recency = 0.3 + 0.7 * math.exp(-age_days / 30.0)

    confidence = 0.5 + (conf_a + conf_b) / 2.0

    is_foundation = False
    for t in (topic_a, topic_b):
        if t and (t.startswith("foundation") or t.startswith("homeforge")):
            is_foundation = True
            break
    foundation_boost = 2.0 if is_foundation else 1.0

    return topic_diversity * similarity_bell * recency * confidence * foundation_boost


def get_or_embed(db, capsule_id, text):
    row = db.execute(
        "SELECT embedding FROM capsule_embeddings WHERE capsule_id=?",
        (capsule_id,),
    ).fetchone()
    if row:
        return unpack_embedding(row[0])

    vec = embed_text(text)
    blob = pack_embedding(vec)
    db.execute(
        "INSERT OR REPLACE INTO capsule_embeddings (capsule_id, embedding, model, created_at) "
        "VALUES (?, ?, ?, ?)",
        (capsule_id, blob, EMBED_MODEL, int(time.time())),
    )
    db.commit()
    return vec


def get_unconnected_capsules(db, limit=20, oldest_first=False):
    order = "ASC" if oldest_first else "DESC"
    rows = db.execute(f"""
        SELECT kc.id, kc.restatement, kc.topic, kc.confidence_score, kc.created_at
        FROM knowledge_capsules kc
        LEFT JOIN (
            SELECT cid, SUM(cnt) as total FROM (
                SELECT capsule_a as cid, COUNT(*) as cnt FROM capsule_graph GROUP BY capsule_a
                UNION ALL
                SELECT capsule_b as cid, COUNT(*) as cnt FROM capsule_graph GROUP BY capsule_b
            ) GROUP BY cid
        ) cg ON kc.id = cg.cid
        WHERE cg.total IS NULL OR cg.total < 3
        ORDER BY kc.id {order}
        LIMIT ?
    """, (limit,)).fetchall()
    return [{
        "id": r[0], "text": r[1], "topic": r[2],
        "confidence": r[3], "created_at": r[4],
    } for r in rows]


def select_neighbors(db, capsule_id, created_at, era_boundaries, landmark_ids):
    neighbors = {}

    rows = db.execute("""
        SELECT id, restatement, topic, confidence_score, created_at
        FROM knowledge_capsules
        WHERE id != ?
        ORDER BY ABS(created_at - ?) ASC
        LIMIT ?
    """, (capsule_id, created_at, NEIGHBOR_WINDOW)).fetchall()
    for r in rows:
        neighbors[r[0]] = {
            "id": r[0], "text": r[1], "topic": r[2],
            "confidence": r[3], "created_at": r[4],
        }

    for lid in landmark_ids:
        if lid == capsule_id or lid in neighbors:
            continue
        row = db.execute(
            "SELECT id, restatement, topic, confidence_score, created_at "
            "FROM knowledge_capsules WHERE id=?", (lid,),
        ).fetchone()
        if row:
            neighbors[row[0]] = {
                "id": row[0], "text": row[1], "topic": row[2],
                "confidence": row[3], "created_at": row[4],
            }

    cap_era = get_capsule_era(capsule_id, era_boundaries)
    for i, (start, end) in enumerate(era_boundaries):
        if i == cap_era:
            continue
        era_caps = db.execute(
            "SELECT id, restatement, topic, confidence_score, created_at "
            "FROM knowledge_capsules WHERE id BETWEEN ? AND ? "
            "ORDER BY RANDOM() LIMIT ?",
            (start, end, CROSS_ERA_SAMPLES),
        ).fetchall()
        for r in era_caps:
            if r[0] != capsule_id and r[0] not in neighbors:
                neighbors[r[0]] = {
                    "id": r[0], "text": r[1], "topic": r[2],
                    "confidence": r[3], "created_at": r[4],
                }

    return list(neighbors.values())


def get_existing_connections(db, capsule_id):
    rows = db.execute("""
        SELECT capsule_a, capsule_b, quality FROM capsule_graph
        WHERE capsule_a = ? OR capsule_b = ?
        ORDER BY quality DESC
    """, (capsule_id, capsule_id)).fetchall()
    return [(r[0], r[1], r[2]) for r in rows]


def get_landmark_ids(db):
    rows = db.execute("""
        SELECT cid, SUM(cnt) as total FROM (
            SELECT capsule_a as cid, COUNT(*) as cnt FROM capsule_graph GROUP BY capsule_a
            UNION ALL
            SELECT capsule_b as cid, COUNT(*) as cnt FROM capsule_graph GROUP BY capsule_b
        ) GROUP BY cid ORDER BY total DESC LIMIT ?
    """, (LANDMARK_COUNT,)).fetchall()
    return [r[0] for r in rows]


def connect_capsule(db, capsule, neighbors, era_boundaries, now):
    cap_vec = get_or_embed(db, capsule["id"], capsule["text"])
    cap_era = get_capsule_era(capsule["id"], era_boundaries)
    now_ts = now

    candidates = []
    for nb in neighbors:
        nb_vec = get_or_embed(db, nb["id"], nb["text"])
        sim = cosine_sim(cap_vec, nb_vec)

        age_days = max(0, (now_ts - max(capsule["created_at"], nb["created_at"])) / 86400)
        nb_era = get_capsule_era(nb["id"], era_boundaries)

        quality = compute_quality(
            capsule.get("topic"), nb.get("topic"),
            sim, age_days,
            capsule.get("confidence", 0.8), nb.get("confidence", 0.8),
        )

        if quality >= QUALITY_PRUNE_FLOOR:
            candidates.append({
                "neighbor": nb,
                "similarity": sim,
                "quality": quality,
                "era_a": cap_era,
                "era_b": nb_era,
            })

    candidates.sort(key=lambda c: -c["quality"])

    existing = get_existing_connections(db, capsule["id"])
    existing_ids = set()
    for a, b, q in existing:
        other = b if a == capsule["id"] else a
        existing_ids.add(other)

    slots = MAX_CONNECTIONS_PER_CAPSULE - len(existing)
    new_connections = 0

    LANDMARK_CAP = MAX_CONNECTIONS_PER_CAPSULE * 4

    for cand in candidates:
        if slots <= 0:
            break
        nb_id = cand["neighbor"]["id"]
        if nb_id in existing_ids:
            continue

        nb_count = db.execute(
            "SELECT COUNT(*) FROM capsule_graph WHERE capsule_a=? OR capsule_b=?",
            (nb_id, nb_id),
        ).fetchone()[0]
        if nb_count >= LANDMARK_CAP:
            continue

        try:
            db.execute(
                "INSERT INTO capsule_graph "
                "(capsule_a, capsule_b, similarity, quality, relationship, era_a, era_b, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (capsule["id"], nb_id,
                 round(cand["similarity"], 4), round(cand["quality"], 4),
                 "", cand["era_a"], cand["era_b"], int(now_ts)),
            )
            new_connections += 1
            slots -= 1
            existing_ids.add(nb_id)
        except sqlite3.IntegrityError:
            existing_ids.add(nb_id)

    if new_connections > 0:
        all_conns = db.execute("""
            SELECT id, capsule_a, capsule_b, quality FROM capsule_graph
            WHERE capsule_a = ? OR capsule_b = ?
            ORDER BY quality ASC
        """, (capsule["id"], capsule["id"])).fetchall()

        if len(all_conns) > MAX_CONNECTIONS_PER_CAPSULE:
            to_prune = len(all_conns) - MAX_CONNECTIONS_PER_CAPSULE
            for row_id, _, _, _ in all_conns[:to_prune]:
                db.execute("DELETE FROM capsule_graph WHERE id=?", (row_id,))

    db.commit()
    return new_connections


def cmd_run(batch=20):
    db = _db()
    era_boundaries = compute_era_boundaries(db)
    landmark_ids = get_landmark_ids(db)
    now = time.time()

    capsules = get_unconnected_capsules(db, limit=batch)
    if not capsules:
        print("All capsules connected.")
        db.close()
        return 0

    print(f"Processing {len(capsules)} capsules (newest first)...")
    total_new = 0
    for i, cap in enumerate(capsules):
        neighbors = select_neighbors(db, cap["id"], cap["created_at"], era_boundaries, landmark_ids)
        n = connect_capsule(db, cap, neighbors, era_boundaries, now)
        total_new += n
        if (i + 1) % 5 == 0 or i == len(capsules) - 1:
            print(f"  [{i+1}/{len(capsules)}] capsule {cap['id']}: +{n} connections")

    print(f"\nDone. {total_new} new connections across {len(capsules)} capsules.")
    db.close()
    return 0


def cmd_backfill(batch=20):
    db = _db()
    era_boundaries = compute_era_boundaries(db)
    landmark_ids = get_landmark_ids(db)
    now = time.time()

    capsules = get_unconnected_capsules(db, limit=batch, oldest_first=True)
    if not capsules:
        print("All capsules connected.")
        db.close()
        return 0

    print(f"Backfilling {len(capsules)} capsules (oldest first)...")
    total_new = 0
    for i, cap in enumerate(capsules):
        neighbors = select_neighbors(db, cap["id"], cap["created_at"], era_boundaries, landmark_ids)
        n = connect_capsule(db, cap, neighbors, era_boundaries, now)
        total_new += n
        if (i + 1) % 5 == 0 or i == len(capsules) - 1:
            print(f"  [{i+1}/{len(capsules)}] capsule {cap['id']}: +{n} connections")

    print(f"\nDone. {total_new} new connections across {len(capsules)} capsules.")
    db.close()
    return 0


def cmd_landmarks():
    db = _db()
    era_boundaries = compute_era_boundaries(db)
    now = time.time()

    rows = db.execute("""
        SELECT cid, SUM(cnt) as total FROM (
            SELECT capsule_a as cid, COUNT(*) as cnt FROM capsule_graph GROUP BY capsule_a
            UNION ALL
            SELECT capsule_b as cid, COUNT(*) as cnt FROM capsule_graph GROUP BY capsule_b
        ) GROUP BY cid ORDER BY total DESC LIMIT ?
    """, (LANDMARK_COUNT,)).fetchall()

    if not rows:
        print("No connections yet — run `keeper_connect.py run` first.")
        db.close()
        return 0

    db.execute("DELETE FROM capsule_landmarks")

    landmark_ids = [r[0] for r in rows]
    print(f"Updating {len(landmark_ids)} landmark capsules...")

    for cid, count in rows:
        avg_q = db.execute(
            "SELECT AVG(quality) FROM capsule_graph WHERE capsule_a=? OR capsule_b=?",
            (cid, cid),
        ).fetchone()[0] or 0
        era = get_capsule_era(cid, era_boundaries)
        db.execute(
            "INSERT OR REPLACE INTO capsule_landmarks VALUES (?, ?, ?, ?, ?)",
            (cid, count, round(avg_q, 4), era, int(now)),
        )

    db.commit()

    for cid, count in rows[:10]:
        cap = db.execute(
            "SELECT restatement, topic FROM knowledge_capsules WHERE id=?", (cid,),
        ).fetchone()
        label = (cap[0] or "")[:60] if cap else "?"
        topic = cap[1] or "?" if cap else "?"
        print(f"  #{cid} ({count} conn, {topic}): {label}")

    if len(rows) > 10:
        print(f"  ... and {len(rows) - 10} more")

    cross_era_reconnects = 0
    for cid in landmark_ids:
        cap = db.execute(
            "SELECT id, restatement, topic, confidence_score, created_at "
            "FROM knowledge_capsules WHERE id=?", (cid,),
        ).fetchone()
        if not cap:
            continue
        capsule = {
            "id": cap[0], "text": cap[1], "topic": cap[2],
            "confidence": cap[3], "created_at": cap[4],
        }

        existing_eras = set()
        conns = db.execute(
            "SELECT capsule_a, capsule_b, era_a, era_b FROM capsule_graph "
            "WHERE capsule_a=? OR capsule_b=?",
            (cid, cid),
        ).fetchall()
        for a, b, ea, eb in conns:
            if ea is not None:
                existing_eras.add(ea)
            if eb is not None:
                existing_eras.add(eb)

        cap_era = get_capsule_era(cid, era_boundaries)
        missing_eras = []
        for i, (start, end) in enumerate(era_boundaries):
            if i not in existing_eras and i != cap_era:
                missing_eras.append(i)

        if not missing_eras:
            continue

        for era_idx in missing_eras[:3]:
            start, end = era_boundaries[era_idx]
            samples = db.execute(
                "SELECT id, restatement, topic, confidence_score, created_at "
                "FROM knowledge_capsules WHERE id BETWEEN ? AND ? "
                "ORDER BY RANDOM() LIMIT 5",
                (start, end),
            ).fetchall()
            for r in samples:
                nb = {"id": r[0], "text": r[1], "topic": r[2], "confidence": r[3], "created_at": r[4]}
                cap_vec = get_or_embed(db, cid, capsule["text"])
                nb_vec = get_or_embed(db, nb["id"], nb["text"])
                sim = cosine_sim(cap_vec, nb_vec)
                age_days = max(0, (now - max(capsule["created_at"], nb["created_at"])) / 86400)
                quality = compute_quality(
                    capsule.get("topic"), nb.get("topic"),
                    sim, age_days,
                    capsule.get("confidence", 0.8), nb.get("confidence", 0.8),
                )
                if quality >= QUALITY_PRUNE_FLOOR:
                    try:
                        db.execute(
                            "INSERT INTO capsule_graph "
                            "(capsule_a, capsule_b, similarity, quality, relationship, era_a, era_b, created_at) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                            (cid, nb["id"], round(sim, 4), round(quality, 4),
                             "landmark-bridge", cap_era, era_idx, int(now)),
                        )
                        cross_era_reconnects += 1
                    except sqlite3.IntegrityError:
                        pass

    db.commit()
    print(f"\nLandmark reconnection: {cross_era_reconnects} new cross-era bridges.")
    db.close()
    return 0


def cmd_status():
    db = _db()
    era_boundaries = compute_era_boundaries(db)

    total_capsules = db.execute("SELECT COUNT(*) FROM knowledge_capsules").fetchone()[0]
    total_connections = db.execute("SELECT COUNT(*) FROM capsule_graph").fetchone()[0]
    total_embeddings = db.execute("SELECT COUNT(*) FROM capsule_embeddings").fetchone()[0]

    connected = db.execute("""
        SELECT COUNT(DISTINCT cid) FROM (
            SELECT capsule_a as cid FROM capsule_graph
            UNION
            SELECT capsule_b as cid FROM capsule_graph
        )
    """).fetchone()[0]

    avg_quality = db.execute("SELECT AVG(quality) FROM capsule_graph").fetchone()[0] or 0
    avg_sim = db.execute("SELECT AVG(similarity) FROM capsule_graph").fetchone()[0] or 0

    conn_dist = db.execute("""
        SELECT cnt, COUNT(*) FROM (
            SELECT cid, SUM(c) as cnt FROM (
                SELECT capsule_a as cid, COUNT(*) as c FROM capsule_graph GROUP BY capsule_a
                UNION ALL
                SELECT capsule_b as cid, COUNT(*) as c FROM capsule_graph GROUP BY capsule_b
            ) GROUP BY cid
        ) GROUP BY cnt ORDER BY cnt
    """).fetchall()

    era_coverage = {}
    for i, (start, end) in enumerate(era_boundaries):
        era_conns = db.execute(
            "SELECT COUNT(*) FROM capsule_graph WHERE era_a=? OR era_b=?",
            (i, i),
        ).fetchone()[0]
        era_caps = db.execute(
            "SELECT COUNT(*) FROM knowledge_capsules WHERE id BETWEEN ? AND ?",
            (start, end),
        ).fetchone()[0]
        era_coverage[i] = {"connections": era_conns, "capsules": era_caps}

    cross_era = db.execute(
        "SELECT COUNT(*) FROM capsule_graph WHERE era_a IS NOT NULL AND era_b IS NOT NULL AND era_a != era_b"
    ).fetchone()[0]

    landmark_count = db.execute("SELECT COUNT(*) FROM capsule_landmarks").fetchone()[0]

    print(f"Keeper Connection Graph Status")
    print(f"{'='*50}")
    print(f"Capsules:     {total_capsules:>8}")
    print(f"Connected:    {connected:>8} ({100*connected/total_capsules:.1f}%)" if total_capsules else "")
    print(f"Connections:  {total_connections:>8}")
    print(f"Embeddings:   {total_embeddings:>8} cached")
    print(f"Avg quality:  {avg_quality:>8.3f}")
    print(f"Avg sim:      {avg_sim:>8.3f}")
    print(f"Cross-era:    {cross_era:>8}")
    print(f"Landmarks:    {landmark_count:>8}")
    print(f"Eras:         {len(era_boundaries):>8} (each ~{ERA_SIZE} capsules)")

    if conn_dist:
        print(f"\nConnections per capsule:")
        for cnt, n in conn_dist:
            bar = "#" * min(n // 2, 40)
            print(f"  {cnt:>3} connections: {n:>6} capsules {bar}")

    eras_with_conns = sum(1 for e in era_coverage.values() if e["connections"] > 0)
    print(f"\nEra coverage: {eras_with_conns}/{len(era_boundaries)} eras have connections")
    for i, info in sorted(era_coverage.items()):
        if info["connections"] > 0 or i < 3 or i >= len(era_boundaries) - 3:
            print(f"  Era {i:>3}: {info['capsules']:>5} capsules, {info['connections']:>6} connections")

    db.close()
    return 0


def cmd_sync():
    # Two paths for canister sync:
    # 1. Map to CausalEdge format + import_causal_edges (no canister change needed)
    #    capsule_a_id->source_id, capsule_b_id->target_id, quality_score->strength,
    #    "keeper_connection"->edge_type
    # 2. Add import_keeper_connections to Rust canister (cleaner, needs redeploy)
    #    KeeperConnection: {capsule_a, capsule_b, relationship, similarity, discovered_at}
    print("Sync to Keeper canister — not yet implemented.")
    print("(Connections stored locally in capsule_graph table for now.)")
    print(f"  Local connections ready: {_count_local_connections()} connections")
    return 0


def _count_local_connections():
    db = sqlite3.connect(DB_PATH)
    count = db.execute("SELECT COUNT(*) FROM capsule_graph").fetchone()[0]
    db.close()
    return count


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(2)

    cmd = sys.argv[1]
    batch = 20
    for i, arg in enumerate(sys.argv):
        if arg == "--batch" and i + 1 < len(sys.argv):
            batch = int(sys.argv[i + 1])

    if cmd == "run":
        sys.exit(cmd_run(batch))
    elif cmd == "backfill":
        sys.exit(cmd_backfill(batch))
    elif cmd == "landmarks":
        sys.exit(cmd_landmarks())
    elif cmd == "status":
        sys.exit(cmd_status())
    elif cmd == "sync":
        sys.exit(cmd_sync())
    else:
        print(__doc__)
        sys.exit(2)


if __name__ == "__main__":
    main()
