#!/usr/bin/env python3
"""Backfill KG relationships from existing co-occurring entity mentions.
Also cleans up contaminated entities.

Run on AGX: python3 kg_backfill.py
"""

import sqlite3, os, re, json, time, requests

DB_PATH = os.environ.get(
    "CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db"),
)
OLLAMA_URL = os.environ.get("CHRONICLE_OLLAMA_URL", "http://localhost:11434")
MODEL = os.environ.get("INTERN_MODEL", "chronicle-deep")  # Use 32B for quality

# System metadata entities to block
BLOCK_SUBSTRINGS = [
    "novelty=", "canister:capsule", "seed [think]", "seed [deep]",
    "chronicle memory metabolism", "homeforge:", "chronicle:",
    "agentic ai: seed", "npu-driven agx", "agx orin 64gb",
    "homeforge ecosystem", "chronicle's memory", "nate's evolving role",
    "chronicle: homeforge", "jetson 64gb ram",
    "chronicle: memory metabolism", "homeforge:arcanist",
]


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")


def cleanup_entities(db):
    """Block contaminated system metadata entities."""
    cur = db.execute("SELECT id, canonical_name, mention_count FROM kg_entities")
    blocked = 0
    for row in cur.fetchall():
        name_lower = row[1].lower()
        if any(sub in name_lower for sub in BLOCK_SUBSTRINGS):
            # Delete mentions for this entity
            db.execute("DELETE FROM kg_mentions WHERE entity_id = ?", (row[0],))
            # Delete entity
            db.execute("DELETE FROM kg_entities WHERE id = ?", (row[0],))
            blocked += 1
            if row[2] > 10:  # Only log significant ones
                log(f"  Blocked: [{row[2]}x] {row[1][:80]}")
    db.commit()
    log(f"Cleanup: blocked {blocked} contaminated entities")


def get_co_occurring_sources(db):
    """Find source_type+source_id pairs that have 2+ entity mentions."""
    rows = db.execute("""
        SELECT source_type, source_id, COUNT(DISTINCT entity_id) as entity_count
        FROM kg_mentions
        GROUP BY source_type, source_id
        HAVING entity_count >= 2
        ORDER BY source_id DESC
    """).fetchall()
    return [(r[0], r[1], r[2]) for r in rows]


def get_entities_for_source(db, source_type, source_id):
    """Get entities mentioned in a specific source."""
    return db.execute("""
        SELECT DISTINCT e.id, e.canonical_name, e.entity_type
        FROM kg_mentions m JOIN kg_entities e ON m.entity_id = e.id
        WHERE m.source_type = ? AND m.source_id = ?
    """, (source_type, source_id)).fetchall()


def get_context_for_source(db, source_type, source_id):
    """Get the context text for a source."""
    row = db.execute(
        "SELECT context FROM kg_mentions WHERE source_type = ? AND source_id = ? LIMIT 1",
        (source_type, source_id),
    ).fetchone()
    return row[0] if row else ""


def extract_relationships(db, entities, context, timestamp):
    """Use LLM to extract relationships between entities."""
    entity_names = [f"{e[1]} ({e[2]})" for e in entities[:8]]
    entity_list = ", ".join(entity_names)

    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/chat",
            json={
                "model": MODEL,
                "messages": [
                    {"role": "system", "content":
                        "Given a text and entities, identify relationships between pairs.\n"
                        "For each: source (exact name), target (exact name), "
                        "relation (short verb phrase), confidence (0.0-1.0).\n"
                        "Reply ONLY with a JSON array. Max 5 relationships.\n"
                        "Skip trivial relationships."},
                    {"role": "user", "content": f"Entities: {entity_list}\n\nText: {context[:600]}"},
                ],
                "stream": False,
                "options": {"num_predict": 512, "temperature": 0.3},
            },
            timeout=300,
        )
        if r.status_code != 200:
            return []

        raw = r.json().get("message", {}).get("content", "").strip()
        # Strip think tags from qwen3
        if '</think>' in raw:
            raw = raw.split('</think>')[-1].strip()
        raw = re.sub(r'^```json\s*', '', raw)
        raw = re.sub(r'\s*```$', '', raw)
        match = re.search(r'\[.*\]', raw, re.DOTALL)
        if not match:
            return []
        return json.loads(match.group())[:5]
    except Exception as e:
        log(f"  LLM error: {e}")
        return []


def store_relationships(db, rels, entities, context, timestamp):
    """Store extracted relationships in kg_relationships."""
    name_to_id = {e[1].lower(): e[0] for e in entities}
    stored = 0

    for rel in rels:
        src_name = rel.get("source", "").strip()
        tgt_name = rel.get("target", "").strip()
        relation = rel.get("relation", "").strip()
        confidence = min(1.0, max(0.0, float(rel.get("confidence", 0.5))))

        src_id = name_to_id.get(src_name.lower())
        tgt_id = name_to_id.get(tgt_name.lower())

        if not src_id or not tgt_id or not relation or src_id == tgt_id:
            continue

        # Upsert
        existing = db.execute(
            "SELECT id, mention_count FROM kg_relationships "
            "WHERE source_entity = ? AND target_entity = ? AND relation_type = ?",
            (src_id, tgt_id, relation),
        ).fetchone()

        if existing:
            db.execute(
                "UPDATE kg_relationships SET mention_count = mention_count + 1, "
                "confidence = MIN(1.0, confidence + 0.05), last_seen = ? WHERE id = ?",
                (timestamp, existing[0]),
            )
        else:
            db.execute(
                "INSERT INTO kg_relationships (source_entity, target_entity, relation_type, "
                "confidence, evidence, first_seen, last_seen, mention_count) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, 1)",
                (src_id, tgt_id, relation, confidence,
                 context[:200], timestamp, timestamp),
            )
        stored += 1

    db.commit()
    return stored


def main():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    log(f"KG Backfill starting — DB: {DB_PATH}")
    log(f"Model: {MODEL}")

    # Step 1: Cleanup
    cleanup_entities(db)

    # Step 2: Find co-occurring sources
    sources = get_co_occurring_sources(db)
    log(f"Found {len(sources)} sources with 2+ entities")

    # Step 3: Process each source
    total_stored = 0
    processed = 0
    for source_type, source_id, entity_count in sources:
        entities = get_entities_for_source(db, source_type, source_id)
        if len(entities) < 2:
            continue

        context = get_context_for_source(db, source_type, source_id)
        if not context:
            continue

        timestamp = int(time.time())
        rels = extract_relationships(db, entities, context, timestamp)
        if rels:
            stored = store_relationships(db, rels, entities, context, timestamp)
            total_stored += stored
            if stored:
                names = [e[1][:30] for e in entities[:4]]
                log(f"  [{processed+1}/{len(sources)}] {stored} rels from {', '.join(names)}")

        processed += 1
        if processed % 50 == 0:
            log(f"  Progress: {processed}/{len(sources)} sources, {total_stored} relationships")

    log(f"\n=== BACKFILL COMPLETE ===")
    log(f"Processed: {processed} sources")
    log(f"Relationships created: {total_stored}")

    # Report final stats
    ent_count = db.execute("SELECT COUNT(*) FROM kg_entities").fetchone()[0]
    rel_count = db.execute("SELECT COUNT(*) FROM kg_relationships").fetchone()[0]
    mention_count = db.execute("SELECT COUNT(*) FROM kg_mentions").fetchone()[0]
    log(f"KG: {ent_count} entities, {rel_count} relationships, {mention_count} mentions")

    db.close()


if __name__ == "__main__":
    main()
