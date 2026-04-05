"""KG Utilities — shared functions for knowledge graph access tracking and temporal management.

Used by crossref, opus-cycle, and any agent that reads KG relationships.
Implements:
  - Usage counters (Cognee pattern): track which relationships are actually used
  - Temporal validity (Graphiti pattern): relationships have valid_from/valid_until windows
"""

import time
from typing import Optional, List, Dict


def touch_relationship(db, relationship_id: int):
    """Record that a relationship was accessed/used by a downstream consumer.

    Bumps access_count and updates last_accessed timestamp.
    Called by crossref when it uses a relationship for connection discovery,
    by opus when it references a relationship in a thread finding, etc.
    """
    now = int(time.time())
    try:
        cur = db.conn.cursor()
        cur.execute(
            "UPDATE kg_relationships SET access_count = access_count + 1, last_accessed = ? WHERE id = ?",
            (now, relationship_id),
        )
        db.conn.commit()
    except Exception:
        pass


def touch_relationships_bulk(db, relationship_ids: List[int]):
    """Bulk version of touch_relationship for batch operations."""
    if not relationship_ids:
        return
    now = int(time.time())
    try:
        cur = db.conn.cursor()
        placeholders = ",".join("?" * len(relationship_ids))
        cur.execute(
            f"UPDATE kg_relationships SET access_count = access_count + 1, last_accessed = ? "
            f"WHERE id IN ({placeholders})",
            [now] + list(relationship_ids),
        )
        db.conn.commit()
    except Exception:
        pass


def supersede_relationship(db, old_id: int, new_id: int):
    """Mark a relationship as superseded by a newer one.

    Sets valid_until on the old relationship and links it to the new one.
    The old relationship is NOT deleted -- it becomes historical.

    Example: "XRP classified as security" superseded by "XRP classified as commodity"
    """
    now = int(time.time())
    try:
        cur = db.conn.cursor()
        cur.execute(
            "UPDATE kg_relationships SET valid_until = ?, superseded_by = ? WHERE id = ? AND valid_until IS NULL",
            (now, new_id, old_id),
        )
        db.conn.commit()
    except Exception:
        pass


def invalidate_relationship(db, relationship_id: int):
    """Mark a relationship as no longer valid without a replacement.

    Sets valid_until to now. Use when a fact becomes false
    but there is no replacement fact.
    """
    now = int(time.time())
    try:
        cur = db.conn.cursor()
        cur.execute(
            "UPDATE kg_relationships SET valid_until = ? WHERE id = ? AND valid_until IS NULL",
            (now, relationship_id),
        )
        db.conn.commit()
    except Exception:
        pass


def get_active_relationships(db, entity_id: int, as_of: Optional[int] = None) -> List[Dict]:
    """Get all currently valid relationships for an entity.

    If as_of is provided, returns relationships valid at that point in time
    (historical query). Otherwise returns currently valid ones.
    """
    ts = as_of or int(time.time())
    try:
        cur = db.conn.cursor()
        cur.execute(
            "SELECT r.*, "
            "  se.canonical_name as source_name, te.canonical_name as target_name "
            "FROM kg_relationships r "
            "JOIN kg_entities se ON r.source_entity = se.id "
            "JOIN kg_entities te ON r.target_entity = te.id "
            "WHERE (r.source_entity = ? OR r.target_entity = ?) "
            "AND (r.valid_from IS NULL OR r.valid_from <= ?) "
            "AND (r.valid_until IS NULL OR r.valid_until > ?) "
            "ORDER BY r.access_count DESC, r.confidence DESC",
            (entity_id, entity_id, ts, ts),
        )
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception:
        return []


def get_most_accessed_relationships(db, limit: int = 20) -> List[Dict]:
    """Get the most frequently accessed relationships -- the load-bearing edges."""
    try:
        cur = db.conn.cursor()
        cur.execute(
            "SELECT r.*, "
            "  se.canonical_name as source_name, te.canonical_name as target_name "
            "FROM kg_relationships r "
            "JOIN kg_entities se ON r.source_entity = se.id "
            "JOIN kg_entities te ON r.target_entity = te.id "
            "WHERE r.valid_until IS NULL "
            "ORDER BY r.access_count DESC "
            "LIMIT ?",
            (limit,),
        )
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception:
        return []


def decay_unused_relationships(db, days_inactive: int = 30, decay_amount: float = 0.05):
    """Decay confidence on relationships that have not been accessed recently.

    Relationships that are written but never read are losing value.
    Run periodically (e.g., daily) to keep the graph honest.
    """
    cutoff = int(time.time()) - (days_inactive * 86400)
    try:
        cur = db.conn.cursor()
        cur.execute(
            "UPDATE kg_relationships SET confidence = MAX(0.1, confidence - ?) "
            "WHERE valid_until IS NULL "
            "AND (last_accessed < ? OR last_accessed = 0) "
            "AND access_count = 0",
            (decay_amount, cutoff),
        )
        affected = cur.rowcount
        db.conn.commit()
        return affected
    except Exception:
        return 0
