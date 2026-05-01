#!/usr/bin/env python3
"""Retroactive backfill of seed_routing_log.feedback_score from existing data.

Joins seed routing decisions with intern activity to determine which
routing decisions led to successful research briefs.

Scoring:
  0.8  — intern produced a brief from this seed observation
  0.2  — intern picked it up but failed to synthesize
  0.0  — seed routed to think/deep but intern never picked it up

Run once on AGX: python3 scripts/backfill_feedback.py
"""

import os
import sqlite3

DB_PATH = os.environ.get(
    "CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db"),
)


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Count current state
    row = conn.execute(
        "SELECT COUNT(*) as total, "
        "SUM(CASE WHEN feedback_score IS NOT NULL THEN 1 ELSE 0 END) as filled "
        "FROM seed_routing_log"
    ).fetchone()
    total = row["total"]
    filled = row["filled"]
    print(f"seed_routing_log: {total} entries, {filled} already have feedback")

    # Strategy: For each seed routing entry that went to think/deep,
    # find matching activity_feed entries from seed, then check if
    # intern produced a brief within a reasonable time window.

    # Step 1: Find seed activity_feed entries and their timestamps
    # These are the entries the intern would pick up
    seed_activities = conn.execute(
        "SELECT id, created_at, activity_type FROM activity_feed "
        "WHERE source = 'seed' AND activity_type IN ('think', 'deep')"
    ).fetchall()
    print(f"Found {len(seed_activities)} seed activity_feed entries")

    # Step 2: Find intern brief entries and their input_ids
    intern_briefs = conn.execute(
        "SELECT id, created_at, metadata FROM activity_feed "
        "WHERE source = 'intern' AND activity_type = 'brief'"
    ).fetchall()
    print(f"Found {len(intern_briefs)} intern brief entries")

    # Build set of seed activity_feed IDs that led to briefs
    # The intern's brief metadata has input_id like "seed:12345"
    briefed_af_ids = set()
    for b in intern_briefs:
        if b["metadata"]:
            try:
                import json
                meta = json.loads(b["metadata"])
                input_id = meta.get("input_id", "")
                if input_id.startswith("seed:"):
                    af_id = int(input_id.split(":")[1])
                    briefed_af_ids.add(af_id)
            except (json.JSONDecodeError, ValueError, TypeError):
                pass
    print(f"Found {len(briefed_af_ids)} seed items that produced briefs")

    # Also check intern pickups that didn't produce briefs
    intern_pickups = conn.execute(
        "SELECT id, created_at, content FROM activity_feed "
        "WHERE source = 'intern' AND activity_type = 'pickup'"
    ).fetchall()

    # Step 3: Match seed routing log entries to activity_feed entries by timestamp
    # Then determine outcome
    updated = 0
    matched_brief = 0
    matched_no_brief = 0
    unmatched = 0

    routing_entries = conn.execute(
        "SELECT id, timestamp, route FROM seed_routing_log "
        "WHERE feedback_score IS NULL AND route IN ('think', 'deep')"
    ).fetchall()
    print(f"Processing {len(routing_entries)} unfilled think/deep routing entries...")

    for entry in routing_entries:
        ts = entry["timestamp"]
        # Find the closest seed activity_feed entry within 5 seconds
        match = conn.execute(
            "SELECT id FROM activity_feed "
            "WHERE source = 'seed' AND activity_type = ? "
            "AND created_at BETWEEN ? AND ? "
            "ORDER BY ABS(created_at - ?) LIMIT 1",
            (entry["route"], ts - 5, ts + 5, ts),
        ).fetchone()

        if match and match["id"] in briefed_af_ids:
            score = 0.8
            matched_brief += 1
        elif match:
            # Was in activity_feed but didn't produce a brief
            score = 0.2
            matched_no_brief += 1
        else:
            # No matching activity_feed entry found
            continue

        conn.execute(
            "UPDATE seed_routing_log SET feedback_score = ? WHERE id = ?",
            (score, entry["id"]),
        )
        updated += 1

    # Step 4: Mark ignore/glance entries with feedback_score = 0
    # These were correctly filtered — they shouldn't have been researched
    ignore_updated = conn.execute(
        "UPDATE seed_routing_log SET feedback_score = 0.0 "
        "WHERE feedback_score IS NULL AND route IN ('ignore', 'glance')"
    ).rowcount

    conn.commit()

    print(f"\nResults:")
    print(f"  Think/deep matched to brief: {matched_brief} (score=0.8)")
    print(f"  Think/deep matched, no brief: {matched_no_brief} (score=0.2)")
    print(f"  Think/deep unmatched: {len(routing_entries) - updated}")
    print(f"  Ignore/glance filled: {ignore_updated} (score=0.0)")
    print(f"  Total updated: {updated + ignore_updated}")

    # Final state
    row = conn.execute(
        "SELECT COUNT(*) as total, "
        "SUM(CASE WHEN feedback_score IS NOT NULL THEN 1 ELSE 0 END) as filled "
        "FROM seed_routing_log"
    ).fetchone()
    print(f"\nFinal: {row['filled']}/{row['total']} entries have feedback scores")

    conn.close()


if __name__ == "__main__":
    main()
