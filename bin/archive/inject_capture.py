#!/usr/bin/env python3
"""Inject content into the Chronicle pipeline as an operator capture.

Usage:
    python3 inject_capture.py "https://x.com/someone/status/123"
    python3 inject_capture.py "Some interesting observation or question"
    python3 inject_capture.py --requeue-failed [--hours 3]

Injected captures get the full treatment: intern pickup, web fetch,
related capsule search, Darby deep dive, Ada analysis.
"""

import argparse
import json
import sqlite3
import sys
import time

DB_PATH = "/mnt/hdd/chronicle-data/processed.db"


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def inject(content, source="operator:capture"):
    """Inject a single capture into the pipeline. Indistinguishable from Nate's."""
    db = get_db()
    now = int(time.time())
    db.execute(
        "INSERT INTO activity_feed (source, activity_type, title, content, metadata, created_at) "
        "VALUES (?, 'capture', '', ?, NULL, ?)",
        (source, content, now),
    )
    db.commit()
    cap_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
    db.close()
    return cap_id


def requeue_failed(hours=3):
    """Find captures that never got briefs and requeue them."""
    db = get_db()
    since = int(time.time()) - (hours * 3600)

    unbriefed = db.execute("""
        SELECT id, content, created_at FROM activity_feed
        WHERE source = 'operator:capture'
        AND created_at > ?
        AND id NOT IN (
            SELECT CAST(json_extract(metadata, '$.input_id') AS INTEGER)
            FROM activity_feed
            WHERE activity_type = 'brief'
            AND json_extract(metadata, '$.input_id') IS NOT NULL
            AND created_at > ?
        )
        ORDER BY created_at DESC
    """, (since, since)).fetchall()

    if not unbriefed:
        print("All captures have briefs. Nothing to requeue.")
        db.close()
        return 0

    now = int(time.time())
    count = 0
    for row in unbriefed:
        db.execute(
            "INSERT INTO activity_feed (source, activity_type, title, content, metadata, created_at) "
            "VALUES ('operator:capture', 'capture', '', ?, ?, ?)",
            (row["content"],
             json.dumps({"requeued_from": row["id"], "injected_by": "opus"}),
             now + count),
        )
        count += 1

    db.commit()
    db.close()
    print(f"Requeued {count} captures from last {hours}h")
    return count


def list_unbriefed(hours=3):
    """Show captures missing briefs."""
    db = get_db()
    since = int(time.time()) - (hours * 3600)

    unbriefed = db.execute("""
        SELECT id, substr(content, 1, 120), datetime(created_at, 'unixepoch', 'localtime') as ts
        FROM activity_feed
        WHERE source = 'operator:capture'
        AND created_at > ?
        AND id NOT IN (
            SELECT CAST(json_extract(metadata, '$.input_id') AS INTEGER)
            FROM activity_feed
            WHERE activity_type = 'brief'
            AND json_extract(metadata, '$.input_id') IS NOT NULL
            AND created_at > ?
        )
        ORDER BY created_at DESC
    """, (since, since)).fetchall()

    db.close()
    if not unbriefed:
        print("All captures have briefs.")
        return
    print(f"{len(unbriefed)} unbriefed captures:")
    for row in unbriefed:
        print(f"  #{row[0]} [{row[2]}] {row[1]}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inject captures into Chronicle pipeline")
    parser.add_argument("content", nargs="?", help="URL or text to inject as a capture")
    parser.add_argument("--requeue-failed", action="store_true", help="Requeue captures that failed briefing")
    parser.add_argument("--list-unbriefed", action="store_true", help="Show captures without briefs")
    parser.add_argument("--hours", type=int, default=3, help="Lookback window for requeue/list (default: 3)")
    parser.add_argument("--source", default="operator:capture", help="Source tag (default: operator:capture)")
    args = parser.parse_args()

    if args.requeue_failed:
        requeue_failed(args.hours)
    elif args.list_unbriefed:
        list_unbriefed(args.hours)
    elif args.content:
        cap_id = inject(args.content, source=args.source)
        print(f"Injected capture #{cap_id}: {args.content[:80]}")
    else:
        parser.print_help()
