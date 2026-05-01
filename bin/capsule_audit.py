#!/usr/bin/env python3
"""Capsule Audit — visibility into memory selection health.

The Selection Problem: 80% of capsules are never accessed. This tool
shows what's being used, what's dormant, and where selection is failing.

Usage:
    capsule_audit.py summary       # overview of access distribution
    capsule_audit.py hot           # most accessed capsules
    capsule_audit.py cold          # never-accessed by topic
    capsule_audit.py recent        # recently accessed
    capsule_audit.py growth        # capsule growth rate vs access rate

Build from the 80% finding. Selection > accumulation.
"""

import os
import sys
import sqlite3
from datetime import datetime, timezone, timedelta

DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")
PDT = timezone(timedelta(hours=-7))


def get_db():
    return sqlite3.connect(DB_PATH, timeout=10)


def cmd_summary():
    """Overview of capsule access health."""
    db = get_db()

    total = db.execute("SELECT COUNT(*) FROM knowledge_capsules").fetchone()[0]
    live = db.execute(
        "SELECT COUNT(*) FROM knowledge_capsules "
        "WHERE superseded_at IS NULL AND metabolized_at IS NULL"
    ).fetchone()[0]
    superseded = db.execute(
        "SELECT COUNT(*) FROM knowledge_capsules WHERE superseded_at IS NOT NULL"
    ).fetchone()[0]
    metabolized = db.execute(
        "SELECT COUNT(*) FROM knowledge_capsules WHERE metabolized_at IS NOT NULL"
    ).fetchone()[0]

    # Access distribution for live capsules
    access_data = db.execute("""
        WITH capsule_access AS (
            SELECT item_id, COUNT(*) as acc,
                   MAX(accessed_at) as last_access
            FROM memory_access_log
            WHERE item_type = 'capsule'
            GROUP BY item_id
        )
        SELECT
            SUM(CASE WHEN ca.acc IS NULL THEN 1 ELSE 0 END) as never,
            SUM(CASE WHEN ca.acc = 1 THEN 1 ELSE 0 END) as once,
            SUM(CASE WHEN ca.acc BETWEEN 2 AND 5 THEN 1 ELSE 0 END) as low,
            SUM(CASE WHEN ca.acc BETWEEN 6 AND 20 THEN 1 ELSE 0 END) as med,
            SUM(CASE WHEN ca.acc > 20 THEN 1 ELSE 0 END) as high
        FROM knowledge_capsules kc
        LEFT JOIN capsule_access ca ON kc.id = ca.item_id
        WHERE kc.superseded_at IS NULL AND kc.metabolized_at IS NULL
    """).fetchone()

    never, once, low, med, high = access_data

    # Growth rate (last 7 days)
    recent_new = db.execute(
        "SELECT COUNT(*) FROM knowledge_capsules "
        "WHERE created_at > strftime('%s','now') - 604800"
    ).fetchone()[0]
    recent_access = db.execute(
        "SELECT COUNT(DISTINCT item_id) FROM memory_access_log "
        "WHERE item_type='capsule' AND accessed_at > strftime('%s','now') - 604800"
    ).fetchone()[0]

    db.close()

    print("═══ Capsule Health Audit ═══")
    print(f"\nTotal: {total:,}  |  Live: {live:,}  |  Superseded: {superseded:,}  |  Metabolized: {metabolized:,}")
    print(f"\n  Access Distribution (live capsules):")
    print(f"    Never accessed:  {never:>6,}  ({100*never/live:.1f}%)  ████████████████")
    print(f"    Accessed once:   {once:>6,}  ({100*once/live:.1f}%)")
    print(f"    2-5 times:       {low:>6,}  ({100*low/live:.1f}%)")
    print(f"    6-20 times:      {med:>6,}  ({100*med/live:.1f}%)")
    print(f"    20+ times:       {high:>6,}  ({100*high/live:.1f}%)  ← doing the work")
    print(f"\n  Selection Ratio: {100*(1 - never/live):.1f}% of live capsules ever accessed")
    print(f"  7-day growth: +{recent_new:,} new  |  {recent_access:,} unique accessed")
    if recent_new > 0:
        print(f"  Selection pressure: {recent_access/recent_new:.2f}x (accessed/created)")


def cmd_hot():
    """Most accessed capsules."""
    db = get_db()
    rows = db.execute("""
        SELECT kc.id, kc.topic, substr(kc.restatement, 1, 100),
               COUNT(mal.id) as acc, MAX(mal.accessed_at) as last
        FROM knowledge_capsules kc
        JOIN memory_access_log mal ON kc.id = mal.item_id AND mal.item_type = 'capsule'
        WHERE kc.superseded_at IS NULL AND kc.metabolized_at IS NULL
        GROUP BY kc.id
        ORDER BY acc DESC
        LIMIT 20
    """).fetchall()
    db.close()

    print("═══ Hottest Capsules (most accessed) ═══\n")
    for cid, topic, text, acc, last_ts in rows:
        last_dt = datetime.fromtimestamp(last_ts, PDT).strftime("%m/%d") if last_ts else "?"
        print(f"  #{cid} [{topic or '?'}] ({acc}x, last {last_dt})")
        print(f"    {text}")
        print()


def cmd_cold():
    """Never-accessed capsules by topic."""
    db = get_db()
    rows = db.execute("""
        SELECT kc.topic, COUNT(*) as cnt
        FROM knowledge_capsules kc
        LEFT JOIN memory_access_log mal
            ON kc.id = mal.item_id AND mal.item_type = 'capsule'
        WHERE kc.superseded_at IS NULL
          AND kc.metabolized_at IS NULL
          AND mal.id IS NULL
        GROUP BY kc.topic
        ORDER BY cnt DESC
        LIMIT 20
    """).fetchall()
    db.close()

    print("═══ Cold Storage (never-accessed by topic) ═══\n")
    for topic, cnt in rows:
        bar = "█" * min(cnt // 100, 30)
        print(f"  {topic or '(none)':<30} {cnt:>5}  {bar}")


def cmd_recent():
    """Recently accessed capsules."""
    db = get_db()
    rows = db.execute("""
        SELECT kc.id, kc.topic, substr(kc.restatement, 1, 100),
               mal.agent, mal.accessed_at
        FROM memory_access_log mal
        JOIN knowledge_capsules kc ON kc.id = mal.item_id
        WHERE mal.item_type = 'capsule'
        ORDER BY mal.accessed_at DESC
        LIMIT 20
    """).fetchall()
    db.close()

    print("═══ Recently Accessed ═══\n")
    for cid, topic, text, agent, ts in rows:
        dt = datetime.fromtimestamp(ts, PDT).strftime("%m/%d %H:%M")
        print(f"  {dt} [{agent}] #{cid} ({topic or '?'})")
        print(f"    {text}")
        print()


def cmd_growth():
    """Capsule growth vs access over time."""
    db = get_db()

    # Weekly buckets for last 4 weeks
    print("═══ Growth vs Selection (weekly) ═══\n")
    print(f"  {'Week':<12} {'Created':>8} {'Accessed':>9} {'Ratio':>7}")
    print(f"  {'─'*12} {'─'*8} {'─'*9} {'─'*7}")

    for weeks_ago in range(3, -1, -1):
        start = f"strftime('%s','now') - {(weeks_ago+1)*604800}"
        end = f"strftime('%s','now') - {weeks_ago*604800}"

        created = db.execute(
            f"SELECT COUNT(*) FROM knowledge_capsules WHERE created_at > {start} AND created_at <= {end}"
        ).fetchone()[0]
        accessed = db.execute(
            f"SELECT COUNT(DISTINCT item_id) FROM memory_access_log "
            f"WHERE item_type='capsule' AND accessed_at > {start} AND accessed_at <= {end}"
        ).fetchone()[0]

        ratio = f"{accessed/created:.2f}x" if created > 0 else "N/A"
        label = f"{weeks_ago}w ago" if weeks_ago > 0 else "this week"
        print(f"  {label:<12} {created:>8,} {accessed:>9,} {ratio:>7}")

    db.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  capsule_audit.py summary  # access distribution")
        print("  capsule_audit.py hot      # most accessed capsules")
        print("  capsule_audit.py cold     # never-accessed by topic")
        print("  capsule_audit.py recent   # recently accessed")
        print("  capsule_audit.py growth   # growth vs selection rate")
        sys.exit(0)

    cmd = sys.argv[1]
    cmds = {"summary": cmd_summary, "hot": cmd_hot, "cold": cmd_cold,
            "recent": cmd_recent, "growth": cmd_growth}

    if cmd in cmds:
        cmds[cmd]()
    else:
        print(f"Unknown: {cmd}")
        sys.exit(1)
