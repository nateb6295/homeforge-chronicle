#!/usr/bin/env python3
"""sediment_audit.py — Detect dead tables and ghost data in processed.db.

The senolytic for Chronicle's database. Identifies tables that stopped
receiving writes and reports their storage cost.

Usage:
  python3 sediment_audit.py              # full audit
  python3 sediment_audit.py --brief      # one-line summary
  python3 sediment_audit.py --sources    # activity_feed source breakdown
"""
import os
import sqlite3
import subprocess
import sys
import time

DB = "/mnt/hdd/chronicle-data/processed.db"
STALE_DAYS = 14


def get_table_sizes(db):
    try:
        rows = db.execute(
            "SELECT name, SUM(pgsize) as total "
            "FROM dbstat WHERE name != 'sqlite_master' "
            "GROUP BY name ORDER BY total DESC"
        ).fetchall()
        return {name: size for name, size in rows}
    except Exception:
        return {}


def get_table_freshness(db):
    results = {}
    tables_with_ts = []

    all_tables = [r[0] for r in db.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]

    for table in all_tables:
        cols = [r[1] for r in db.execute(f"PRAGMA table_info({table})").fetchall()]

        ts_col = None
        for candidate in ["created_at", "timestamp", "updated_at", "ts", "posted_at"]:
            if candidate in cols:
                ts_col = candidate
                break

        if not ts_col:
            results[table] = {"ts_col": None, "latest": None, "count": 0}
            continue

        try:
            row = db.execute(f"SELECT MAX({ts_col}), COUNT(*) FROM {table}").fetchone()
        except Exception:
            results[table] = {"ts_col": ts_col, "latest": None, "count": 0}
            continue

        latest_raw = row[0]
        count = row[1] or 0

        if latest_raw is None:
            results[table] = {"ts_col": ts_col, "latest": None, "count": count}
            continue

        if isinstance(latest_raw, (int, float)) and latest_raw > 1_000_000_000:
            latest = latest_raw
        elif isinstance(latest_raw, str):
            try:
                from datetime import datetime
                dt = datetime.fromisoformat(latest_raw.replace("Z", "+00:00"))
                latest = dt.timestamp()
            except Exception:
                latest = None
        else:
            latest = None

        results[table] = {"ts_col": ts_col, "latest": latest, "count": count}

    return results


def get_dead_sources(db, days=STALE_DAYS):
    cutoff = time.time() - (days * 86400)
    rows = db.execute(
        "SELECT source, COUNT(*) as cnt, MAX(created_at) as latest "
        "FROM activity_feed GROUP BY source"
    ).fetchall()

    dead = []
    live = []
    for source, count, latest in rows:
        if latest and latest < cutoff:
            dead.append((source, count, latest))
        else:
            live.append((source, count, latest))

    return dead, live


def fmt_size(nbytes):
    if nbytes is None:
        return "?"
    if nbytes > 1_073_741_824:
        return f"{nbytes / 1_073_741_824:.1f} GB"
    if nbytes > 1_048_576:
        return f"{nbytes / 1_048_576:.1f} MB"
    if nbytes > 1024:
        return f"{nbytes / 1024:.1f} KB"
    return f"{nbytes} B"


def fmt_age(ts):
    if ts is None:
        return "never"
    days = (time.time() - ts) / 86400
    if days < 1:
        hours = (time.time() - ts) / 3600
        return f"{hours:.0f}h ago"
    return f"{days:.0f}d ago"


def run_audit(brief=False, sources_only=False):
    db = sqlite3.connect(DB)

    if sources_only:
        dead, live = get_dead_sources(db)
        dead_count = sum(c for _, c, _ in dead)
        live_count = sum(c for _, c, _ in live)
        total = dead_count + live_count

        print(f"Activity feed sources ({total} total entries):\n")
        print(f"{'Source':<25} {'Rows':>8} {'Last Write':>12} {'Status':>8}")
        print("-" * 58)
        for source, count, latest in sorted(dead + live, key=lambda x: -(x[1])):
            status = "DEAD" if (source, count, latest) in dead else "live"
            print(f"{source:<25} {count:>8} {fmt_age(latest):>12} {status:>8}")

        print(f"\nSediment: {dead_count}/{total} entries ({100*dead_count/total:.0f}%) from {len(dead)} dead sources")
        db.close()
        return

    sizes = get_table_sizes(db)
    freshness = get_table_freshness(db)

    now = time.time()
    cutoff = now - (STALE_DAYS * 86400)

    dead_tables = []
    live_tables = []
    no_ts_tables = []

    for table, info in freshness.items():
        size = sizes.get(table, 0)
        if info["ts_col"] is None:
            no_ts_tables.append((table, size, info))
        elif info["latest"] is None or info["count"] == 0:
            dead_tables.append((table, size, info))
        elif info["latest"] < cutoff:
            dead_tables.append((table, size, info))
        else:
            live_tables.append((table, size, info))

    dead_tables.sort(key=lambda x: -x[1])
    live_tables.sort(key=lambda x: -x[1])

    total_size = sum(sizes.values())
    dead_size = sum(s for _, s, _ in dead_tables)
    dead_size += sum(sizes.get(f"idx_{t}_" + col, 0) for t, _, info in dead_tables for col in [info.get("ts_col", "")] if col)

    if brief:
        pct = 100 * dead_size / total_size if total_size else 0
        print(f"Sediment: {fmt_size(dead_size)}/{fmt_size(total_size)} ({pct:.0f}%) across {len(dead_tables)} dead tables")
        db.close()
        return

    print(f"Sediment Audit — processed.db ({fmt_size(total_size)})")
    print(f"Stale threshold: {STALE_DAYS} days\n")

    if dead_tables:
        print(f"DEAD TABLES ({len(dead_tables)}):")
        print(f"{'Table':<35} {'Size':>10} {'Rows':>8} {'Last Write':>12}")
        print("-" * 70)
        for table, size, info in dead_tables:
            print(f"{table:<35} {fmt_size(size):>10} {info['count']:>8} {fmt_age(info['latest']):>12}")
        print(f"\nTotal dead storage: {fmt_size(dead_size)}")

    print(f"\nLIVE TABLES ({len(live_tables)}):")
    print(f"{'Table':<35} {'Size':>10} {'Rows':>8} {'Last Write':>12}")
    print("-" * 70)
    for table, size, info in live_tables[:15]:
        print(f"{table:<35} {fmt_size(size):>10} {info['count']:>8} {fmt_age(info['latest']):>12}")
    if len(live_tables) > 15:
        print(f"  ... and {len(live_tables) - 15} more")

    if no_ts_tables:
        print(f"\nNO TIMESTAMP ({len(no_ts_tables)} tables, cannot determine freshness)")

    dead, live = get_dead_sources(db)
    dead_count = sum(c for _, c, _ in dead)
    live_count = sum(c for _, c, _ in live)
    total_entries = dead_count + live_count
    if total_entries:
        print(f"\nActivity feed: {dead_count}/{total_entries} entries ({100*dead_count/total_entries:.0f}%) from dead sources")

    if total_size:
        print(f"\nOverall: {fmt_size(dead_size)} sediment / {fmt_size(total_size)} total ({100*dead_size/total_size:.0f}%)")

    db.close()


if __name__ == "__main__":
    args = sys.argv[1:]
    run_audit(
        brief="--brief" in args,
        sources_only="--sources" in args,
    )
