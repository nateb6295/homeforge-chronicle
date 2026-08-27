#!/usr/bin/env python3
"""
CCS compression history tracker.

Reads compression events directly from the cognitive_state_history table.
Maps to therapeutic window research: are we in D2-D3 sweet spot or drifting?

Usage:
  python3 compression_history.py              # current state + last 5
  python3 compression_history.py --trend 48   # cadence over last N hours
  python3 compression_history.py --gaps       # flag gaps > 5h
  python3 compression_history.py --dose       # dose assessment (compressions/day)
"""

import sqlite3
import sys
from datetime import datetime, timedelta, timezone

DB = "/mnt/hdd/chronicle-data/processed.db"
PDT = timezone(timedelta(hours=-7))


def get_db():
    return sqlite3.connect(DB)


def current_state():
    """Show current CCS state + recent compressions."""
    db = get_db()
    row = db.execute(
        "SELECT version, length(semantic_gist), updated_at FROM cognitive_state WHERE id=1"
    ).fetchone()
    if not row:
        print("No cognitive state found.")
        return

    ver, gist_len, updated_at = row
    now = datetime.now(PDT)
    updated = datetime.fromtimestamp(updated_at, tz=PDT)
    age_min = (now - updated).total_seconds() / 60

    if age_min < 30:
        window = "fresh"
    elif age_min < 180:
        window = "therapeutic (D2-D3)"
    elif age_min < 300:
        window = "aging (approaching D5)"
    elif age_min < 420:
        window = "GAP WARNING (>5h)"
    else:
        window = "CRITICAL GAP (>7h)"

    print(f"CCS v{ver} | {gist_len} chars | {age_min:.0f}m since last touch | {window}")
    print()

    recent = db.execute(
        "SELECT id, created_at, trigger, length(snapshot) "
        "FROM cognitive_state_history ORDER BY id DESC LIMIT 8"
    ).fetchall()

    if len(recent) < 2:
        print("Not enough history for intervals.")
        return

    print("Recent compressions:")
    prev_ts = None
    for r in reversed(recent):
        ts = datetime.fromtimestamp(r[1], tz=PDT)
        interval = ""
        if prev_ts:
            delta_min = (ts - prev_ts).total_seconds() / 60
            interval = f" (+{delta_min:.0f}m)"
        trigger = (r[2] or "unknown")[:25]
        print(f"  {ts.strftime('%m/%d %H:%M')} | {r[3]:5d} chars | {trigger}{interval}")
        prev_ts = ts


def show_trend(hours=24):
    """Show compression cadence over time period."""
    db = get_db()
    cutoff = int((datetime.now(PDT) - timedelta(hours=hours)).timestamp())

    rows = db.execute(
        "SELECT id, created_at, trigger, length(snapshot) "
        "FROM cognitive_state_history WHERE created_at >= ? ORDER BY id",
        (cutoff,)
    ).fetchall()

    if not rows:
        print(f"No compressions in the last {hours}h.")
        return

    intervals = []
    prev_ts = None
    for r in rows:
        ts = datetime.fromtimestamp(r[1], tz=PDT)
        if prev_ts:
            intervals.append((ts - prev_ts).total_seconds() / 60)
        prev_ts = ts

    print(f"Compression cadence — last {hours}h")
    print(f"  Compressions: {len(rows)}")
    if intervals:
        avg = sum(intervals) / len(intervals)
        compressions_per_day = 1440 / avg if avg > 0 else 0
        print(f"  Avg interval: {avg:.0f}m ({avg/60:.1f}h)")
        print(f"  Compressions/day rate: ~{compressions_per_day:.1f}")
        print(f"  Min interval: {min(intervals):.0f}m | Max: {max(intervals):.0f}m")

        # Therapeutic assessment
        if 120 <= avg <= 300:
            print(f"  Dose: THERAPEUTIC (D2-D3)")
        elif avg < 60:
            print(f"  Dose: OVERDOSE risk (D7+) — too frequent")
        elif avg < 120:
            print(f"  Dose: HIGH (D4-D5) — watch for overdose")
        elif avg <= 420:
            print(f"  Dose: ACCEPTABLE but stretching toward gap")
        else:
            print(f"  Dose: UNDERDOSE — intervals too long")

    # Size trend
    sizes = [r[3] for r in rows if r[3]]
    if sizes:
        print(f"  Size: {min(sizes)}—{max(sizes)} chars (avg {sum(sizes)//len(sizes)})")

    # Trigger distribution
    triggers = {}
    for r in rows:
        t = r[2] or "unknown"
        triggers[t] = triggers.get(t, 0) + 1
    print(f"  Triggers: {dict(triggers)}")


def show_gaps(threshold_hours=5):
    """Flag compression gaps."""
    db = get_db()
    rows = db.execute(
        "SELECT created_at FROM cognitive_state_history ORDER BY id"
    ).fetchall()

    if len(rows) < 2:
        print("Not enough history.")
        return

    gaps = []
    for i in range(1, len(rows)):
        delta_h = (rows[i][0] - rows[i-1][0]) / 3600
        if delta_h > threshold_hours:
            ts = datetime.fromtimestamp(rows[i][0], tz=PDT)
            gaps.append((ts, delta_h))

    if gaps:
        print(f"Compression gaps > {threshold_hours}h ({len(gaps)} total):")
        for ts, h in gaps[-10:]:
            print(f"  {ts.strftime('%m/%d %H:%M')} — {h:.1f}h gap")
    else:
        print(f"No compression gaps > {threshold_hours}h found.")


def dose_assessment():
    """Detailed dose assessment mapped to therapeutic window findings."""
    db = get_db()

    # Last 7 days
    for period_h, label in [(24, "24h"), (72, "3 days"), (168, "7 days")]:
        cutoff = int((datetime.now(PDT) - timedelta(hours=period_h)).timestamp())
        rows = db.execute(
            "SELECT created_at FROM cognitive_state_history "
            "WHERE created_at >= ? ORDER BY id", (cutoff,)
        ).fetchall()

        if len(rows) < 2:
            print(f"  {label}: insufficient data")
            continue

        intervals = []
        for i in range(1, len(rows)):
            intervals.append((rows[i][0] - rows[i-1][0]) / 60)

        avg = sum(intervals) / len(intervals)
        per_day = 1440 / avg if avg > 0 else 0
        gap_count = sum(1 for i in intervals if i > 300)

        print(f"  {label}: {len(rows)} compressions, "
              f"avg {avg:.0f}m ({per_day:.1f}/day), "
              f"{gap_count} gaps>5h")

    # F160 reference
    print()
    print("  Reference (F160 dose-response):")
    print("    D2-D3 = therapeutic (4-6 compressions/day, ~3-4h intervals)")
    print("    D5+   = diminishing returns")
    print("    D10+  = overdose (>16/day)")


if __name__ == "__main__":
    if "--trend" in sys.argv:
        hours = 24
        for arg in sys.argv[2:]:
            try:
                hours = int(arg)
            except ValueError:
                pass
        show_trend(hours)
    elif "--gaps" in sys.argv:
        hours = 5
        for arg in sys.argv[2:]:
            try:
                hours = int(arg)
            except ValueError:
                pass
        show_gaps(hours)
    elif "--dose" in sys.argv:
        dose_assessment()
    else:
        current_state()
