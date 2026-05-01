#!/usr/bin/env python3
"""Beat Log — records what each nudge cycle did for adaptive scheduling.

Inspired by Heartbeat-Driven Autonomous Thinking (arxiv 2604.14178).
Logs mode, action, and outcome per beat to enable learned scheduling.

Usage:
  beat_log.py log MODE ACTION [--outcome VALUE] [--note TEXT]
  beat_log.py history [--hours N] [--mode MODE]
  beat_log.py profile [--hours N]

Modes: recaller, critic, builder, dreamer, explorer, responder, idle
Outcomes: productive (default), null, error

Examples:
  beat_log.py log explorer "X search looped transformers" --outcome productive
  beat_log.py log responder "processed 3 captures" --outcome productive
  beat_log.py log idle "no captures, system steady" --outcome null
  beat_log.py profile --hours 24
"""

import os
import sqlite3
import sys
from datetime import datetime, timezone, timedelta

PDT = timezone(timedelta(hours=-7))
DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")


def ensure_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS beat_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            beat_ts TEXT NOT NULL,
            hour INTEGER NOT NULL,
            mode TEXT NOT NULL,
            action TEXT NOT NULL,
            outcome TEXT NOT NULL DEFAULT 'productive',
            note TEXT,
            created_at INTEGER NOT NULL
        )
    """)
    conn.commit()


def cmd_log(args):
    if len(args) < 2:
        print("Usage: beat_log.py log MODE ACTION [--outcome VALUE] [--note TEXT]")
        return

    mode = args[0].lower()
    action = args[1]
    outcome = "productive"
    note = None

    i = 2
    while i < len(args):
        if args[i] == "--outcome" and i + 1 < len(args):
            outcome = args[i + 1]
            i += 2
        elif args[i] == "--note" and i + 1 < len(args):
            note = args[i + 1]
            i += 2
        else:
            i += 1

    now = datetime.now(PDT)
    beat_ts = now.strftime("%Y%m%d_%H%M")
    hour = now.hour

    conn = sqlite3.connect(DB_PATH, timeout=5)
    ensure_table(conn)
    conn.execute(
        "INSERT INTO beat_log (beat_ts, hour, mode, action, outcome, note, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (beat_ts, hour, mode, action, outcome, note, int(now.timestamp()))
    )
    conn.commit()
    conn.close()

    print("Beat logged: {} {} → {}".format(mode, action[:60], outcome))


def cmd_history(args):
    hours = 24
    mode_filter = None

    i = 0
    while i < len(args):
        if args[i] == "--hours" and i + 1 < len(args):
            hours = int(args[i + 1])
            i += 2
        elif args[i] == "--mode" and i + 1 < len(args):
            mode_filter = args[i + 1]
            i += 2
        else:
            i += 1

    cutoff = int((datetime.now(PDT) - timedelta(hours=hours)).timestamp())

    conn = sqlite3.connect(DB_PATH, timeout=5)
    ensure_table(conn)

    if mode_filter:
        rows = conn.execute(
            "SELECT beat_ts, mode, action, outcome FROM beat_log "
            "WHERE created_at > ? AND mode = ? ORDER BY created_at",
            (cutoff, mode_filter)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT beat_ts, mode, action, outcome FROM beat_log "
            "WHERE created_at > ? ORDER BY created_at",
            (cutoff,)
        ).fetchall()
    conn.close()

    if not rows:
        print("No beats in last {}h.".format(hours))
        return

    print("Beat history — last {}h ({} beats)\n".format(hours, len(rows)))
    for ts, mode, action, outcome in rows:
        tag = "✓" if outcome == "productive" else ("·" if outcome == "null" else "✗")
        print("  {} {:>12} {:>10}  {}".format(tag, ts, mode, action[:70]))


def cmd_profile(args):
    hours = 24

    i = 0
    while i < len(args):
        if args[i] == "--hours" and i + 1 < len(args):
            hours = int(args[i + 1])
            i += 2
        else:
            i += 1

    cutoff = int((datetime.now(PDT) - timedelta(hours=hours)).timestamp())

    conn = sqlite3.connect(DB_PATH, timeout=5)
    ensure_table(conn)

    rows = conn.execute(
        "SELECT hour, mode, outcome FROM beat_log WHERE created_at > ?",
        (cutoff,)
    ).fetchall()
    conn.close()

    if not rows:
        print("No beats in last {}h.".format(hours))
        return

    # Mode distribution
    mode_counts = {}
    mode_productive = {}
    for _, mode, outcome in rows:
        mode_counts[mode] = mode_counts.get(mode, 0) + 1
        if outcome == "productive":
            mode_productive[mode] = mode_productive.get(mode, 0) + 1

    print("Mode profile — last {}h ({} beats)\n".format(hours, len(rows)))
    print("{:>12}  {:>5}  {:>5}  {:>6}".format("Mode", "Count", "Prod", "Rate"))
    print("{} {} {} {}".format("-" * 12, "-" * 5, "-" * 5, "-" * 6))
    for mode in sorted(mode_counts, key=mode_counts.get, reverse=True):
        count = mode_counts[mode]
        prod = mode_productive.get(mode, 0)
        rate = prod / count if count > 0 else 0
        print("{:>12}  {:>5}  {:>5}  {:>5.0%}".format(mode, count, prod, rate))

    # Hour distribution
    hour_counts = {}
    hour_modes = {}
    for hour, mode, _ in rows:
        hour_counts[hour] = hour_counts.get(hour, 0) + 1
        if hour not in hour_modes:
            hour_modes[hour] = {}
        hour_modes[hour][mode] = hour_modes[hour].get(mode, 0) + 1

    print("\nHour profile:")
    print("{:>4}  {:>5}  {}".format("Hour", "Beats", "Dominant mode"))
    print("{} {} {}".format("-" * 4, "-" * 5, "-" * 20))
    for hour in sorted(hour_counts):
        count = hour_counts[hour]
        modes = hour_modes[hour]
        dominant = max(modes, key=modes.get)
        print("{:>4}  {:>5}  {} ({})".format(hour, count, dominant, modes[dominant]))


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return

    cmd = sys.argv[1]
    args = sys.argv[2:]

    if cmd == "log":
        cmd_log(args)
    elif cmd == "history":
        cmd_history(args)
    elif cmd == "profile":
        cmd_profile(args)
    else:
        print(__doc__)


if __name__ == "__main__":
    main()
