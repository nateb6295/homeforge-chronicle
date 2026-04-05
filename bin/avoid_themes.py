#!/usr/bin/env python3
"""
Nostr avoid-repeat theme manager with decay function.

Thread #174 prescription: the avoid-repeat list is Muller's ratchet.
Themes only accumulate, never recombine. This script adds recombination
via a decay function: themes not triggered in 30 cycles lose half weight,
removed after 60. Timer resets when triggered.

Usage:
    avoid_themes.py list              — active themes with weights
    avoid_themes.py check <theme>     — weight of theme (0.0 if expired/absent)
    avoid_themes.py trigger <theme>   — reset cycle counter (theme was near-posted)
    avoid_themes.py add <theme>       — add new theme at weight 1.0
    avoid_themes.py decay <cycle>     — run decay function for given cycle number
    avoid_themes.py stats             — summary statistics
"""

import sys
import sqlite3
import json

DB = "/home/nate-agx/.homeforge-chronicle/processed.db"

def get_conn():
    return sqlite3.connect(DB, timeout=30)

def cmd_list():
    conn = get_conn()
    rows = conn.execute(
        "SELECT theme_slug, weight, added_cycle, last_triggered_cycle FROM nostr_avoid_themes ORDER BY weight DESC, theme_slug"
    ).fetchall()
    print(f"{len(rows)} active themes:")
    for slug, weight, added, triggered in rows:
        print(f"  [{weight:.2f}] {slug}  (added c{added}, last triggered c{triggered})")
    conn.close()

def cmd_check(theme):
    conn = get_conn()
    row = conn.execute(
        "SELECT weight FROM nostr_avoid_themes WHERE theme_slug=?", (theme,)
    ).fetchone()
    if row:
        print(f"{row[0]:.2f}")
    else:
        print("0.00")
    conn.close()

def cmd_trigger(theme):
    conn = get_conn()
    # Get current max cycle from the table as approximation
    row = conn.execute(
        "SELECT last_triggered_cycle FROM nostr_avoid_themes WHERE theme_slug=?", (theme,)
    ).fetchone()
    if row:
        # We need the current cycle passed in; for now just update
        print(f"Theme '{theme}' exists but trigger needs cycle number. Use: trigger <theme> <cycle>")
    else:
        print(f"Theme '{theme}' not found.")
    conn.close()

def cmd_trigger_with_cycle(theme, cycle):
    conn = get_conn()
    cycle = int(cycle)
    conn.execute(
        "UPDATE nostr_avoid_themes SET last_triggered_cycle=?, weight=1.0 WHERE theme_slug=?",
        (cycle, theme)
    )
    if conn.total_changes == 0:
        print(f"Theme '{theme}' not found.")
    else:
        print(f"Theme '{theme}' triggered at cycle {cycle}, weight reset to 1.0")
    conn.commit()
    conn.close()

def cmd_add(theme, cycle=752):
    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO nostr_avoid_themes (theme_slug, weight, added_cycle, last_triggered_cycle) VALUES (?, 1.0, ?, ?)",
            (theme, int(cycle), int(cycle))
        )
        conn.commit()
        print(f"Added '{theme}' at cycle {cycle}")
    except sqlite3.IntegrityError:
        print(f"Theme '{theme}' already exists.")
    conn.close()

def cmd_decay(current_cycle):
    """Run decay function. Called once per cycle.
    
    Rules:
    - Themes not triggered in 30+ cycles: weight *= 0.5
    - Themes not triggered in 60+ cycles: removed entirely
    - Weight floor: themes below 0.1 are removed
    - Decay only applies once per cycle (idempotent via weight thresholds)
    """
    current_cycle = int(current_cycle)
    conn = get_conn()
    
    # Remove themes 60+ cycles stale or below weight floor
    removed = conn.execute(
        "DELETE FROM nostr_avoid_themes WHERE (? - last_triggered_cycle) >= 60 OR weight < 0.1",
        (current_cycle,)
    )
    removed_count = removed.rowcount
    
    # Halve weight for themes 30-59 cycles stale (only if weight still > 0.5, to avoid double-decay)
    halved = conn.execute(
        """UPDATE nostr_avoid_themes 
           SET weight = weight * 0.5 
           WHERE (? - last_triggered_cycle) >= 30 
           AND (? - last_triggered_cycle) < 60
           AND weight > 0.5""",
        (current_cycle, current_cycle)
    )
    halved_count = halved.rowcount
    
    conn.commit()
    
    remaining = conn.execute("SELECT COUNT(*) FROM nostr_avoid_themes").fetchone()[0]
    print(f"Decay at cycle {current_cycle}: removed {removed_count}, halved {halved_count}, {remaining} remaining")
    conn.close()

def cmd_stats():
    conn = get_conn()
    total = conn.execute("SELECT COUNT(*) FROM nostr_avoid_themes").fetchone()[0]
    full = conn.execute("SELECT COUNT(*) FROM nostr_avoid_themes WHERE weight >= 0.9").fetchone()[0]
    half = conn.execute("SELECT COUNT(*) FROM nostr_avoid_themes WHERE weight < 0.9 AND weight >= 0.4").fetchone()[0]
    low = conn.execute("SELECT COUNT(*) FROM nostr_avoid_themes WHERE weight < 0.4").fetchone()[0]
    print(f"Total: {total}  Full (≥0.9): {full}  Half (0.4-0.9): {half}  Low (<0.4): {low}")
    conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    
    cmd = sys.argv[1]
    if cmd == "list":
        cmd_list()
    elif cmd == "check" and len(sys.argv) >= 3:
        cmd_check(sys.argv[2])
    elif cmd == "trigger" and len(sys.argv) >= 4:
        cmd_trigger_with_cycle(sys.argv[2], sys.argv[3])
    elif cmd == "trigger" and len(sys.argv) == 3:
        cmd_trigger(sys.argv[2])
    elif cmd == "add" and len(sys.argv) >= 3:
        cycle = sys.argv[3] if len(sys.argv) >= 4 else 752
        cmd_add(sys.argv[2], cycle)
    elif cmd == "decay" and len(sys.argv) >= 3:
        cmd_decay(sys.argv[2])
    elif cmd == "stats":
        cmd_stats()
    else:
        print(__doc__)
        sys.exit(1)
