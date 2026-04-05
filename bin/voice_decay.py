#!/usr/bin/env python3
"""Voice decay — auto-resolve stale voices to prevent backlog burial.

Nate approved 2026-04-04: "Yeah, those need to be Decayed or capped."

Rules:
  - Voices older than MAX_AGE_H hours → auto-resolved as 'decayed'
  - If unread count still exceeds CAP after decay, resolve oldest until at cap
  - Recent voices (< MAX_AGE_H) are never touched
  - Prints summary of what was decayed

Usage:
  python3 voice_decay.py              # run decay with defaults
  python3 voice_decay.py --dry-run    # show what would decay without changing DB
  python3 voice_decay.py --max-age 12 # custom age threshold in hours
  python3 voice_decay.py --cap 30     # custom unread cap
"""

import sqlite3
import time
import sys
import os

DB_PATH = os.path.expanduser("~/.homeforge-chronicle/processed.db")
DEFAULT_MAX_AGE_H = 24
DEFAULT_CAP = 50


def decay(db_path=DB_PATH, max_age_h=DEFAULT_MAX_AGE_H, cap=DEFAULT_CAP, dry_run=False):
    """Run voice decay. Returns dict with counts."""
    conn = sqlite3.connect(db_path, timeout=10)
    conn.row_factory = sqlite3.Row
    now = int(time.time())
    cutoff = now - (max_age_h * 3600)

    # Phase 1: age-based decay
    old_voices = conn.execute(
        "SELECT id, agent, voice_type, created_at FROM agent_voice "
        "WHERE status='unread' AND created_at < ? ORDER BY created_at",
        (cutoff,)
    ).fetchall()

    aged_out = len(old_voices)
    if not dry_run and aged_out > 0:
        conn.execute(
            "UPDATE agent_voice SET status='decayed' "
            "WHERE status='unread' AND created_at < ?",
            (cutoff,)
        )
        conn.commit()

    # Phase 2: cap-based decay (if still too many after age decay)
    # In dry-run, exclude the IDs we would have decayed in phase 1
    if dry_run and old_voices:
        aged_ids = [r["id"] for r in old_voices]
        placeholders = ",".join("?" * len(aged_ids))
        remaining = conn.execute(
            f"SELECT id, agent, voice_type, created_at FROM agent_voice "
            f"WHERE status='unread' AND id NOT IN ({placeholders}) ORDER BY created_at",
            aged_ids
        ).fetchall()
    else:
        remaining = conn.execute(
            "SELECT id, agent, voice_type, created_at FROM agent_voice "
            "WHERE status='unread' ORDER BY created_at"
        ).fetchall()

    capped = 0
    if len(remaining) > cap:
        excess = len(remaining) - cap
        to_cap = remaining[:excess]
        capped = len(to_cap)
        if not dry_run and capped > 0:
            ids = [r["id"] for r in to_cap]
            placeholders = ",".join("?" * len(ids))
            conn.execute(
                f"UPDATE agent_voice SET status='decayed' "
                f"WHERE id IN ({placeholders})",
                ids
            )
            conn.commit()

    # Final count
    if dry_run:
        final_unread = len(remaining) - capped
    else:
        final_unread = conn.execute(
            "SELECT COUNT(*) FROM agent_voice WHERE status='unread'"
        ).fetchone()[0]

    conn.close()

    return {
        "aged_out": aged_out,
        "capped": capped,
        "total_decayed": aged_out + capped,
        "remaining_unread": final_unread,
        "max_age_h": max_age_h,
        "cap": cap,
        "dry_run": dry_run,
    }


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Voice decay — resolve stale voices")
    parser.add_argument("--dry-run", action="store_true", help="Show what would decay")
    parser.add_argument("--max-age", type=int, default=DEFAULT_MAX_AGE_H, help="Max age in hours (default 24)")
    parser.add_argument("--cap", type=int, default=DEFAULT_CAP, help="Max unread voices to keep (default 50)")
    args = parser.parse_args()

    result = decay(max_age_h=args.max_age, cap=args.cap, dry_run=args.dry_run)

    prefix = "[DRY RUN] " if result["dry_run"] else ""
    print(f"{prefix}Voice decay (max_age={result['max_age_h']}h, cap={result['cap']}):")
    print(f"  Age-based:  {result['aged_out']} voices older than {result['max_age_h']}h")
    print(f"  Cap-based:  {result['capped']} voices exceeding cap of {result['cap']}")
    print(f"  Total:      {result['total_decayed']} decayed")
    print(f"  Remaining:  {result['remaining_unread']} unread")


if __name__ == "__main__":
    main()
