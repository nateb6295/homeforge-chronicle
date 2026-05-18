#!/usr/bin/env python3
"""
CCS mid-cycle touch-up.

Full compress_cognitive_state is expensive (LLM call) and runs at rotation
boundaries. In between, substrate drift accumulates: new captures, thread
advances, directives. A stale CCS fails the calibration-vs-effort test
(measured 2026-04-15: 3/4 questions failed with 6.5h-old CCS).

This script does a cheap refresh:
  - Append the most recent thread advance summaries to episodic_trace
  - Append recent operator captures (titles only)
  - Update predictive_cue from the latest checkpoint if present
  - Bump updated_at

It does NOT re-run compression. It keeps the bounded-state discipline by
trimming episodic_trace to the last N entries.

Usage:
  python3 ~/chronicle/bin/ccs_touch.py        # run refresh
  python3 ~/chronicle/bin/ccs_touch.py --dry  # show what would change
"""
import json
import re
import sqlite3
import sys
import time
from pathlib import Path

DB = "/mnt/hdd/chronicle-data/processed.db"
TRACE_WINDOW_EPOCHS = 3600  # 1 hour
MAX_EPISODIC = 10


def latest_thread_advances(db, window=TRACE_WINDOW_EPOCHS, limit=5):
    cutoff = int(time.time()) - window
    rows = db.execute(
        "SELECT content, created_at FROM thread_history "
        "WHERE event_type = 'advanced' AND created_at >= ? "
        "ORDER BY created_at DESC LIMIT ?",
        (cutoff, limit),
    ).fetchall()
    summaries = []
    for content, ts in rows:
        first_line = content.split("\n", 1)[0][:220]
        summaries.append(first_line.strip())
    return summaries


def latest_directives(db, window=TRACE_WINDOW_EPOCHS * 6, limit=3):
    cutoff = int(time.time()) - window
    rows = db.execute(
        "SELECT content, priority, created_at FROM directives "
        "WHERE created_at >= ? AND source != 'operator:nate' "
        "ORDER BY created_at DESC LIMIT ?",
        (cutoff, limit),
    ).fetchall()
    out = []
    for content, priority, ts in rows:
        snippet = (content or "").split("\n", 1)[0][:180].strip()
        out.append(f"directive(p{priority}): {snippet}")
    return out


def board_digest():
    out = []
    for name in ("nate-board.md", "opus-board.md"):
        path = Path.home() / "chronicle" / name
        if not path.exists():
            continue
        text = path.read_text()
        head = text.split("\n\n", 1)[0].strip()[:300]
        out.append(f"{name}: {head}")
    return out


def latest_operator_captures(db, window=TRACE_WINDOW_EPOCHS, limit=5):
    cutoff = int(time.time()) - window
    rows = db.execute(
        "SELECT title, content, created_at FROM activity_feed "
        "WHERE source LIKE 'operator%' AND activity_type = 'capture' "
        "AND created_at >= ? ORDER BY created_at DESC LIMIT ?",
        (cutoff, limit),
    ).fetchall()
    out = []
    for title, content, ts in rows:
        label = (title or "").strip() or (content or "")[:80].split("\n", 1)[0]
        out.append(f"capture: {label}")
    return out


FILLER_RE = re.compile(
    r'\b(the|a|an|is|are|was|were|has|have|had|this|that|these|those|which|who|whom)\b',
    re.IGNORECASE,
)


def _compact_entry(text):
    """Compress an episodic entry — strip filler, collapse whitespace.

    Advance 46 confirmed compact CCS outperforms verbose at LLM-reading level.
    Applying at storage time so all consumers (Python + Rust MCP) benefit.
    """
    text = FILLER_RE.sub('', text)
    text = ' '.join(text.split())
    return text


def touch_ccs(dry=False):
    db = sqlite3.connect(DB)
    row = db.execute(
        "SELECT id, episodic_trace, predictive_cue, updated_at "
        "FROM cognitive_state ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if not row:
        print("No CCS row found; nothing to touch.", file=sys.stderr)
        return 1
    ccs_id, ep_json, predictive_cue, old_updated = row

    try:
        episodic = json.loads(ep_json) if ep_json else []
    except (json.JSONDecodeError, TypeError):
        episodic = []

    additions = (
        latest_directives(db)
        + latest_thread_advances(db)
        + latest_operator_captures(db)
    )
    if not additions:
        print(f"No new activity in last {TRACE_WINDOW_EPOCHS}s. Skipping touch.")
        return 0

    compacted = [_compact_entry(a) for a in additions]
    new_episodic = (compacted + episodic)[:MAX_EPISODIC]

    new_updated = int(time.time())

    if dry:
        print("DRY RUN — would update CCS:")
        print(f"  id: {ccs_id}")
        print(f"  old updated_at: {old_updated} ({time.ctime(old_updated)})")
        print(f"  new updated_at: {new_updated} ({time.ctime(new_updated)})")
        print(f"  additions ({len(additions)}):")
        for a in compacted:
            print(f"    - {a}")
        print(f"  new episodic length: {len(new_episodic)}")
        return 0

    db.execute(
        "UPDATE cognitive_state SET episodic_trace = ?, updated_at = ? WHERE id = ?",
        (json.dumps(new_episodic), new_updated, ccs_id),
    )
    db.commit()
    drift = new_updated - old_updated
    print(
        f"CCS touched. Added {len(additions)} entries. "
        f"Previous updated_at was {drift}s ago ({drift//60} min)."
    )
    return 0


if __name__ == "__main__":
    dry = "--dry" in sys.argv
    sys.exit(touch_ccs(dry=dry))
