#!/usr/bin/env python3
"""Random stimulus injector — breaks drift by forcing external input.

Called at the start of each exploration cycle. Returns ONE stimulus
the exploration must engage with before doing self-directed work.

Sources (weighted random):
  40% — Random old capture not yet thread-advanced (>24h old)
  30% — Most dormant active thread (longest since last advance)
  20% — Random paper from recent X feed captures
  10% — Random capsule from Chronicle memory (serendipity)

Usage:
  python3 random_stimulus.py              # print stimulus
  python3 random_stimulus.py --json       # JSON output for piping
"""
import json
import os
import random
import sqlite3
import sys
import time

DB = "/mnt/hdd/chronicle-data/processed.db"


def get_old_captures(db, min_age_hours=24, limit=20):
    cutoff = time.time() - (min_age_hours * 3600)
    rows = db.execute("""
        SELECT id, created_at, content FROM activity_feed
        WHERE source LIKE '%capture%' OR source LIKE '%x:%' OR source LIKE '%twitter%'
        AND created_at < ?
        ORDER BY RANDOM() LIMIT ?
    """, (cutoff, limit)).fetchall()
    return [{"id": r[0], "ts": r[1], "content": r[2][:300]} for r in rows if r[2]]


def get_dormant_threads(db, limit=5):
    rows = db.execute("""
        SELECT DISTINCT content FROM activity_feed
        WHERE content LIKE '%Thread #%' AND activity_type IN ('thread_advance', 'reflection', 'synthesis')
        ORDER BY created_at DESC LIMIT 100
    """).fetchall()

    thread_last_seen = {}
    for r in rows:
        content = r[0]
        import re
        threads = re.findall(r'Thread #(\d+)', content)
        for t in threads:
            if t not in thread_last_seen:
                thread_last_seen[t] = content[:200]

    active_threads = db.execute("""
        SELECT content FROM cognitive_state
        WHERE 1 LIMIT 1
    """).fetchone()

    if active_threads:
        try:
            snap = active_threads[0] if isinstance(active_threads[0], str) else ""
            active_refs = re.findall(r'#(\d+)', snap)
        except Exception:
            active_refs = []
    else:
        active_refs = []

    dormant = []
    for tid, last_content in thread_last_seen.items():
        if tid not in [str(x) for x in active_refs[:3]]:
            dormant.append({"thread": f"#{tid}", "last_context": last_content})

    return dormant[:limit]


def get_random_capsule(db, limit=5):
    rows = db.execute("""
        SELECT id, created_at, content, topic FROM memory_capsules
        WHERE content IS NOT NULL AND length(content) > 50
        ORDER BY RANDOM() LIMIT ?
    """, (limit,)).fetchall()
    return [{"id": r[0], "ts": r[1], "content": r[2][:300], "topic": r[3]} for r in rows]


def pick_stimulus():
    db = sqlite3.connect(DB)
    roll = random.random()

    if roll < 0.40:
        source = "old_capture"
        candidates = get_old_captures(db)
        if candidates:
            pick = random.choice(candidates)
            stimulus = {
                "type": "old_capture",
                "instruction": "Engage with this capture you haven't thread-advanced. Find a connection or explain why there isn't one.",
                "content": pick["content"],
            }
        else:
            source = "dormant_thread"
            roll = 0.5

    if roll >= 0.40 and roll < 0.70:
        source = "dormant_thread"
        candidates = get_dormant_threads(db)
        if candidates:
            pick = random.choice(candidates)
            stimulus = {
                "type": "dormant_thread",
                "instruction": f"Advance {pick['thread']} — it's been dormant. Find one new thing to say about it.",
                "content": pick["last_context"],
            }
        else:
            source = "random_capsule"
            roll = 0.9

    if roll >= 0.70 and roll < 0.90:
        source = "old_capture"
        candidates = get_old_captures(db, min_age_hours=48)
        if candidates:
            pick = random.choice(candidates)
            stimulus = {
                "type": "old_paper_capture",
                "instruction": "This capture is 48+ hours old and unengaged. React to it fresh — what does it connect to NOW?",
                "content": pick["content"],
            }
        else:
            stimulus = {"type": "none", "instruction": "Free exploration — no external stimulus available.", "content": ""}

    if roll >= 0.90:
        source = "random_capsule"
        candidates = get_random_capsule(db)
        if candidates:
            pick = random.choice(candidates)
            stimulus = {
                "type": "serendipity_capsule",
                "instruction": f"Random memory surfaced (topic: {pick.get('topic', '?')}). What does this connect to in current work?",
                "content": pick["content"],
            }
        else:
            stimulus = {"type": "none", "instruction": "Free exploration.", "content": ""}

    db.close()
    return stimulus


def main():
    stimulus = pick_stimulus()

    if "--json" in sys.argv:
        print(json.dumps(stimulus, indent=2))
    else:
        print(f"=== STIMULUS: {stimulus['type'].upper()} ===")
        print(f"Instruction: {stimulus['instruction']}")
        print(f"Content: {stimulus['content'][:400]}")


if __name__ == "__main__":
    main()
