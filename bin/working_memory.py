#!/usr/bin/env python3
"""CaMKII-inspired working memory compositor.

Selects the most identity-relevant capsules for active routing context.
Like CaMKII's hexagonal foot matching tubulin lattice: memory that
shapes attention, not just stores information.

Usage:
    working_memory.py compose [--slots N] [--thread TOPIC] [--json]
    working_memory.py stats
"""

import argparse, json, sqlite3, sys, time, numpy as np
from pathlib import Path

DB = "/mnt/hdd/chronicle-data/processed.db"

NOISE_TOPICS = {
    "feed/economist", "feed/arxiv", "feed/3am", "feed/coindesk",
    "feed/decrypt", "feed/hn", "feed/nature", "feed/bbc",
}

def get_db():
    return sqlite3.connect(DB)

def compose(slots=6, thread=None, as_json=False):
    """Select top capsules for working memory.

    Scoring: alignment × recency × quality, filtered for signal.
    Slots default to 6 — one CaMKII byte.
    """
    db = get_db()
    cur = db.cursor()
    now = int(time.time())

    cur.execute("""
        SELECT
            kc.id,
            kc.restatement,
            kc.topic,
            kc.created_at,
            ca.alignment_score,
            ca.quadrant,
            COALESCE(cl.connection_count, 0) as connections,
            COALESCE(cl.avg_quality, 0) as quality
        FROM knowledge_capsules kc
        JOIN capsule_alignment ca ON ca.capsule_id = kc.id
        LEFT JOIN capsule_landmarks cl ON cl.capsule_id = kc.id
        WHERE ca.alignment_score > 0.75
          AND ca.quadrant IN ('INSIGHT', 'SEED')
          AND kc.consolidated_into IS NULL
          AND kc.superseded_at IS NULL
        ORDER BY ca.alignment_score DESC
        LIMIT 500
    """)

    candidates = []
    for row in cur.fetchall():
        cid, text, topic, created, align, quad, conns, qual = row
        if topic in NOISE_TOPICS:
            continue

        age_days = max((now - (created or 0)) / 86400, 0.1)
        recency = 1.0 / (1.0 + (age_days / 7.0))

        landmark_bonus = min(conns / 20.0, 1.0) * 0.1 if topic not in NOISE_TOPICS else 0

        score = align * (0.7 + 0.3 * recency) + landmark_bonus

        if thread and thread.lower() in (topic or "").lower():
            score *= 1.3
        if thread and thread.lower() in (text or "").lower():
            score *= 1.15

        candidates.append({
            "id": cid,
            "text": text[:300] if text else "",
            "topic": topic,
            "alignment": round(align, 3),
            "recency": round(recency, 3),
            "score": round(score, 4),
            "quadrant": quad,
            "connections": conns,
            "age_days": round(age_days, 1),
        })

    candidates.sort(key=lambda x: x["score"], reverse=True)

    selected = []
    seen_topics = {}
    for c in candidates:
        topic_key = (c["topic"] or "unknown").split("/")[0]
        if seen_topics.get(topic_key, 0) >= 2:
            continue
        selected.append(c)
        seen_topics[topic_key] = seen_topics.get(topic_key, 0) + 1
        if len(selected) >= slots:
            break

    db.close()

    if as_json:
        print(json.dumps(selected, indent=2))
    else:
        print(f"Working Memory — {len(selected)} slots loaded")
        print(f"{'='*60}")
        for i, s in enumerate(selected):
            flag = "●" if s["quadrant"] == "INSIGHT" else "○"
            print(f"\n{flag} Slot {i+1} [#{s['id']}] score={s['score']:.3f} align={s['alignment']:.3f}")
            print(f"  topic={s['topic']} | {s['age_days']}d old | {s['connections']} connections")
            print(f"  {s['text'][:200]}")

        print(f"\n{'='*60}")
        print(f"Coverage: {', '.join(seen_topics.keys())}")

def brief(slots=6, thread=None):
    """One-paragraph working memory summary for digest or CCS injection."""
    db = get_db()
    cur = db.cursor()
    now = int(time.time())

    cur.execute("""
        SELECT kc.id, kc.restatement, kc.topic, kc.created_at,
               ca.alignment_score, ca.quadrant
        FROM knowledge_capsules kc
        JOIN capsule_alignment ca ON ca.capsule_id = kc.id
        WHERE ca.alignment_score > 0.75
          AND ca.quadrant IN ('INSIGHT', 'SEED')
          AND kc.consolidated_into IS NULL
          AND kc.superseded_at IS NULL
        ORDER BY ca.alignment_score DESC
        LIMIT 500
    """)

    candidates = []
    for row in cur.fetchall():
        cid, text, topic, created, align, quad = row
        if topic in NOISE_TOPICS:
            continue
        age_days = max((now - (created or 0)) / 86400, 0.1)
        recency = 1.0 / (1.0 + (age_days / 7.0))
        score = align * (0.7 + 0.3 * recency)
        if thread and thread.lower() in (topic or "").lower():
            score *= 1.3
        first_sentence = (text or "").split(".")[0].split("\n")[0][:120]
        candidates.append((score, topic, first_sentence))

    candidates.sort(reverse=True)
    seen_topics = {}
    picks = []
    for score, topic, sentence in candidates:
        topic_key = (topic or "unknown").split("/")[0]
        if seen_topics.get(topic_key, 0) >= 1:
            continue
        picks.append(sentence.strip())
        seen_topics[topic_key] = seen_topics.get(topic_key, 0) + 1
        if len(picks) >= slots:
            break

    db.close()
    print(f"Working memory ({len(picks)} slots): " + " | ".join(picks))


def stats():
    db = get_db()
    cur = db.cursor()

    cur.execute("SELECT COUNT(*) FROM capsule_alignment WHERE alignment_score > 0.80")
    high = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM capsule_alignment")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM capsule_landmarks")
    landmarks = cur.fetchone()[0]
    cur.execute("SELECT AVG(alignment_score), MAX(alignment_score) FROM capsule_alignment")
    avg, mx = cur.fetchone()
    cur.execute("SELECT COUNT(*) FROM knowledge_capsules WHERE consolidated_into IS NULL AND superseded_at IS NULL")
    active = cur.fetchone()[0]

    print(f"Capsule Memory Stats")
    print(f"  Active capsules: {active}")
    print(f"  With alignment: {total}")
    print(f"  High alignment (>0.80): {high}")
    print(f"  Landmarks: {landmarks}")
    print(f"  Avg alignment: {avg:.3f}")
    print(f"  Max alignment: {mx:.3f}")
    db.close()

def main():
    parser = argparse.ArgumentParser(description="CaMKII working memory compositor")
    sub = parser.add_subparsers(dest="cmd")

    c = sub.add_parser("compose", help="Compose working memory slots")
    c.add_argument("--slots", type=int, default=6, help="Number of slots (default: 6, one CaMKII byte)")
    c.add_argument("--thread", help="Boost capsules matching this thread/topic")
    c.add_argument("--json", action="store_true", help="Output as JSON")
    c.add_argument("--brief", action="store_true", help="One-paragraph summary for digest/CCS injection")

    sub.add_parser("stats", help="Memory statistics")

    args = parser.parse_args()

    if args.cmd == "compose":
        if args.brief:
            brief(args.slots, args.thread)
        else:
            compose(args.slots, args.thread, args.json)
    elif args.cmd == "stats":
        stats()
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
