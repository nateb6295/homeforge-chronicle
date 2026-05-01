#!/usr/bin/env python3
"""
feed_triage.py — Automated feed relevance scoring against active threads.

Gap #5 from ecosystem inventory: feed triage is entirely manual. This tool
scores recent feed items against active thread questions to surface what's
worth reading.

Uses nomic-embed-text (matches capsule embedding model) for semantic similarity.

Usage:
    feed_triage.py scan [--hours N] [--min-score 0.3]  # Score recent feeds
    feed_triage.py top [--hours N] [--limit 10]         # Show top relevant items
    feed_triage.py stats                                 # Feed source quality stats
"""
import json
import math
import os
import sqlite3
import struct
import sys
import time
import urllib.request

DB = "/mnt/hdd/chronicle-data/processed.db"
OLLAMA = os.environ.get("OLLAMA_URL", "http://192.168.1.11:11434")
MODEL = "nomic-embed-text"

# Feed sources worth triaging (skip internal operational sources)
TRIAGE_SOURCES = {"seeker:algo", "eye", "discord:capture", "operator:capture"}
SKIP_SOURCES = {"gemma", "sentinel", "hal", "spot_check", "capsule-sync",
                "keeper-pull", "discord:opus", "discord:reaction",
                "prediction_monitor", "discord:nate"}


def embed(text, timeout=30):
    """Get embedding via Ollama."""
    body = json.dumps({"model": MODEL, "prompt": text[:800]}).encode()
    req = urllib.request.Request(
        f"{OLLAMA}/api/embeddings", data=body,
        headers={"Content-Type": "application/json"}
    )
    resp = urllib.request.urlopen(req, timeout=timeout)
    return json.loads(resp.read())["embedding"]


def cosine(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def get_active_threads():
    """Get active thread questions with embeddings."""
    db = sqlite3.connect(DB)
    threads = db.execute(
        "SELECT id, title, question FROM cognitive_threads "
        "WHERE status IN ('active', 'advancing') ORDER BY updated_at DESC"
    ).fetchall()
    db.close()

    result = []
    for tid, title, question in threads:
        text = f"{title}: {question}"
        try:
            vec = embed(text)
            result.append({"id": tid, "title": title, "question": question, "embedding": vec})
        except Exception:
            continue
    return result


def get_recent_feeds(hours=6, skip_scored=True):
    """Get recent feed items from triageable sources."""
    cutoff = int(time.time()) - (hours * 3600)
    db = sqlite3.connect(DB)

    # Build source filter
    source_clauses = " OR ".join(f"source = '{s}'" for s in TRIAGE_SOURCES)
    like_clauses = " OR ".join(f"source LIKE '{s}%'" for s in TRIAGE_SOURCES)

    rows = db.execute(
        f"SELECT id, source, title, content, created_at FROM activity_feed "
        f"WHERE created_at > ? AND ({source_clauses} OR {like_clauses}) "
        f"ORDER BY created_at DESC LIMIT 200",
        (cutoff,)
    ).fetchall()
    db.close()

    feeds = []
    for fid, source, title, content, ts in rows:
        text = f"{title or ''} {content[:300]}"
        feeds.append({
            "id": fid, "source": source, "title": title or "",
            "content": content[:200], "timestamp": ts, "text": text
        })
    return feeds


def score_feeds(threads, feeds, min_score=0.25):
    """Score each feed item against all active threads."""
    scored = []

    for feed in feeds:
        try:
            feed_vec = embed(feed["text"])
        except Exception:
            continue

        best_thread = None
        best_score = 0

        for thread in threads:
            sim = cosine(feed_vec, thread["embedding"])
            if sim > best_score:
                best_score = sim
                best_thread = thread

        if best_score >= min_score and best_thread:
            scored.append({
                "feed_id": feed["id"],
                "source": feed["source"],
                "title": feed["title"][:80],
                "content_preview": feed["content"][:100],
                "thread_id": best_thread["id"],
                "thread_title": best_thread["title"],
                "relevance": round(best_score, 4),
                "age_hours": round((time.time() - feed["timestamp"]) / 3600, 1),
            })

    scored.sort(key=lambda x: -x["relevance"])
    return scored


def scan(hours=6, min_score=0.25):
    """Full triage scan."""
    print(f"Loading active threads...")
    threads = get_active_threads()
    print(f"  {len(threads)} active threads embedded")

    print(f"Loading feeds ({hours}h lookback)...")
    feeds = get_recent_feeds(hours)
    print(f"  {len(feeds)} feed items found")

    if not threads or not feeds:
        print("Nothing to triage.")
        return []

    print(f"Scoring...")
    scored = score_feeds(threads, feeds, min_score)

    print(f"\nFEED TRIAGE ({len(scored)} items above {min_score} threshold):\n")
    for item in scored[:30]:
        print(f"  {item['relevance']:.3f}  [{item['source'][:12]}] → Thread #{item['thread_id']} ({item['thread_title']})")
        print(f"         {item['title'][:70]}")
        print()

    # Summary
    if scored:
        by_thread = {}
        for s in scored:
            by_thread.setdefault(s["thread_id"], []).append(s)
        print(f"\nPer-thread relevance:")
        for tid, items in by_thread.items():
            avg = sum(i["relevance"] for i in items) / len(items)
            print(f"  Thread #{tid} ({items[0]['thread_title']}): {len(items)} items, avg relevance {avg:.3f}")

    return scored


def show_top(hours=6, limit=10):
    """Show top N most relevant feed items."""
    threads = get_active_threads()
    feeds = get_recent_feeds(hours)

    if not threads or not feeds:
        print("Nothing to triage.")
        return

    scored = score_feeds(threads, feeds, min_score=0.2)

    print(f"TOP {min(limit, len(scored))} RELEVANT FEEDS ({hours}h):\n")
    for item in scored[:limit]:
        print(f"  {item['relevance']:.3f}  [{item['source'][:12]}] {item['title'][:60]}")
        print(f"         → Thread #{item['thread_id']}: {item['thread_title']}")
        print(f"         {item['content_preview'][:80]}...")
        print()


def show_stats():
    """Show feed source quality statistics."""
    db = sqlite3.connect(DB)
    # Last 24h feed volume by source
    cutoff = int(time.time()) - 86400
    rows = db.execute(
        "SELECT source, COUNT(*) as cnt FROM activity_feed "
        "WHERE created_at > ? GROUP BY source ORDER BY cnt DESC",
        (cutoff,)
    ).fetchall()
    db.close()

    total = sum(cnt for _, cnt in rows)
    triage_total = sum(cnt for src, cnt in rows if src in TRIAGE_SOURCES or
                       any(src.startswith(s) for s in TRIAGE_SOURCES))

    print(f"FEED STATS (24h)")
    print(f"  Total items: {total}")
    print(f"  Triageable:  {triage_total} ({triage_total/max(total,1)*100:.0f}%)")
    print()
    for src, cnt in rows:
        triage = "T" if src in TRIAGE_SOURCES or any(src.startswith(s) for s in TRIAGE_SOURCES) else " "
        print(f"  [{triage}] {src:<25} {cnt:>4}")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return

    cmd = sys.argv[1]

    if cmd == "scan":
        hours = 6
        min_score = 0.25
        for i, arg in enumerate(sys.argv[2:], 2):
            if arg == "--hours" and i + 1 < len(sys.argv):
                hours = int(sys.argv[i + 1])
            if arg == "--min-score" and i + 1 < len(sys.argv):
                min_score = float(sys.argv[i + 1])
        scan(hours, min_score)

    elif cmd == "top":
        hours = 6
        limit = 10
        for i, arg in enumerate(sys.argv[2:], 2):
            if arg == "--hours" and i + 1 < len(sys.argv):
                hours = int(sys.argv[i + 1])
            if arg == "--limit" and i + 1 < len(sys.argv):
                limit = int(sys.argv[i + 1])
        show_top(hours, limit)

    elif cmd == "stats":
        show_stats()
    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)


if __name__ == "__main__":
    main()
