#!/usr/bin/env python3
"""Capture Enrichment — fetches tweet text for bare-URL captures.

Scans discord:capture entries that are just URLs, fetches tweet content
via xmcp getPostsById, and updates the DB entry with the actual text.
This makes captures embeddable for connection_ripple.py.

Usage:
  capture_enrich.py              # enrich all unenriched captures from last 48h
  capture_enrich.py --dry-run    # show what would be enriched without changing DB
  capture_enrich.py --window 72  # custom lookback window in hours
"""

import argparse
import json
import os
import re
import sqlite3
import subprocess
import sys
import time

DB = "/mnt/hdd/chronicle-data/processed.db"
XMCP = os.path.expanduser("~/chronicle/bin/xmcp_call.py")
ENRICHMENT_MARKER = "\n@"  # enriched entries have @Author: text appended


def extract_tweet_id(content: str) -> str | None:
    m = re.search(r'x\.com/\w+/status/(\d+)', content)
    return m.group(1) if m else None


def extract_author_from_url(content: str) -> str:
    m = re.search(r'x\.com/(\w+)/status/', content)
    return m.group(1) if m else "unknown"


def is_bare_url(content: str) -> bool:
    stripped = re.sub(r'^\[Discord #\w+\]\s*\w+:\s*', '', content.strip())
    stripped = re.sub(r'https?://\S+', '', stripped).strip()
    return len(stripped) < 20


def fetch_tweet(tweet_id: str) -> str | None:
    try:
        env = {**os.environ}
        envfile = os.path.expanduser("~/chronicle/chronicle.env")
        if os.path.exists(envfile):
            with open(envfile) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        env[k.strip()] = v.strip()
        result = subprocess.run(
            [sys.executable, XMCP, "getPostsById", json.dumps({"id": tweet_id})],
            capture_output=True, text=True, timeout=30, env=env,
        )
        if result.returncode != 0:
            return None
        data = json.loads(result.stdout)
        return data.get("data", {}).get("text")
    except Exception as e:
        print(f"  fetch error for {tweet_id}: {e}", file=sys.stderr)
        return None


def get_unenriched_captures(db, hours=48):
    cutoff = int(time.time()) - (hours * 3600)
    rows = db.execute(
        "SELECT id, content FROM activity_feed "
        "WHERE source LIKE '%capture%' AND created_at >= ? "
        "ORDER BY created_at DESC",
        (cutoff,),
    ).fetchall()
    unenriched = []
    for row_id, content in rows:
        if is_bare_url(content):
            tweet_id = extract_tweet_id(content)
            if tweet_id:
                unenriched.append((row_id, content, tweet_id))
    return unenriched


def enrich_capture(db, row_id: int, original: str, tweet_text: str, author: str):
    enriched = f"{original}\n@{author}: {tweet_text}"
    db.execute(
        "UPDATE activity_feed SET content = ?, activity_type = 'capture' WHERE id = ?",
        (enriched, row_id),
    )
    db.commit()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--window", type=int, default=48)
    args = parser.parse_args()

    db = sqlite3.connect(DB, timeout=10)
    unenriched = get_unenriched_captures(db, args.window)
    print(f"Unenriched captures: {len(unenriched)}")

    if not unenriched:
        print("Nothing to enrich.")
        db.close()
        return

    enriched_count = 0
    failed_count = 0

    for row_id, content, tweet_id in unenriched:
        author = extract_author_from_url(content)
        if args.dry_run:
            print(f"  [{row_id}] @{author} tweet {tweet_id} — would enrich")
            enriched_count += 1
            continue

        tweet_text = fetch_tweet(tweet_id)
        if tweet_text:
            enrich_capture(db, row_id, content, tweet_text, author)
            print(f"  [{row_id}] @{author}: {tweet_text[:60]}...")
            enriched_count += 1
        else:
            print(f"  [{row_id}] @{author} tweet {tweet_id} — fetch failed")
            failed_count += 1

    db.close()
    print(f"\nEnriched: {enriched_count} | Failed: {failed_count}")


if __name__ == "__main__":
    main()
