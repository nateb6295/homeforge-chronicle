#!/usr/bin/env python3
"""pulse_capture_pull — atomic pull-and-log for PULSE-DAY capture-injection.

Bypasses the broken activity_feed-based dedup (the system stopped logging
discord:opus posts on 2026-04-24, so DB-side dedup couldn't see my recent
engagements). Instead, maintains a local state file of capture URLs I've
already engaged, filters against it, and appends atomically.

Usage:
  pulse_capture_pull.py        # pull a random un-engaged capture, print + log
  pulse_capture_pull.py --reset  # clear engagement log (for testing)
  pulse_capture_pull.py --status # show counts + recently-engaged URLs
"""
import argparse
import re
import sqlite3
import subprocess
import sys
from pathlib import Path

DB = "/mnt/hdd/chronicle-data/processed.db"
STATE_FILE = Path.home() / "chronicle" / "data" / "pulse_engaged_captures.txt"
STATE_FILE.parent.mkdir(parents=True, exist_ok=True)


def load_engaged_urls():
    if not STATE_FILE.is_file():
        return set()
    return {line.strip() for line in STATE_FILE.read_text().splitlines() if line.strip() and not line.startswith("#")}


def append_engaged(url):
    with STATE_FILE.open("a") as f:
        f.write(url + "\n")


def extract_url(content):
    m = re.search(r"https?://x\.com/[A-Za-z0-9_]+/status/\d+", content)
    if m:
        return m.group(0)
    m = re.search(r"https?://\S+", content)
    return m.group(0) if m else ""


def pull_capture():
    engaged = load_engaged_urls()
    con = sqlite3.connect(DB)
    cur = con.execute(
        "SELECT created_at, content FROM activity_feed "
        "WHERE source='operator:capture' "
        "AND created_at > strftime('%s','now')-86400 "
        "ORDER BY RANDOM()"
    )
    for ts, content in cur:
        url = extract_url(content)
        if url and url not in engaged:
            con.close()
            return ts, content, url
    con.close()
    # All recent captures engaged — return None to signal no fresh items
    return (None, None, None)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--reset", action="store_true")
    parser.add_argument("--status", action="store_true")
    args = parser.parse_args()

    if args.reset:
        STATE_FILE.write_text("")
        print(f"Cleared {STATE_FILE}")
        return

    if args.status:
        urls = load_engaged_urls()
        print(f"Engaged captures: {len(urls)}")
        for u in list(urls)[-10:]:
            print(f"  {u}")
        return

    ts, content, url = pull_capture()
    if not content:
        print("(queue exhausted — all recent captures already engaged)")
        sys.exit(2)

    # Convert sqlite-format ts (epoch) to readable
    import datetime
    ts_str = datetime.datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S")
    short = content[:400]
    if url:
        append_engaged(url)
    print(f"{ts_str} | [fresh]")
    print(short)
    if url:
        print(f"\n[engaged-url: {url}]")


if __name__ == "__main__":
    main()
