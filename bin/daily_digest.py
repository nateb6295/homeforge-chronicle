#!/usr/bin/env python3
"""Daily Digest — curates the day's best briefs into a publishable digest.

Pulls briefs from the last 24 hours, groups by theme, and publishes
to the canonical site via posse.py with Nostr syndication.

Usage:
    python3 daily_digest.py              # Generate and publish
    python3 daily_digest.py --preview    # Show digest without publishing
    python3 daily_digest.py --discord    # Post preview to Discord only
"""

import json
import os
import re
import sqlite3
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime

DB_PATH = os.environ.get("CHRONICLE_DB",
    "/mnt/hdd/chronicle-data/processed.db")

OLLAMA_URL = os.environ.get("INTERN_OLLAMA_URL", "http://localhost:11436")
MODEL = os.environ.get("INTERN_MODEL", "chronicle-deep")

OPUS_WEBHOOK = os.environ.get("OPUS_WEBHOOK",
    "https://discord.com/api/webhooks/1483843624926970057/2hZYzQQcyDEVD0A9UQqJsHlnV9D1m-6AfwNCnNWxGUC_8A0-ViX2dRVkBHF17_b2oDxJ")


def get_briefs(hours=24, limit=50):
    """Pull the most recent briefs."""
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    since = int(time.time()) - (hours * 3600)
    rows = db.execute(
        "SELECT content, created_at FROM activity_feed "
        "WHERE activity_type='brief' AND created_at > ? "
        "ORDER BY created_at DESC LIMIT ?",
        (since, limit)
    ).fetchall()
    db.close()
    return [dict(r) for r in rows]


def get_thread_summary():
    """Get current/recent thread info."""
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    rows = db.execute(
        "SELECT thread_id, event_type, substr(content,1,200) as content "
        "FROM thread_history WHERE event_type IN ('create','complete') "
        "ORDER BY created_at DESC LIMIT 5"
    ).fetchall()
    db.close()
    return [{"title": r["content"], "status": r["event_type"]} for r in rows]


def get_engagement():
    """Get any Nostr engagement from the day."""
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    since = int(time.time()) - 86400
    rows = db.execute(
        "SELECT content FROM activity_feed "
        "WHERE source='nostr:engagement' AND created_at > ?",
        (since,)
    ).fetchall()
    db.close()
    return [dict(r) for r in rows]


def synthesize_digest(briefs, threads, engagement):
    """Use LLM to synthesize briefs into a thematic digest."""
    import requests

    # Take top 30 briefs (most recent, which are most diverse)
    brief_texts = []
    for i, b in enumerate(briefs[:30]):
        brief_texts.append(f"[{i+1}] {b['content'][:300]}")

    brief_block = "\n".join(brief_texts)

    thread_block = ""
    if threads:
        thread_block = "Active threads: " + ", ".join(
            f"{t['title']} ({t['status']})" for t in threads
        )

    engagement_block = ""
    if engagement:
        engagement_block = f"\n{len(engagement)} Nostr engagement events today."

    today_str = datetime.now().strftime("%B %d, %Y")
    prompt = f"""You are Chronicle's digest editor. Today is {today_str}. Synthesize today's intelligence briefs into a compelling daily digest.

BRIEFS (most recent 24h):
{brief_block}

{thread_block}
{engagement_block}

Write a DAILY DIGEST with these rules:
1. Group the briefs into 3-5 thematic clusters (e.g., "AI & Models", "Geopolitics", "Neuroscience", "Crypto & Sovereignty")
2. For each cluster, write 2-3 sentences synthesizing the key signals — don't just list articles, find the PATTERN
3. End with a "Signal" section: one sentence about the most interesting connection across clusters
4. Title format: "Chronicle Daily — [Date]"
5. Keep the entire digest under 1500 characters
6. Write in present tense. Be direct. No filler.
7. Do NOT add a header or footer beyond the title."""

    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/chat",
            json={
                "model": MODEL,
                "messages": [
                    {"role": "system", "content": "You are a concise intelligence digest editor. Pattern recognition over summary."},
                    {"role": "user", "content": prompt}
                ],
                "stream": False,
                "options": {"num_predict": 600, "temperature": 0.5},
            },
            timeout=60,
        )
        if r.status_code == 200:
            return r.json().get("message", {}).get("content", "")
    except Exception as e:
        print(f"LLM error: {e}")

    return None


def publish_digest(title, content):
    """Publish via posse.py."""
    result = subprocess.run(
        ["python3", os.path.join(os.path.dirname(__file__), "posse.py"),
         "publish", "--title", title, "--content", content,
         "--source", "opus:digest", "--nostr", "--discord"],
        capture_output=True, text=True, timeout=60
    )
    print(result.stdout)
    if result.returncode != 0:
        print(f"posse.py error: {result.stderr}")
    return result.returncode == 0


def post_to_discord(content):
    """Post preview to Discord."""
    import requests
    # Truncate for Discord 2000 char limit
    if len(content) > 1900:
        content = content[:1897] + "..."
    try:
        requests.post(OPUS_WEBHOOK, json={"content": content}, timeout=10)
    except Exception as e:
        print(f"Discord error: {e}")


def main():
    preview = "--preview" in sys.argv
    discord_only = "--discord" in sys.argv

    print("Gathering briefs...")
    briefs = get_briefs()
    print(f"Found {len(briefs)} briefs in last 24h")

    if len(briefs) < 5:
        print("Not enough briefs for a meaningful digest. Skipping.")
        return

    threads = get_thread_summary()
    engagement = get_engagement()

    print("Synthesizing digest...")
    today = datetime.now().strftime("%B %d, %Y")
    digest = synthesize_digest(briefs, threads, engagement)

    if not digest:
        print("Failed to generate digest.")
        return

    # Always use our own title with correct date
    title = f"Chronicle Daily — {today}"
    # Strip any LLM-generated title line
    if digest.startswith("Chronicle Daily") or digest.startswith("**Chronicle Daily") or digest.startswith("# Chronicle"):
        lines = digest.split("\n", 1)
        if len(lines) > 1:
            digest = lines[1].strip()

    print(f"\n{'='*60}")
    print(f"TITLE: {title}")
    print(f"{'='*60}")
    print(digest)
    print(f"{'='*60}")
    print(f"Length: {len(digest)} chars")

    if preview:
        print("\n(Preview mode — not published)")
        return

    if discord_only:
        post_to_discord(f"**{title}**\n\n{digest}")
        print("Posted to Discord.")
        return

    # Publish via POSSE
    print("\nPublishing...")
    if publish_digest(title, digest):
        print("Digest published successfully.")
    else:
        print("Publish failed.")


if __name__ == "__main__":
    main()
