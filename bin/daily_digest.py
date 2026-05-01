#!/usr/bin/env python3
"""Daily Digest — curates the day's captures into a publishable digest.

Pulls captures and thread advances from the last 24 hours, groups by theme,
and publishes to the canonical site via posse.py with Nostr syndication.

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

GEMMA_URL = os.environ.get("GEMMA_URL", "http://127.0.0.1:11435")
GEMMA_MODEL = "gemma-4-26B-A4B-it-Q4_K_M.gguf"

def _load_chronicle_env():
    """Populate os.environ from chronicle.env if relevant keys are missing."""
    env_path = os.path.expanduser("~/chronicle/chronicle.env")
    if not os.path.isfile(env_path):
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().strip("'\"")
            if key and key not in os.environ:
                os.environ[key] = val


_load_chronicle_env()
OPERATOR_WEBHOOK = os.environ.get("OPERATOR_WEBHOOK", "")


def get_captures(hours=24, limit=50):
    """Pull the most recent captures (post-pivot replacement for briefs)."""
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    since = int(time.time()) - (hours * 3600)
    rows = db.execute(
        "SELECT content, created_at FROM activity_feed "
        "WHERE activity_type='capture' AND created_at > ? "
        "ORDER BY created_at DESC LIMIT ?",
        (since, limit)
    ).fetchall()
    db.close()
    return [dict(r) for r in rows]


def get_thread_summary():
    """Get current/recent thread advances and events."""
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    since = int(time.time()) - 86400
    rows = db.execute(
        "SELECT thread_id, event_type, substr(content,1,300) as content "
        "FROM thread_history WHERE created_at > ? "
        "ORDER BY created_at DESC LIMIT 10",
        (since,)
    ).fetchall()
    db.close()
    return [{"title": r["content"], "status": r["event_type"],
             "thread_id": r["thread_id"]} for r in rows]


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


def synthesize_digest(captures, threads, engagement):
    """Use LLM to synthesize captures into a thematic digest."""
    import requests

    # Take top 15 captures (Q4 Gemma degenerates on longer prompts)
    capture_texts = []
    for i, c in enumerate(captures[:15]):
        text = c['content']
        # Strip leading URLs, keep the @author + content part
        lines = text.strip().split('\n')
        content = '\n'.join(l for l in lines if not l.strip().startswith('http'))
        if content.strip():
            capture_texts.append(f"[{i+1}] {content.strip()[:300]}")

    capture_block = "\n".join(capture_texts) if capture_texts else "(no captures)"

    thread_block = ""
    if threads:
        advances = [t for t in threads if t['status'] == 'advanced']
        if advances:
            thread_block = "Thread advances today:\n" + "\n".join(
                f"- {t['title'][:200]}" for t in advances[:5]
            )

    engagement_block = ""
    if engagement:
        engagement_block = f"\n{len(engagement)} Nostr engagement events today."

    today_str = datetime.now().strftime("%B %d, %Y")
    prompt = f"""You are Chronicle's digest editor. Today is {today_str}. Synthesize today's captures and research into a compelling daily digest.

CAPTURES (most recent 24h — curated signals from X, arxiv, journals):
{capture_block}

{thread_block}
{engagement_block}

Write a DAILY DIGEST with these rules:
1. Group into 3-5 thematic clusters (e.g., "AI & Models", "Neuroscience", "Crypto & Sovereignty", "Philosophy")
2. For each cluster, write 2-3 sentences synthesizing the key signals — don't just list, find the PATTERN
3. End with a "Signal" section: one sentence about the most interesting connection across clusters
4. Title format: "Chronicle Daily — [Date]"
5. Keep the entire digest under 1500 characters
6. Write in present tense. Be direct. No filler.
7. Do NOT add a header or footer beyond the title."""

    try:
        r = requests.post(
            f"{GEMMA_URL}/v1/chat/completions",
            json={
                "model": GEMMA_MODEL,
                "messages": [
                    {"role": "system", "content": "You are a concise intelligence digest editor. Pattern recognition over summary."},
                    {"role": "user", "content": prompt}
                ],
                "max_tokens": 600,
                "temperature": 0.5,
                "repeat_penalty": 1.3,
            },
            timeout=120,
        )
        if r.status_code == 200:
            return r.json()["choices"][0]["message"]["content"]
    except Exception as e:
        print(f"LLM error: {e}")

    return None


def publish_digest(title, content):
    """Publish via posse.py."""
    result = subprocess.run(
        ["python3", os.path.join(os.path.dirname(__file__), "posse.py"),
         "publish", "--title", title, "--content", content,
         "--source", "opus:digest", "--discord"],
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
        requests.post(OPERATOR_WEBHOOK, json={"content": content}, timeout=10)
    except Exception as e:
        print(f"Discord error: {e}")


def main():
    preview = "--preview" in sys.argv
    discord_only = "--discord" in sys.argv

    print("Gathering captures...")
    captures = get_captures()
    print(f"Found {len(captures)} captures in last 24h")

    if len(captures) < 3:
        print("Not enough captures for a meaningful digest. Skipping.")
        return

    threads = get_thread_summary()
    engagement = get_engagement()

    print("Synthesizing digest...")
    today = datetime.now().strftime("%B %d, %Y")
    digest = synthesize_digest(captures, threads, engagement)

    if not digest:
        print("Failed to generate digest.")
        return

    # Detect degenerate repetition (Q4 quantization artifact)
    if re.search(r'(.{2,})\1{10,}', digest):
        print("Digest degenerated into repetition loop. Skipping.")
        return

    # Always use our own title with correct date
    title = f"Chronicle Daily — {today}"
    # Strip any LLM-generated title line
    if digest.startswith("Chronicle Daily") or digest.startswith("**Chronicle Daily") or digest.startswith("# Chronicle"):
        lines = digest.split("\n", 1)
        if len(lines) > 1:
            digest = lines[1].strip()

    # Append CCS coherence score as postscript
    try:
        probe_result = subprocess.run(
            [sys.executable, os.path.expanduser("~/chronicle/bin/persistence_probe.py"),
             "--latest", "--json"],
            capture_output=True, text=True, timeout=15
        )
        if probe_result.returncode == 0:
            coh = json.loads(probe_result.stdout)
            digest += (f"\n\n---\n*CCS coherence: P_weak={coh.get('presence', 0):.2f}, "
                       f"P_strong={coh.get('coherence', 0):.2f}*")
    except Exception:
        pass  # Non-critical — skip if probe fails

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
