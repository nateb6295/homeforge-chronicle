#!/usr/bin/env python3
"""Shared utilities for thread engagement agents.

Enriches thread post content with fetched URL data so mesh agents
can see behind links instead of saying "I can't access that."
"""

import json
import re
import subprocess
import sys
from pathlib import Path

TWEET_FETCH = Path.home() / "chronicle" / "bin" / "tweet_fetch.py"
WEB_SEARCH = Path.home() / "chronicle" / "bin" / "web_search.py"

TWEET_URL_RE = re.compile(r'https?://(?:x\.com|twitter\.com)/\w+/status/(\d+)')
GENERIC_URL_RE = re.compile(r'https?://[^\s<>"]+')


def fetch_tweet(tweet_id: str) -> str:
    try:
        result = subprocess.run(
            [sys.executable, str(TWEET_FETCH), tweet_id],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode == 0:
            data = json.loads(result.stdout)
            if isinstance(data, list) and data:
                tweet = data[0]
                text = tweet.get("text", "")
                author = tweet.get("author", "unknown")
                return f"[Tweet by @{author}]: {text}"
    except Exception:
        pass
    return ""


def enrich_post_content(post: dict) -> str:
    """Take a Discord message dict and return content enriched with URL data."""
    content = post.get("content", "")

    extras = []

    embeds = post.get("embeds", [])
    for embed in embeds:
        desc = embed.get("description", "")
        title = embed.get("title", "")
        if desc:
            extras.append(f"[Link preview]: {title + ': ' if title else ''}{desc}")

    tweet_matches = TWEET_URL_RE.findall(content)
    for tweet_id in tweet_matches:
        fetched = fetch_tweet(tweet_id)
        if fetched:
            extras.append(fetched)

    if extras:
        content = content + "\n\n--- Fetched content ---\n" + "\n".join(extras)

    return content
