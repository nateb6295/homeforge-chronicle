#!/usr/bin/env python3
"""
x_get_full — fetch a tweet's full text including note_tweet (for >280 char
tweets). Wraps xmcp for the basic call but supplements with direct X API
for the note_tweet field which the xmcp wrapper doesn't expose.

Workaround for the limitation noted 2026-04-24: xmcp's getPostsById
returns truncated text for long-form tweets. Direct API has note_tweet.

Usage:
  python3 x_get_full.py <tweet_id>
"""
import json
import os
import sys
import urllib.request
from pathlib import Path


def load_env():
    env_path = Path.home() / "chronicle" / "chronicle.env"
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if "=" in line and not line.startswith("#"):
            k, _, v = line.partition("=")
            v = v.strip().strip('"').strip("'")
            if k.strip() and k.strip() not in os.environ:
                os.environ[k.strip()] = v


def main():
    if len(sys.argv) < 2:
        print("usage: x_get_full.py <tweet_id>")
        sys.exit(1)
    tweet_id = sys.argv[1]
    load_env()
    bearer = os.environ.get("X_BEARER_TOKEN")
    if not bearer:
        print("ERROR: X_BEARER_TOKEN not set")
        sys.exit(1)
    url = (f"https://api.x.com/2/tweets/{tweet_id}"
           f"?tweet.fields=text,note_tweet,author_id,created_at,referenced_tweets")
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {bearer}",
            "User-Agent": "chronicle-x-get-full/1.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            d = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        print(f"HTTP {e.code}: {body[:300]}", file=sys.stderr)
        sys.exit(1)

    data = d.get("data", {})
    note = data.get("note_tweet")
    out = {
        "id": tweet_id,
        "text": data.get("text", ""),
        "full_text": (note.get("text") if note else data.get("text", "")),
        "has_note": bool(note),
        "author_id": data.get("author_id"),
        "created_at": data.get("created_at"),
        "referenced_tweets": data.get("referenced_tweets"),
    }
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
