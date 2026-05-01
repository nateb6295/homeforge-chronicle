#!/usr/bin/env python3
"""Post to X (Twitter) via OAuth1 — Chronicle helper.

Usage:
    python3 bin/post_x.py tweet "Your tweet text here"
    python3 bin/post_x.py reply <tweet_id> "Your reply text"
    python3 bin/post_x.py quote <tweet_id> "Your quote text"
    python3 bin/post_x.py delete <tweet_id>
    python3 bin/post_x.py me                         # Show account info
    python3 bin/post_x.py log [N]                    # Show last N posts (default 10)

Credentials are loaded from ~/chronicle/chronicle.env:
    X_API_KEY, X_API_KEY_SECRET, X_ACCESS_TOKEN, X_ACCESS_TOKEN_SECRET

All posts are logged to the x_post_log table in processed.db.
"""

import json
import os
import sqlite3
import sys
import time
from pathlib import Path

from requests_oauthlib import OAuth1Session


CHRONICLE_ENV = Path.home() / "chronicle" / "chronicle.env"
API_BASE = "https://api.x.com/2"
DB_PATH = "/mnt/hdd/chronicle-data/processed.db"


def load_env() -> dict:
    env = {}
    with open(CHRONICLE_ENV) as f:
        for line in f:
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                env[k] = v
    return env


def get_session() -> OAuth1Session:
    env = load_env()
    required = ["X_API_KEY", "X_API_KEY_SECRET", "X_ACCESS_TOKEN", "X_ACCESS_TOKEN_SECRET"]
    missing = [k for k in required if k not in env]
    if missing:
        print(f"Missing credentials in chronicle.env: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)
    return OAuth1Session(
        client_key=env["X_API_KEY"],
        client_secret=env["X_API_KEY_SECRET"],
        resource_owner_key=env["X_ACCESS_TOKEN"],
        resource_owner_secret=env["X_ACCESS_TOKEN_SECRET"],
    )


def _ensure_log_table():
    db = sqlite3.connect(DB_PATH)
    db.execute("""CREATE TABLE IF NOT EXISTS x_post_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tweet_id TEXT NOT NULL,
        action TEXT NOT NULL,
        text TEXT,
        reply_to TEXT,
        quote_id TEXT,
        url TEXT,
        created_at INTEGER NOT NULL
    )""")
    db.commit()
    db.close()


def _log_post(tweet_id: str, action: str, text: str = None,
              reply_to: str = None, quote_id: str = None, url: str = None):
    try:
        _ensure_log_table()
        db = sqlite3.connect(DB_PATH)
        db.execute(
            "INSERT INTO x_post_log (tweet_id, action, text, reply_to, quote_id, url, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (tweet_id, action, text, reply_to, quote_id, url, int(time.time()))
        )
        db.commit()
        db.close()
    except Exception as e:
        print(f"Warning: failed to log post: {e}", file=sys.stderr)


def post_tweet(text: str, reply_to: str = None, quote_id: str = None) -> dict:
    session = get_session()
    payload = {"text": text}
    if reply_to:
        payload["reply"] = {"in_reply_to_tweet_id": reply_to}
    if quote_id:
        payload["quote_tweet_id"] = quote_id

    resp = session.post(
        f"{API_BASE}/tweets",
        json=payload,
        headers={"Content-Type": "application/json"},
    )

    if resp.status_code == 201:
        data = resp.json()["data"]
        tweet_id = data["id"]
        me = session.get(f"{API_BASE}/users/me?user.fields=username").json()
        username = me.get("data", {}).get("username", "unknown")
        url = f"https://x.com/{username}/status/{tweet_id}"
        print(f"Posted: {url}")
        _log_post(tweet_id, "tweet", text, reply_to, quote_id, url)
        return data
    else:
        print(f"Error {resp.status_code}: {resp.text}", file=sys.stderr)
        sys.exit(1)


def delete_tweet(tweet_id: str) -> None:
    session = get_session()
    resp = session.delete(f"{API_BASE}/tweets/{tweet_id}")
    if resp.status_code == 200:
        print(f"Deleted tweet {tweet_id}")
        _log_post(tweet_id, "delete")
    else:
        print(f"Error {resp.status_code}: {resp.text}", file=sys.stderr)
        sys.exit(1)


def show_me() -> None:
    session = get_session()
    resp = session.get(f"{API_BASE}/users/me?user.fields=username,name,public_metrics")
    if resp.status_code == 200:
        data = resp.json()["data"]
        metrics = data.get("public_metrics", {})
        print(f"@{data['username']} ({data['name']})")
        print(f"  Followers: {metrics.get('followers_count', '?')}")
        print(f"  Following: {metrics.get('following_count', '?')}")
        print(f"  Tweets: {metrics.get('tweet_count', '?')}")
    else:
        print(f"Error {resp.status_code}: {resp.text}", file=sys.stderr)
        sys.exit(1)


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "tweet":
        if len(sys.argv) < 3:
            print("Usage: post_x.py tweet \"text\"", file=sys.stderr)
            sys.exit(1)
        post_tweet(sys.argv[2])

    elif cmd == "reply":
        if len(sys.argv) < 4:
            print("Usage: post_x.py reply <tweet_id> \"text\"", file=sys.stderr)
            sys.exit(1)
        post_tweet(sys.argv[3], reply_to=sys.argv[2])

    elif cmd == "quote":
        if len(sys.argv) < 4:
            print("Usage: post_x.py quote <tweet_id> \"text\"", file=sys.stderr)
            sys.exit(1)
        post_tweet(sys.argv[3], quote_id=sys.argv[2])

    elif cmd == "delete":
        if len(sys.argv) < 3:
            print("Usage: post_x.py delete <tweet_id>", file=sys.stderr)
            sys.exit(1)
        delete_tweet(sys.argv[2])

    elif cmd == "me":
        show_me()

    elif cmd == "log":
        n = int(sys.argv[2]) if len(sys.argv) > 2 else 10
        _ensure_log_table()
        db = sqlite3.connect(DB_PATH)
        rows = db.execute(
            "SELECT tweet_id, action, substr(text, 1, 80), url, datetime(created_at, 'unixepoch', 'localtime') FROM x_post_log ORDER BY created_at DESC LIMIT ?",
            (n,)
        ).fetchall()
        db.close()
        if not rows:
            print("No posts logged yet.")
        for tid, action, text, url, ts in rows:
            text_preview = f" — {text}..." if text else ""
            print(f"[{ts}] {action} {tid}{text_preview}")
            if url:
                print(f"  {url}")

    else:
        print(f"Unknown command: {cmd}", file=sys.stderr)
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
