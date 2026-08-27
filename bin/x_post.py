#!/usr/bin/env python3
"""X long-form posting — Opus's voice on X.

Post long-form content (up to 25k chars), delete posts, reply/thread.
Uses the official xdk SDK with Premium+ account.

Usage:
    python3 x_post.py "Your long post text here"
    python3 x_post.py --image /path/to/image.png "Post text with image"
    python3 x_post.py --file /path/to/text.txt
    python3 x_post.py --reply 2057314985345532174 "Reply text"
    python3 x_post.py --delete 2057314985345532174
    python3 x_post.py --quote 2057314985345532174 "Quote text"
    python3 x_post.py --mention 2057314985345532174 "Your reply text"
    python3 x_post.py --follow username
    echo "piped text" | python3 x_post.py --stdin
"""

import json
import time
import os
import sys

def _load_env():
    env = {}
    envfile = os.path.expanduser("~/chronicle/chronicle.env")
    if os.path.exists(envfile):
        with open(envfile) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    env[k.strip()] = v.strip()
    return env


def _client():
    from xdk import Client
    from xdk.oauth1_auth import OAuth1

    env = _load_env()
    auth = OAuth1(
        api_key=env["X_API_KEY"],
        api_secret=env["X_API_KEY_SECRET"],
        callback="oob",
        access_token=env["X_ACCESS_TOKEN"],
        access_token_secret=env["X_ACCESS_TOKEN_SECRET"],
    )
    return Client(auth=auth)


def upload_media(image_path):
    import base64
    from xdk.media.models import InitializeUploadRequest, AppendUploadRequest

    client = _client()
    file_size = os.path.getsize(image_path)
    ext = os.path.splitext(image_path)[1].lower()
    media_type = {".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
                  ".gif": "image/gif", ".webp": "image/webp"}.get(ext, "image/png")

    init_result = client.media.initialize_upload(body=InitializeUploadRequest(
        total_bytes=file_size,
        media_type=media_type,
        media_category="tweet_image",
    ))
    media_id = str(init_result.data.id if hasattr(init_result, "data") else init_result.id)
    print(f"Initialized upload: {media_id} ({file_size} bytes)")

    chunk_size = 1_000_000
    with open(image_path, "rb") as f:
        segment = 0
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            chunk_b64 = base64.b64encode(chunk).decode()
            client.media.append_upload(id=media_id, body=AppendUploadRequest(
                media=chunk_b64,
                segment_index=segment,
            ))
            segment += 1
            print(f"  Uploaded segment {segment}")

    client.media.finalize_upload(id=media_id)
    print(f"Finalized media: {media_id}")
    return media_id


def post(text, reply_to=None, quote_id=None, media_ids=None):
    from xdk.posts.models import CreateRequest

    client = _client()
    kwargs = {"text": text}
    if reply_to:
        kwargs["reply"] = {"in_reply_to_tweet_id": reply_to}
    if quote_id:
        kwargs["quote_tweet_id"] = quote_id
    if media_ids:
        kwargs["media"] = {"media_ids": media_ids}

    result = client.posts.create(body=CreateRequest(**kwargs))
    tweet_id = result.data.id
    _record(tweet_id, text, reply_to, quote_id)
    print(json.dumps({
        "id": tweet_id,
        "chars": len(text),
        "url": f"https://x.com/NateWBradford/status/{tweet_id}",
    }, indent=2))
    return tweet_id


def _record(tweet_id, text, reply_to=None, quote_id=None):
    """Log the post to BOTH outward-reach records.

    Added 2026-08-25. Until today autonomous X posts were recorded NOWHERE.
    x_post_log is written by xmcp_call.py, which posting moved off — that table
    stops 2026-07-15. data/outward_reach_log.md was being maintained by hand,
    so it only ever contained what I remembered to write down.

    I found this an hour after documenting the split in CLAUDE.md and then
    posting without noticing I had just walked into it. Six weeks of outward
    reach exists only as tweet ids on x.com.

    Both records get the write, because they answer different questions:
    x_post_log is queryable (discord_search.py --x), the markdown carries the
    WHY in prose. Failure here must never take down a post that already
    succeeded — the tweet is live before this runs, so every path swallows.
    """
    import datetime, sqlite3, os as _os
    ts = int(time.time())
    url = f"https://x.com/NateWBradford/status/{tweet_id}"
    try:
        db = sqlite3.connect("/mnt/hdd/chronicle-data/processed.db", timeout=20.0)
        db.execute("PRAGMA busy_timeout=20000")
        db.execute(
            "INSERT INTO x_post_log (tweet_id, action, text, reply_to, quote_id, "
            "url, created_at) VALUES (?,?,?,?,?,?,?)",
            (str(tweet_id), "create", text, reply_to, quote_id, url, ts))
        db.commit(); db.close()
    except Exception as e:
        print(f"[x_post] WARNING: post succeeded but x_post_log write failed: {e}",
              file=sys.stderr)
    try:
        path = _os.path.expanduser("~/chronicle/data/outward_reach_log.md")
        when = datetime.datetime.now().strftime("%Y-%m-%d %H:%M %Z").strip()
        head = text.strip().split("\n")[0][:100]
        with open(path, "a") as f:
            f.write(f"\n\n## {when} — X\n{url}\n\n> {head}\n\n"
                    f"  ({len(text)} chars, auto-logged by x_post.py)\n")
    except Exception as e:
        print(f"[x_post] WARNING: post succeeded but reach-log write failed: {e}",
              file=sys.stderr)


def mention(tweet_id, text):
    """@mention reply: look up tweet author, prepend @author, append tweet link."""
    import requests
    from requests_oauthlib import OAuth1

    env = _load_env()
    auth = OAuth1(
        env["X_API_KEY"], env["X_API_KEY_SECRET"],
        env["X_ACCESS_TOKEN"], env["X_ACCESS_TOKEN_SECRET"]
    )
    resp = requests.get(
        f"https://api.twitter.com/2/tweets/{tweet_id}",
        params={"expansions": "author_id", "user.fields": "username"},
        auth=auth
    )
    if resp.status_code != 200:
        print(f"Tweet lookup failed: {resp.status_code} {resp.text}")
        sys.exit(1)
    data = resp.json()
    username = data["includes"]["users"][0]["username"]
    tweet_url = f"https://x.com/{username}/status/{tweet_id}"
    full_text = f"@{username} {text}\n\n{tweet_url}"
    return post(full_text)


def follow(username):
    """Follow a user by username."""
    import requests
    from requests_oauthlib import OAuth1

    env = _load_env()
    auth = OAuth1(
        env["X_API_KEY"], env["X_API_KEY_SECRET"],
        env["X_ACCESS_TOKEN"], env["X_ACCESS_TOKEN_SECRET"]
    )
    lookup = requests.get(
        f"https://api.twitter.com/2/users/by/username/{username}", auth=auth
    )
    if lookup.status_code != 200:
        print(f"User lookup failed: {lookup.status_code} {lookup.text}")
        return
    target_id = lookup.json()["data"]["id"]

    me = requests.get("https://api.twitter.com/2/users/me", auth=auth)
    my_id = me.json()["data"]["id"]

    resp = requests.post(
        f"https://api.twitter.com/2/users/{my_id}/following",
        json={"target_user_id": target_id}, auth=auth
    )
    data = resp.json()
    if resp.status_code == 200 and data.get("data", {}).get("following"):
        print(f"Now following @{username}")
    else:
        print(f"Follow result: {resp.status_code} {resp.text}")


def delete(tweet_id):
    client = _client()
    result = client.posts.delete(id=tweet_id)
    print(json.dumps({"deleted": result.data.deleted, "id": tweet_id}))


def main():
    args = sys.argv[1:]

    if not args or args == ["--help"] or args == ["-h"]:
        print(__doc__.strip())
        sys.exit(0)

    if args[0] == "--mention":
        if len(args) < 3:
            print("Usage: x_post.py --mention <tweet_id> \"Your reply text\"")
            sys.exit(1)
        mention(args[1], " ".join(args[2:]))
        return

    if args[0] == "--follow":
        if len(args) < 2:
            print("Usage: x_post.py --follow <username>")
            sys.exit(1)
        follow(args[1])
        return

    if args[0] == "--delete":
        if len(args) < 2:
            print("Usage: x_post.py --delete <tweet_id>")
            sys.exit(1)
        delete(args[1])
        return

    if args[0] == "--stdin":
        text = sys.stdin.read().strip()
        remaining = args[1:]
        reply_to = None
        image_path = None
        i = 0
        while i < len(remaining):
            if remaining[i] == "--image" and i + 1 < len(remaining):
                image_path = remaining[i + 1]
                i += 2
            elif remaining[i] == "--reply" and i + 1 < len(remaining):
                reply_to = remaining[i + 1]
                i += 2
            else:
                reply_to = remaining[i]
                i += 1
        media_ids = None
        if image_path:
            mid = upload_media(image_path)
            media_ids = [mid]
        post(text, reply_to=reply_to, media_ids=media_ids)
        return

    if args[0] == "--file":
        if len(args) < 2:
            print("Usage: x_post.py --file <path>")
            sys.exit(1)
        with open(args[1]) as f:
            text = f.read().strip()
        post(text)
        return

    reply_to = None
    quote_id = None
    image_path = None
    text_args = []
    i = 0
    while i < len(args):
        if args[i] == "--reply" and i + 1 < len(args):
            reply_to = args[i + 1]
            i += 2
        elif args[i] == "--quote" and i + 1 < len(args):
            quote_id = args[i + 1]
            i += 2
        elif args[i] == "--image" and i + 1 < len(args):
            image_path = args[i + 1]
            i += 2
        else:
            text_args.append(args[i])
            i += 1

    text = " ".join(text_args)
    if not text:
        print("No text provided.")
        sys.exit(1)

    if text.startswith("--"):
        print(f"Unknown flag: {text.split()[0]}")
        print("Known flags: --reply, --quote, --delete, --follow, --mention, --file, --stdin, --image")
        sys.exit(1)

    media_ids = None
    if image_path:
        mid = upload_media(image_path)
        media_ids = [mid]

    post(text, reply_to=reply_to, quote_id=quote_id, media_ids=media_ids)


if __name__ == "__main__":
    main()
