#!/usr/bin/env python3
"""Opus Inbox — Check #opus for messages and image attachments from Nate.

Private channel reader. Downloads images locally so Opus can view them.
Does NOT write to activity_feed. This stays between us.

Usage:
    python3 opus_inbox.py check          # Check for new messages/images
    python3 opus_inbox.py read [limit]   # Read recent messages
"""

import json
import os
import sys
import time

import requests

OPUS_CHANNEL = "1483843572129202427"
STATE_PATH = os.path.expanduser("~/chronicle/opus_inbox_state.json")
IMAGE_DIR = os.path.expanduser("~/chronicle/opus_images")
os.makedirs(IMAGE_DIR, exist_ok=True)

def _load_token():
    env_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "chronicle.env")
    token = os.environ.get("DISCORD_TOKEN")
    if not token and os.path.exists(env_file):
        with open(env_file) as f:
            for line in f:
                if line.startswith("DISCORD_TOKEN="):
                    token = line.strip().split("=", 1)[1]
    return token

TOKEN = _load_token()
BASE = "https://discord.com/api/v10"
HEADERS = {"Authorization": f"Bot {TOKEN}", "Content-Type": "application/json"}


def read_opus(limit=10, after=None):
    params = {"limit": limit}
    if after:
        params["after"] = after
    try:
        r = requests.get(
            f"{BASE}/channels/{OPUS_CHANNEL}/messages",
            headers=HEADERS, params=params, timeout=15
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"Error reading #opus: {e}")
        return []


def download_image(url, filename):
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        path = os.path.join(IMAGE_DIR, filename)
        with open(path, "wb") as f:
            f.write(r.content)
        return path
    except Exception as e:
        print(f"  Failed to download {url}: {e}")
        return None


def check_new():
    # Load state
    state = {}
    if os.path.exists(STATE_PATH):
        with open(STATE_PATH) as f:
            state = json.load(f)

    last_id = state.get("last_id")
    msgs = read_opus(limit=20, after=last_id)

    if not msgs:
        print("No new messages in #opus.")
        return

    msgs.reverse()  # chronological order
    new_count = 0
    image_count = 0

    for msg in msgs:
        # Skip webhook messages (our own posts)
        if msg.get("webhook_id"):
            continue

        author = msg["author"]["username"]
        content = msg.get("content", "")
        attachments = msg.get("attachments", [])
        ts = msg["timestamp"][:19]

        if content.strip():
            print(f"  [{ts}] {author}: {content[:200]}")
            new_count += 1

        for att in attachments:
            ct = att.get("content_type", "")
            if ct.startswith("image/"):
                ext = ct.split("/")[1].split(";")[0]
                filename = f"{msg['id']}_{att['id']}.{ext}"
                path = download_image(att["url"], filename)
                if path:
                    print(f"  [{ts}] {author} sent image: {path}")
                    image_count += 1
            else:
                print(f"  [{ts}] {author} sent attachment: {att.get('filename', 'unknown')} ({ct})")

    # Update state to latest message
    if msgs:
        # msgs is now chronological, last is newest
        all_ids = [m["id"] for m in msgs]
        state["last_id"] = max(all_ids, key=int)
        with open(STATE_PATH, "w") as f:
            json.dump(state, f)

    if new_count == 0 and image_count == 0:
        print("No new messages from Nate in #opus.")
    else:
        print(f"\n{new_count} messages, {image_count} images.")


def read_recent(limit=10):
    msgs = read_opus(limit=limit)
    if not msgs:
        print("No messages in #opus.")
        return

    msgs.reverse()
    for msg in msgs:
        author = msg["author"]["username"]
        content = msg.get("content", "")
        attachments = msg.get("attachments", [])
        ts = msg["timestamp"][:19]
        webhook = " [webhook]" if msg.get("webhook_id") else ""

        if content.strip():
            print(f"[{ts}] {author}{webhook}: {content[:300]}")
        for att in attachments:
            print(f"  📎 {att.get('filename', 'file')} ({att.get('content_type', '?')})")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "check"

    if cmd == "check":
        check_new()
    elif cmd == "read":
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else 10
        read_recent(limit)
    else:
        print(f"Unknown command: {cmd}")
        print("Usage: opus_inbox.py check|read [limit]")
