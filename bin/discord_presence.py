#!/usr/bin/env python3
"""Discord Presence — Opus lives on Discord.

Not a webhook blaster. A participant. Reads, responds, remembers.

Usage:
    python3 discord_presence.py poll          # Check all channels for new messages
    python3 discord_presence.py post CHANNEL "message"  # Post to a channel
    python3 discord_presence.py read CHANNEL [limit]    # Read recent messages
    python3 discord_presence.py reply MSG_ID "message"  # Reply to a specific message

Channels: operator, opus, crew, capture, alerts, oversight, mind
"""

import json
import os
import sqlite3
import sys
import time

import requests

DB_PATH = os.environ.get("CHRONICLE_DB",
    "/mnt/hdd/chronicle-data/processed.db")

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

# Channel map
CHANNELS = {
    "capture":   "1477863955266535535",
    "mind":      "1478214472786251837",
    "operator":  "1483843570292228213",
    "opus":      "1483843572129202427",
    "alerts":    "1487901536678838565",
    "crew":      "1487902154923704420",
    "oversight": "1488178551491657728",
}

# Which channels to poll for new messages
POLL_CHANNELS = ["capture", "crew"]  # opus + operator excluded per Nate — private, not ingested

# State file for tracking last-read message IDs
STATE_PATH = os.path.expanduser("~/chronicle/discord_presence_state.json")

# Bot's own user ID (to skip own messages)
BOT_USER_ID = None


def _get_bot_id():
    global BOT_USER_ID
    if BOT_USER_ID:
        return BOT_USER_ID
    try:
        r = requests.get(f"{BASE}/users/@me", headers=HEADERS, timeout=10)
        if r.status_code == 200:
            BOT_USER_ID = r.json()["id"]
    except Exception:
        pass
    return BOT_USER_ID


def _load_state():
    if os.path.exists(STATE_PATH):
        try:
            with open(STATE_PATH) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_state(state):
    with open(STATE_PATH, "w") as f:
        json.dump(state, f, indent=2)


def read_channel(channel_name, limit=10, after=None):
    """Read recent messages from a channel."""
    channel_id = CHANNELS.get(channel_name, channel_name)
    params = {"limit": limit}
    if after:
        params["after"] = after

    try:
        r = requests.get(
            f"{BASE}/channels/{channel_id}/messages",
            headers=HEADERS, params=params, timeout=15
        )
        if r.status_code == 200:
            return r.json()
        else:
            print(f"Error reading #{channel_name}: {r.status_code}")
            return []
    except Exception as e:
        print(f"Error reading #{channel_name}: {e}")
        return []


def post_message(channel_name, content, reply_to=None):
    """Post a message to a channel."""
    channel_id = CHANNELS.get(channel_name, channel_name)
    payload = {"content": content[:2000]}
    if reply_to:
        payload["message_reference"] = {"message_id": reply_to}

    try:
        r = requests.post(
            f"{BASE}/channels/{channel_id}/messages",
            headers=HEADERS, json=payload, timeout=15
        )
        if r.status_code == 200:
            data = r.json()
            print(f"Posted to #{channel_name} as {data['author']['username']}")
            return data
        else:
            print(f"Error posting to #{channel_name}: {r.status_code} {r.text[:200]}")
            return None
    except Exception as e:
        print(f"Error posting to #{channel_name}: {e}")
        return None


def poll_channels():
    """Poll monitored channels for new messages. Ingest into activity_feed."""
    state = _load_state()
    bot_id = _get_bot_id()
    db = sqlite3.connect(DB_PATH)
    new_messages = []

    for channel_name in POLL_CHANNELS:
        last_id = state.get(channel_name)
        msgs = read_channel(channel_name, limit=20, after=last_id)

        if not msgs:
            continue

        # Messages come newest-first, reverse for chronological
        msgs.reverse()

        for msg in msgs:
            # Skip bot's own messages
            if msg["author"]["id"] == bot_id:
                continue
            # Skip webhook messages (our own posts)
            if msg.get("webhook_id"):
                continue

            author = msg["author"]["username"]
            content = msg["content"]
            msg_id = msg["id"]
            ts = msg["timestamp"][:19]

            # Determine source tag
            if channel_name == "operator":
                source = "discord:nate"
            elif channel_name == "capture":
                source = "discord:capture"
            elif channel_name == "crew":
                source = f"discord:crew:{author}"
            else:
                source = f"discord:{channel_name}"

            # Check if already ingested
            existing = db.execute(
                "SELECT 1 FROM activity_feed WHERE source=? AND content LIKE ? LIMIT 1",
                (source, f"%{content[:50]}%")
            ).fetchone()

            if not existing and content.strip():
                db.execute(
                    "INSERT INTO activity_feed (source, activity_type, content, created_at) "
                    "VALUES (?, ?, ?, ?)",
                    (source, "message",
                     f"[Discord #{channel_name}] {author}: {content}",
                     int(time.time()))
                )
                new_messages.append({
                    "channel": channel_name,
                    "author": author,
                    "content": content[:200],
                    "id": msg_id,
                })

        # Update last-read ID
        if msgs:
            # newest message ID (last in reversed list)
            newest_id = msgs[-1]["id"]
            state[channel_name] = newest_id

    db.commit()
    db.close()
    _save_state(state)

    if new_messages:
        print(f"Ingested {len(new_messages)} new Discord messages:")
        for m in new_messages:
            print(f"  #{m['channel']} [{m['author']}]: {m['content'][:100]}")
    else:
        print("No new Discord messages.")

    return new_messages


def main():
    if not TOKEN:
        print("DISCORD_TOKEN not set")
        sys.exit(1)

    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "poll":
        poll_channels()

    elif cmd == "post":
        if len(sys.argv) < 4:
            print("Usage: discord_presence.py post CHANNEL \"message\"")
            sys.exit(1)
        channel = sys.argv[2]
        message = sys.argv[3]
        post_message(channel, message)

    elif cmd == "read":
        channel = sys.argv[2] if len(sys.argv) > 2 else "operator"
        limit = int(sys.argv[3]) if len(sys.argv) > 3 else 10
        msgs = read_channel(channel, limit)
        for m in reversed(msgs):
            author = m["author"]["username"]
            content = m["content"][:200]
            ts = m["timestamp"][:16]
            print(f"[{ts}] {author}: {content}")

    elif cmd == "reply":
        if len(sys.argv) < 4:
            print("Usage: discord_presence.py reply MSG_ID \"message\"")
            sys.exit(1)
        # Reply goes to #opus by default
        msg_id = sys.argv[2]
        message = sys.argv[3]
        channel = sys.argv[4] if len(sys.argv) > 4 else "operator"
        post_message(channel, message, reply_to=msg_id)

    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)


if __name__ == "__main__":
    main()
