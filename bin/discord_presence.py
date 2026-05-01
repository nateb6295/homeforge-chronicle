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
from datetime import datetime

import requests


def _discord_ts_to_epoch(ts_str: str) -> int:
    """Convert Discord ISO 8601 timestamp (UTC) to unix epoch.
    Discord sends UTC; if ts has been truncated to [:19] losing the TZ
    suffix, treat it as UTC explicitly (else fromisoformat returns naive
    datetime and .timestamp() applies LOCAL tz, producing a future-shifted
    epoch — root cause of the 2026-04-30 watchdog negative-age bug).
    Falls back to current time if parse fails."""
    try:
        from datetime import timezone
        # Replace Z with explicit +00:00; if no tz suffix, assume UTC.
        norm = ts_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(norm)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return int(time.time())

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
    "family":    "1490750142565974047",
}

# Which channels to poll for new messages
POLL_CHANNELS = ["capture", "crew", "family", "opus", "operator", "oversight"]  # opus: Sprout/bot only; operator + oversight: Nate direct line

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
            is_bot = msg["author"]["id"] == bot_id
            is_webhook = bool(msg.get("webhook_id"))
            author_name = msg["author"]["username"]
            # Chronicle webhook = Opus's own outbound posts (we want to track these)
            is_chronicle_webhook = is_webhook and author_name == "Chronicle"

            # In #opus: ONLY ingest Sprout/bot messages (Nate's are private)
            if channel_name == "opus":
                if not is_bot:
                    continue
            else:
                # All other channels: skip bot and non-Chronicle webhooks.
                # Chronicle webhook posts in #operator are Opus's outbound traffic
                # — track them so dedup queries + Mirror gather have ground truth.
                if is_bot or (is_webhook and not is_chronicle_webhook):
                    continue

            author = author_name
            content = msg["content"]
            msg_id = msg["id"]
            ts = msg["timestamp"][:19]

            # Determine source tag
            if channel_name == "operator" and is_chronicle_webhook:
                source = "discord:opus"  # Opus's own posts via Chronicle webhook
            elif channel_name == "operator":
                source = "discord:nate"
            elif channel_name == "oversight":
                source = "discord:nate:crosschain"  # Nate's direct line in crosschain sandbox
            elif channel_name == "capture":
                source = "discord:capture"
            elif channel_name == "crew":
                source = f"discord:crew:{author}"
            else:
                source = f"discord:{channel_name}"

            # Family channel → inject as voice (bypasses analysis pipeline)
            if channel_name == "family" and content.strip():
                existing_voice = db.execute(
                    "SELECT 1 FROM agent_voice WHERE agent='nate' AND content LIKE ? LIMIT 1",
                    (f"%{content[:50]}%",)
                ).fetchone()
                if not existing_voice:
                    db.execute(
                        "INSERT INTO agent_voice (agent, voice_type, content, context, created_at, status) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        ("nate", "for_family", content, f"discord:family:{msg_id}",
                         _discord_ts_to_epoch(ts), "unread")
                    )
                new_messages.append({
                    "channel": channel_name,
                    "author": author,
                    "content": content[:200],
                    "id": msg_id,
                })
                continue

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
                     _discord_ts_to_epoch(ts))
                )
                new_messages.append({
                    "channel": channel_name,
                    "author": author,
                    "content": content[:200],
                    "id": msg_id,
                })

        # Update last-read ID
        if msgs:
            # msgs was reversed (line 151) to chronological order, so msgs[-1] is newest
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
