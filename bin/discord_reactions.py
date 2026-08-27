#!/usr/bin/env python3
"""
discord_reactions.py — Poll Discord for emoji reactions on bot messages.

Nate's emoji reactions are lightweight feedback:
  👍 / ✅  = positive ("got it", "good post")
  👎 / ❌  = negative ("not great", "miss")
  🔥       = important / thread-worthy
  ❓       = want more on this
  👀       = seen / acknowledged

Polls #operator and #threads channels. Stores reactions in swarm_feedback table
so agents (Opus, Ada, Darby) can learn from them.

Usage:
  discord_reactions.py poll          — One-shot poll for new reactions
  discord_reactions.py recent        — Show recent reactions
  discord_reactions.py loop          — Continuous polling (for systemd)
"""

import os
import sys
import time
import json
import sqlite3
import requests

DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")
DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN", "")

# Channel IDs
CHANNELS = {
    "operator": os.environ.get("OPERATOR_CHANNEL_ID", "1483843570292228213"),
    # #opus was DELETED ~2026-08-02. This poller kept 404ing every 5 minutes
    # for 22 days while systemd reported "active (running)" and health_alert
    # reported green — liveness is not function. Swapped to #threads, which is
    # live and is where the mesh rounds happen. Found 2026-08-24 during the
    # first real inventory of what Chronicle actually runs.
    "threads": os.environ.get("THREADS_CHANNEL_ID", ""),
}

# Reaction → feedback mapping
REACTION_MAP = {
    "👍": ("positive", "good / got it"),
    "✅": ("positive", "confirmed / approved"),
    "🔥": ("important", "thread-worthy / significant"),
    "❓": ("question", "want more on this"),
    "👎": ("negative", "not great / miss"),
    "❌": ("negative", "wrong / reject"),
    "👀": ("acknowledged", "seen"),
    "😍": ("love", "love this / great work"),
    "🤩": ("love", "excited / impressive"),
    "💯": ("positive", "perfect / exactly right"),
    "🤔": ("question", "thinking / not sure about this"),
    # Unicode names Discord sometimes uses
    "thumbsup": ("positive", "good / got it"),
    "thumbsdown": ("negative", "not great / miss"),
    "white_check_mark": ("positive", "confirmed / approved"),
    "fire": ("important", "thread-worthy / significant"),
    "question": ("question", "want more on this"),
    "x": ("negative", "wrong / reject"),
    "eyes": ("acknowledged", "seen"),
    "heart_eyes": ("love", "love this / great work"),
    "star_struck": ("love", "excited / impressive"),
    "100": ("positive", "perfect / exactly right"),
    "thinking": ("question", "thinking / not sure about this"),
}

POLL_INTERVAL = int(os.environ.get("REACTION_POLL_INTERVAL", "300"))  # 5 min default

API_BASE = "https://discord.com/api/v10"


def log(msg):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_tables(db):
    db.execute("""
        CREATE TABLE IF NOT EXISTS discord_reactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel TEXT NOT NULL,
            message_id TEXT NOT NULL,
            message_preview TEXT DEFAULT '',
            message_author TEXT DEFAULT '',
            reactor TEXT NOT NULL,
            emoji TEXT NOT NULL,
            feedback_type TEXT NOT NULL,
            feedback_note TEXT DEFAULT '',
            created_at INTEGER NOT NULL,
            UNIQUE(channel, message_id, reactor, emoji)
        )
    """)
    db.commit()


def _headers():
    return {"Authorization": f"Bot {DISCORD_TOKEN}"}


def _get_recent_messages(channel_id, limit=25):
    """Fetch recent messages from a channel."""
    url = f"{API_BASE}/channels/{channel_id}/messages?limit={limit}"
    try:
        r = requests.get(url, headers=_headers(), timeout=10)
        if r.status_code == 200:
            return r.json()
        else:
            log(f"  Messages fetch failed ({r.status_code}): {r.text[:100]}")
            return []
    except Exception as e:
        log(f"  Messages fetch error: {e}")
        return []


def _get_reactions(channel_id, message_id, emoji):
    """Fetch users who reacted with a specific emoji."""
    # URL-encode the emoji for the API
    import urllib.parse
    encoded = urllib.parse.quote(emoji)
    url = f"{API_BASE}/channels/{channel_id}/messages/{message_id}/reactions/{encoded}"
    try:
        r = requests.get(url, headers=_headers(), timeout=10)
        if r.status_code == 200:
            return r.json()
        return []
    except Exception:
        return []


def poll_reactions():
    """Poll all monitored channels for reactions on recent messages."""
    if not DISCORD_TOKEN:
        log("ERROR: DISCORD_TOKEN not set")
        return 0

    db = get_db()
    ensure_tables(db)
    new_reactions = 0

    for channel_name, channel_id in CHANNELS.items():
        messages = _get_recent_messages(channel_id, limit=20)

        for msg in messages:
            # Skip messages with no reactions
            if not msg.get("reactions"):
                continue

            msg_id = msg["id"]
            msg_preview = msg.get("content", "")[:120]
            msg_author = msg.get("author", {}).get("username", "unknown")

            for reaction in msg["reactions"]:
                emoji_obj = reaction.get("emoji", {})
                emoji_name = emoji_obj.get("name", "")

                # Map to feedback type
                feedback_type = None
                feedback_note = ""
                if emoji_name in REACTION_MAP:
                    feedback_type, feedback_note = REACTION_MAP[emoji_name]
                else:
                    # Try matching unicode directly
                    for key, (ft, fn) in REACTION_MAP.items():
                        if emoji_name == key:
                            feedback_type, feedback_note = ft, fn
                            break

                if not feedback_type:
                    # Unknown emoji — store as generic acknowledgment
                    feedback_type = "other"
                    feedback_note = f"emoji: {emoji_name}"

                # Get who reacted
                users = _get_reactions(channel_id, msg_id, emoji_name)

                for user in users:
                    # Skip bot self-reactions
                    if user.get("bot"):
                        continue

                    username = user.get("username", "unknown")

                    try:
                        db.execute(
                            "INSERT OR IGNORE INTO discord_reactions "
                            "(channel, message_id, message_preview, message_author, "
                            "reactor, emoji, feedback_type, feedback_note, created_at) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (channel_name, msg_id, msg_preview, msg_author,
                             username, emoji_name, feedback_type, feedback_note,
                             int(time.time()))
                        )
                        if db.total_changes:
                            new_reactions += 1
                            log(f"  {emoji_name} on [{channel_name}] by {username}: "
                                f"{msg_preview[:60]}... → {feedback_type}")
                    except sqlite3.IntegrityError:
                        pass  # Already stored (UNIQUE constraint)

    db.commit()
    return new_reactions


def cmd_poll():
    n = poll_reactions()
    if n:
        log(f"Found {n} new reaction(s)")
    else:
        log("No new reactions")


def cmd_recent(args):
    limit = int(args[0]) if args else 20
    db = get_db()
    ensure_tables(db)
    rows = db.execute(
        "SELECT channel, emoji, feedback_type, reactor, message_author, "
        "message_preview, created_at FROM discord_reactions "
        "ORDER BY created_at DESC LIMIT ?", (limit,)
    ).fetchall()

    if not rows:
        print("No reactions stored yet.")
        return

    print(f"Recent reactions ({len(rows)}):\n")
    for r in rows:
        ts = time.strftime("%m-%d %H:%M", time.localtime(r["created_at"]))
        print(f"  {r['emoji']} [{r['feedback_type']}] ({ts}) #{r['channel']}")
        print(f"    {r['reactor']} reacted to {r['message_author']}: {r['message_preview'][:80]}")
        print()


def cmd_summary(args):
    """Feedback summary by agent/author — useful for tuning."""
    db = get_db()
    ensure_tables(db)

    # Since when?
    since = int(time.time()) - 86400 * 7  # last 7 days
    rows = db.execute(
        "SELECT message_author, feedback_type, COUNT(*) as cnt "
        "FROM discord_reactions WHERE created_at > ? "
        "GROUP BY message_author, feedback_type "
        "ORDER BY message_author, cnt DESC", (since,)
    ).fetchall()

    if not rows:
        print("No reactions in the last 7 days.")
        return

    print("Reaction summary (7 days):\n")
    current_author = None
    for r in rows:
        if r["message_author"] != current_author:
            current_author = r["message_author"]
            print(f"  [{current_author}]")
        print(f"    {r['feedback_type']}: {r['cnt']}")


def cmd_loop():
    """Continuous polling loop for systemd."""
    log(f"Discord reaction poller starting (interval: {POLL_INTERVAL}s)")
    log(f"Channels: {', '.join(f'#{k}' for k in CHANNELS)}")

    while True:
        try:
            n = poll_reactions()
            if n:
                log(f"Stored {n} new reaction(s)")
        except Exception as e:
            log(f"Poll error: {e}")

        time.sleep(POLL_INTERVAL)


COMMANDS = {
    "poll": lambda args: cmd_poll(),
    "recent": cmd_recent,
    "summary": cmd_summary,
    "loop": lambda args: cmd_loop(),
}

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        print("discord_reactions.py — Poll Discord for emoji feedback")
        print(f"Commands: {', '.join(COMMANDS.keys())}")
        sys.exit(1)

    COMMANDS[sys.argv[1]](sys.argv[2:])
