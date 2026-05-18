#!/usr/bin/env python3
"""Reaction Feedback — polls Discord for Nate's emoji reactions on Opus/Hermes posts.

When Nate reacts with 👍 or 🔥 to a post, this script:
1. Identifies who posted (Opus or Hermes)
2. Logs the reaction as positive feedback
3. Stores it in the activity feed for the relevant agent

Run periodically via cron or sentinel.

Usage:
  python3 reaction_feedback.py          # check and process reactions
  python3 reaction_feedback.py --dry-run  # show what would be processed
"""

import argparse
import json
import os
import sqlite3
import sys
import requests
from datetime import datetime
from pathlib import Path

DB = Path("/mnt/hdd/chronicle-data/processed.db")
STATE_FILE = Path(os.path.expanduser("~/chronicle/data/reaction_feedback_state.json"))
FEEDBACK_EMOJIS = {"👍", "🔥", "\U0001f44d", "\U0001f525"}
EMOJI_NAMES = {"👍": "thumbs_up", "🔥": "fire", "🔥": "fire", "👍": "thumbs_up"}
UNICODE_TO_DISCORD = {"\U0001f44d": "👍", "\U0001f525": "🔥"}

TOKEN = os.environ.get("OPUS_BOT_TOKEN", "")
NATE_ID = os.environ.get("NATE_DISCORD_ID", "")
OPUS_CHANNEL = os.environ.get("OPUS_CHANNEL_ID", "")
OPERATOR_CHANNEL = os.environ.get("OPERATOR_CHANNEL_ID", "")

HEADERS = {"Authorization": f"Bot {TOKEN}"}


def load_state():
    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"processed_reactions": []}


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def get_recent_messages(channel_id, limit=20):
    r = requests.get(
        f"https://discord.com/api/v10/channels/{channel_id}/messages?limit={limit}",
        headers=HEADERS,
    )
    if r.status_code == 200:
        return r.json()
    return []


def get_reaction_users(channel_id, message_id, emoji):
    encoded = requests.utils.quote(emoji)
    r = requests.get(
        f"https://discord.com/api/v10/channels/{channel_id}/messages/{message_id}/reactions/{encoded}",
        headers=HEADERS,
    )
    if r.status_code == 200:
        return r.json()
    return []


def identify_author(message):
    author = message.get("author", {})
    username = author.get("username", "").lower()
    bot = author.get("bot", False)
    content = message.get("content", "")

    if "Opus" in author.get("username", "") or (bot and "opus" in content.lower()[:50]):
        return "opus"
    if "Hermes" in author.get("username", "") or "Sprout" in author.get("username", ""):
        return "hermes"
    if "Chronicle" in author.get("username", ""):
        if content.startswith("**") and ("Build" in content or "DREAM" in content or "am —" in content):
            return "opus"
        return "system"
    return "unknown"


def log_feedback(message_id, channel_id, emoji, author_agent, content_preview):
    db = sqlite3.connect(str(DB))
    db.execute(
        "INSERT INTO activity_feed (source, activity_type, content, created_at) VALUES (?, ?, ?, ?)",
        (
            f"reaction:{author_agent}",
            "feedback",
            f"[Nate reacted {emoji} to {author_agent}] {content_preview[:200]}",
            int(datetime.utcnow().timestamp()),
        ),
    )
    db.commit()
    db.close()


def check_channel(channel_id, channel_name, state, dry_run=False):
    messages = get_recent_messages(channel_id)
    found = []

    for msg in messages:
        reactions = msg.get("reactions", [])
        if not reactions:
            continue

        msg_id = msg.get("id", "")
        content = msg.get("content", "")
        author_agent = identify_author(msg)

        for reaction in reactions:
            emoji_obj = reaction.get("emoji", {})
            emoji_name = emoji_obj.get("name", "")

            if emoji_name not in ("👍", "🔥"):
                continue

            reaction_key = f"{msg_id}:{emoji_name}"
            if reaction_key in state["processed_reactions"]:
                continue

            users = get_reaction_users(channel_id, msg_id, emoji_name)
            nate_reacted = any(u.get("id") == NATE_ID for u in users)

            if not nate_reacted:
                continue

            preview = content[:100].replace("\n", " ")
            found.append({
                "message_id": msg_id,
                "channel": channel_name,
                "emoji": emoji_name,
                "agent": author_agent,
                "preview": preview,
                "reaction_key": reaction_key,
            })

            if not dry_run:
                log_feedback(msg_id, channel_id, emoji_name, author_agent, content)
                state["processed_reactions"].append(reaction_key)
                # Keep state bounded
                if len(state["processed_reactions"]) > 500:
                    state["processed_reactions"] = state["processed_reactions"][-250:]

    return found


def main():
    parser = argparse.ArgumentParser(description="Reaction Feedback Watcher")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not TOKEN or not NATE_ID:
        print("ERROR: OPUS_BOT_TOKEN and NATE_DISCORD_ID required")
        return 1

    state = load_state()
    all_found = []

    for channel_id, name in [(OPUS_CHANNEL, "opus"), (OPERATOR_CHANNEL, "operator")]:
        if channel_id:
            found = check_channel(channel_id, name, state, dry_run=args.dry_run)
            all_found.extend(found)

    if all_found:
        for f in all_found:
            emoji = f["emoji"]
            agent = f["agent"]
            preview = f["preview"]
            print(f"  {emoji} → {agent}: {preview}")

        if not args.dry_run:
            save_state(state)
            print(f"\nProcessed {len(all_found)} reactions")
    else:
        print("No new reactions from Nate")

    return 0


if __name__ == "__main__":
    sys.exit(main())
