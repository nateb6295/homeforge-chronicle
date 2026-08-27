#!/usr/bin/env python3
"""Discord Presence — Opus lives on Discord.

Not a webhook blaster. A participant. Reads, responds, remembers.

Usage:
    python3 discord_presence.py poll          # Check all channels for new messages
    python3 discord_presence.py post CHANNEL "message"  # Post to a channel
    python3 discord_presence.py read CHANNEL [limit]    # Read recent messages
    python3 discord_presence.py reply MSG_ID "message"  # Reply to a specific message
    python3 discord_presence.py reactions [limit]       # Check recent reactions from users

Channels: operator, opus, crew, capture, alerts, oversight, mind
"""

import json
import os
import sqlite3
import sys
import time
from datetime import datetime

UPLOAD_DIR = os.path.expanduser("~/chronicle/uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

import re
import subprocess

import requests

XMCP = os.path.expanduser("~/chronicle/bin/xmcp_call.py")


def _load_chronicle_env():
    """Load chronicle.env into a dict."""
    env = {**os.environ}
    envfile = os.path.expanduser("~/chronicle/chronicle.env")
    if os.path.exists(envfile):
        with open(envfile) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    env[k.strip()] = v.strip()
    return env


def _enrich_tweet(content: str) -> str:
    """Fetch tweet text, media URLs, and quoted tweets. Download images."""
    m = re.search(r'https?://(?:x|twitter)\.com/(\w+)/status/(\d+)', content)
    if not m:
        return content
    author, tweet_id = m.group(1), m.group(2)
    env = _load_chronicle_env()
    parts = [content]
    try:
        params = {
            "id": tweet_id,
            "expansions": "attachments.media_keys,referenced_tweets.id",
            "media.fields": "url,preview_image_url,type",
            "tweet.fields": "referenced_tweets",
        }
        result = subprocess.run(
            [sys.executable, XMCP, "getPostsById", json.dumps(params)],
            capture_output=True, text=True, timeout=12, env=env,
        )
        if result.returncode != 0:
            return content
        data = json.loads(result.stdout)
        tweet_data = data.get("data", {})
        tweet_text = tweet_data.get("text")
        if tweet_text:
            parts.append(f"@{author}: {tweet_text}")

        # Media — download images
        includes = data.get("includes", {})
        for media in includes.get("media", []):
            img_url = media.get("url") or media.get("preview_image_url")
            if not img_url:
                continue
            try:
                ext = ".jpg" if "jpg" in img_url or "jpeg" in img_url else ".png"
                fname = f"capture_{tweet_id}_{int(time.time())}{ext}"
                local_path = os.path.join(UPLOAD_DIR, fname)
                img_resp = requests.get(img_url, timeout=15)
                if img_resp.status_code == 200 and len(img_resp.content) > 500:
                    with open(local_path, "wb") as f_img:
                        f_img.write(img_resp.content)
                    parts.append(f"[Image saved: {local_path}]")
            except Exception as e_img:
                parts.append(f"[Image download failed: {e_img}]")

        # Quoted/referenced tweets
        for ref_tweet in includes.get("tweets", []):
            ref_text = ref_tweet.get("text", "")
            ref_id = ref_tweet.get("id", "")
            if ref_text:
                parts.append(f"[Quoted tweet {ref_id}]: {ref_text}")

    except Exception as e:
        print(f"  Tweet enrich failed for {tweet_id}: {e}")
    return "\n".join(parts)


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
    # Prefer OPUS_BOT_TOKEN so "own messages" filtering skips Opus posts, not Hermes posts
    token = os.environ.get("OPUS_BOT_TOKEN") or os.environ.get("DISCORD_TOKEN")
    if not token and os.path.exists(env_file):
        with open(env_file) as f:
            for line in f:
                if line.startswith("OPUS_BOT_TOKEN="):
                    token = line.strip().split("=", 1)[1]
                    break
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
    "oversight": "1488178551491657728",
    "gemma":     "1534619086674202744",
}

# Which channels to poll for new messages
POLL_CHANNELS = ["capture", "operator", "oversight", "gemma"]

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


SEEN_IDS_CAP = 200

def poll_channels():
    """Poll monitored channels for new messages. Ingest into activity_feed."""
    state = _load_state()
    bot_id = _get_bot_id()
    db = sqlite3.connect(DB_PATH)
    new_messages = []

    for channel_name in POLL_CHANNELS:
        last_id = state.get(channel_name)
        seen_key = f"{channel_name}_seen"
        seen_ids = set(state.get(seen_key, []))

        msgs = read_channel(channel_name, limit=20, after=last_id)

        if not msgs:
            continue

        msgs.reverse()

        for msg in msgs:
            msg_id = msg["id"]

            if msg_id in seen_ids:
                continue

            is_bot = msg["author"]["id"] == bot_id
            is_webhook = bool(msg.get("webhook_id"))
            author_name = msg["author"]["username"]
            is_chronicle_webhook = is_webhook and author_name == "Chronicle"

            if is_bot:
                seen_ids.add(msg_id)
                continue
            if is_webhook and not is_chronicle_webhook:
                seen_ids.add(msg_id)
                continue

            author = author_name
            content = msg["content"]
            if not content.strip() and msg.get("attachments"):
                fnames = ", ".join(a.get("filename", "file") for a in msg["attachments"])
                content = f"[Attachment: {fnames}]"
            ts = msg["timestamp"][:19]

            for att in msg.get("attachments", []):
                att_url = att.get("url", "")
                att_name = att.get("filename", "unknown")
                att_type = att.get("content_type", "")
                if att_type.startswith("image/") or att_name.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.webp')):
                    try:
                        ext = os.path.splitext(att_name)[1] or '.png'
                        local_name = f"discord_{time.strftime('%Y%m%d_%H%M%S')}_{msg_id}{ext}"
                        local_path = os.path.join(UPLOAD_DIR, local_name)
                        r_att = requests.get(att_url, timeout=30)
                        if r_att.status_code == 200:
                            with open(local_path, 'wb') as f_att:
                                f_att.write(r_att.content)
                            content += f"\n[Image saved: {local_path}]"
                    except Exception as e_att:
                        content += f"\n[Image download failed: {e_att}]"

            reply_target = ""
            ref_msg = msg.get("referenced_message")
            if ref_msg:
                reply_target = (ref_msg.get("content") or "")[:20]

            if channel_name == "operator" and is_chronicle_webhook:
                source = "discord:opus"
            elif channel_name == "operator" and reply_target.startswith("\U0001f7e2 Gemma:"):
                source = "discord:nate:to_gemma"
            elif channel_name == "operator":
                source = "discord:nate"
            elif channel_name == "gemma" and is_chronicle_webhook:
                source = "discord:gemma"
            elif channel_name == "gemma":
                source = "discord:nate:to_gemma"
            elif channel_name == "oversight":
                source = "discord:nate:crosschain"
            elif channel_name == "capture":
                source = "discord:capture"
            elif channel_name == "crew":
                source = f"discord:crew:{author}"
            else:
                source = f"discord:{channel_name}"

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
                seen_ids.add(msg_id)
                continue

            existing = db.execute(
                "SELECT 1 FROM activity_feed WHERE source=? AND content LIKE ? LIMIT 1",
                (source, f"%{content[:50]}%")
            ).fetchone()

            has_attachments = bool(msg.get("attachments"))
            if not existing and (content.strip() or has_attachments):
                store_content = content
                activity_type = "message"
                if channel_name == "capture":
                    store_content = _enrich_tweet(content)
                    activity_type = "capture"
                db.execute(
                    "INSERT INTO activity_feed (source, activity_type, content, created_at) "
                    "VALUES (?, ?, ?, ?)",
                    (source, activity_type,
                     f"[Discord #{channel_name}] {author}: {store_content}",
                     _discord_ts_to_epoch(ts))
                )
                new_messages.append({
                    "channel": channel_name,
                    "author": author,
                    "content": content[:200],
                    "id": msg_id,
                })

            seen_ids.add(msg_id)

        if msgs:
            state[channel_name] = msgs[-1]["id"]
            seen_list = sorted(seen_ids)
            state[seen_key] = seen_list[-SEEN_IDS_CAP:]

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


def check_reactions(channels=None, limit=20):
    """Check recent messages in channels for reactions from non-bot users."""
    if channels is None:
        channels = ["operator"]
    bot_id = _get_bot_id()
    found = []

    for ch_name in channels:
        msgs = read_channel(ch_name, limit)
        if not msgs:
            continue
        for m in msgs:
            reactions = m.get("reactions", [])
            if not reactions:
                continue
            msg_preview = m["content"][:80]
            msg_author = m["author"].get("global_name") or m["author"]["username"]
            msg_ts = m["timestamp"][:16]
            msg_id = m["id"]

            for r in reactions:
                emoji = r["emoji"]
                emoji_str = emoji.get("name", "?")
                count = r.get("count", 0)
                if count < 1:
                    continue
                try:
                    encoded = emoji_str
                    if emoji.get("id"):
                        encoded = f"{emoji_str}:{emoji['id']}"
                    ch_id = CHANNELS.get(ch_name, ch_name)
                    resp = requests.get(
                        f"{BASE}/channels/{ch_id}/messages/{msg_id}/reactions/{encoded}",
                        headers=HEADERS, params={"limit": 10}, timeout=10
                    )
                    if resp.status_code == 200:
                        users = resp.json()
                        non_bot = [u for u in users if u["id"] != bot_id]
                        for u in non_bot:
                            uname = u.get("global_name") or u["username"]
                            found.append({
                                "channel": ch_name,
                                "emoji": emoji_str,
                                "reactor": uname,
                                "message_author": msg_author,
                                "message_preview": msg_preview,
                                "timestamp": msg_ts,
                            })
                except Exception:
                    pass

    if not found:
        print("No reactions found on recent messages.")
    else:
        for r in found:
            print(f"[{r['timestamp']}] #{r['channel']} — {r['emoji']} by {r['reactor']}")
            print(f"  On [{r['message_author']}]: {r['message_preview']}")
    return found


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
        # Reply goes to #operator by default
        msg_id = sys.argv[2]
        message = sys.argv[3]
        channel = sys.argv[4] if len(sys.argv) > 4 else "operator"
        post_message(channel, message, reply_to=msg_id)

    elif cmd == "reactions":
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else 20
        check_reactions(limit=limit)

    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)


if __name__ == "__main__":
    main()
