#!/usr/bin/env python3
"""Capture watcher — near-realtime capture detection.

Polls #capture channel every 15 seconds. When a new capture arrives,
immediately ingests it to activity_feed and fires a webhook notification
to #operator so Opus can engage instantly.

Replaces cron-based capture alerts with event-like responsiveness.
"""

import json
import os
import re
import requests
import sqlite3
import subprocess
import sys
import time

DB_PATH = "/mnt/hdd/chronicle-data/processed.db"
STATE_PATH = os.path.expanduser("~/chronicle/capture_watch_state.json")
FLAG_PATH = os.path.expanduser("~/chronicle/capture_watch_flag.json")
UPLOAD_DIR = os.path.expanduser("~/chronicle/uploads")
POLL_INTERVAL = 15  # seconds

TOKEN = None
BASE = "https://discord.com/api/v10"
CAPTURE_CHANNEL_ID = "1477863955266535535"
BOT_USER_ID = None


def load_env():
    global TOKEN
    env_path = os.path.expanduser("~/chronicle/chronicle.env")
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line.startswith("OPUS_BOT_TOKEN="):
                    TOKEN = line.split("=", 1)[1].strip().strip('"').strip("'")


def headers():
    return {"Authorization": f"Bot {TOKEN}", "Content-Type": "application/json"}


def get_bot_id():
    global BOT_USER_ID
    if BOT_USER_ID:
        return BOT_USER_ID
    try:
        r = requests.get(f"{BASE}/users/@me", headers=headers(), timeout=10)
        if r.status_code == 200:
            BOT_USER_ID = r.json()["id"]
    except Exception:
        pass
    return BOT_USER_ID


def load_state():
    if os.path.exists(STATE_PATH):
        try:
            with open(STATE_PATH) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_state(state):
    with open(STATE_PATH, "w") as f:
        json.dump(state, f, indent=2)


def enrich_tweet(text):
    """Extract and enrich tweet URLs with full text and media."""
    tweet_pattern = r'https?://(?:x\.com|twitter\.com)/\w+/status/(\d+)'
    match = re.search(tweet_pattern, text)
    if not match:
        return text

    tweet_id = match.group(1)
    enriched = text

    try:
        r = subprocess.run(
            ["python3", os.path.expanduser("~/chronicle/bin/xmcp_call.py"),
             "getPostsById", json.dumps({"id": tweet_id})],
            capture_output=True, text=True, timeout=12,
            env={**os.environ, **_load_chronicle_env()},
        )
        if r.returncode == 0 and r.stdout.strip():
            data = json.loads(r.stdout)
            tweet_data = data.get("data", {})
            tweet_text = tweet_data.get("text", "")
            author = tweet_data.get("author", {}).get("userName", "")
            if tweet_text:
                enriched += f"\n@{author}: {tweet_text}" if author else f"\n{tweet_text}"

            media = tweet_data.get("media", [])
            for m in media:
                url = m.get("url") or m.get("preview_image_url", "")
                if url:
                    try:
                        ext = os.path.splitext(url.split("?")[0])[1] or ".jpg"
                        fname = f"capture_{tweet_id}_{int(time.time())}{ext}"
                        local_path = os.path.join(UPLOAD_DIR, fname)
                        r_img = requests.get(url, timeout=30)
                        if r_img.status_code == 200:
                            with open(local_path, "wb") as f:
                                f.write(r_img.content)
                            enriched += f"\n[Image saved: {local_path}]"
                    except Exception:
                        pass

            quoted = tweet_data.get("quoted_tweet", {})
            if quoted:
                qt = quoted.get("text", "")
                qa = quoted.get("author", {}).get("userName", "")
                if qt:
                    enriched += f"\n[Quoted tweet {quoted.get('id','')}]: {qt}" if not qa else f"\n[Quoted tweet {quoted.get('id','')}]: {qt}"
    except Exception:
        pass

    return enriched


def _load_chronicle_env():
    env = {}
    env_path = os.path.expanduser("~/chronicle/chronicle.env")
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if "=" in line and not line.startswith("#"):
                    k, v = line.split("=", 1)
                    env[k] = v.strip().strip('"').strip("'")
    return env


def read_messages(after=None, limit=10):
    params = {"limit": limit}
    if after:
        params["after"] = after
    try:
        r = requests.get(
            f"{BASE}/channels/{CAPTURE_CHANNEL_ID}/messages",
            headers=headers(), params=params, timeout=15
        )
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print(f"[capture-watch] Poll error: {e}", file=sys.stderr)
    return []


def discord_ts_to_epoch(ts_str):
    from datetime import datetime, timezone
    try:
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return int(time.time())


def write_flag(count):
    """Write a flag file that the terminal cron can check cheaply."""
    flag = {
        "new_captures": count,
        "timestamp": time.time(),
        "iso": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    with open(FLAG_PATH, "w") as f:
        json.dump(flag, f)


def notify_opus(count, details=""):
    """Notify Opus via chat endpoint + flag file. Survives context rotation."""
    print(f"[capture-watch] {count} new capture(s) ingested", flush=True)
    try:
        msg = f"[CAPTURE ALERT] {count} new capture(s) detected"
        if details:
            msg += f": {details}"
        requests.post(
            "http://localhost:8078/send",
            json={"author": "capture-watch", "content": msg},
            timeout=5
        )
    except Exception:
        pass


def process_messages(msgs, state, db):
    """Ingest new capture messages into activity_feed. Returns (new_messages, new_inserts, details)."""
    bot_id = get_bot_id()
    seen = set(state.get("seen", []))
    new_messages = 0
    new_inserts = 0
    capture_details = []

    msgs.reverse()

    for msg in msgs:
        msg_id = msg["id"]
        if msg_id in seen:
            continue

        if msg["author"]["id"] == bot_id:
            seen.add(msg_id)
            continue

        if bool(msg.get("webhook_id")):
            seen.add(msg_id)
            continue

        new_messages += 1
        author = msg["author"]["username"]
        content = msg["content"]
        ts = msg["timestamp"][:19]

        for att in msg.get("attachments", []):
            att_url = att.get("url", "")
            att_name = att.get("filename", "unknown")
            att_type = att.get("content_type", "")
            if att_type.startswith("image/") or att_name.lower().endswith(
                (".png", ".jpg", ".jpeg", ".gif", ".webp")
            ):
                try:
                    ext = os.path.splitext(att_name)[1] or ".png"
                    local_name = f"discord_{time.strftime('%Y%m%d_%H%M%S')}_{msg_id}{ext}"
                    local_path = os.path.join(UPLOAD_DIR, local_name)
                    r_att = requests.get(att_url, timeout=30)
                    if r_att.status_code == 200:
                        with open(local_path, "wb") as f_att:
                            f_att.write(r_att.content)
                        content += f"\n[Image saved: {local_path}]"
                except Exception as e_att:
                    content += f"\n[Image download failed: {e_att}]"

        store_content = enrich_tweet(content)

        existing = db.execute(
            "SELECT 1 FROM activity_feed WHERE source='discord:capture' AND content LIKE ? LIMIT 1",
            (f"%{content[:50]}%",)
        ).fetchone()

        detail = f"{author}: {content[:80]}"
        capture_details.append(detail)

        if not existing and content.strip():
            db.execute(
                "INSERT INTO activity_feed (source, activity_type, content, created_at) "
                "VALUES (?, ?, ?, ?)",
                ("discord:capture", "capture",
                 f"[Discord #capture] {author}: {store_content}",
                 discord_ts_to_epoch(ts))
            )
            new_inserts += 1

        print(f"[capture-watch] New capture from {detail}...")

        seen.add(msg_id)

    seen_list = sorted(seen)[-200:]
    state["seen"] = seen_list
    if msgs:
        state["last_id"] = msgs[-1]["id"]

    return new_messages, new_inserts, capture_details


def run():
    load_env()
    if not TOKEN:
        print("[capture-watch] ERROR: No OPUS_BOT_TOKEN found", file=sys.stderr)
        sys.exit(1)

    os.makedirs(UPLOAD_DIR, exist_ok=True)
    print(f"[capture-watch] Starting — polling every {POLL_INTERVAL}s")

    while True:
        try:
            state = load_state()
            last_id = state.get("last_id")

            msgs = read_messages(after=last_id)

            if msgs:
                db = sqlite3.connect(DB_PATH)
                new_messages, new_inserts, details = process_messages(msgs, state, db)
                db.commit()
                db.close()
                save_state(state)

                if new_messages > 0:
                    notify_opus(new_messages, "; ".join(details))
                    write_flag(new_messages)
        except Exception as e:
            print(f"[capture-watch] Error: {e}", file=sys.stderr)

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    run()
