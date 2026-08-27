#!/usr/bin/env python3
"""discord_fetch — inbound Discord reader. Counterpart to discord_post.py.

Usage:
  discord_fetch.py --capture [--limit 5]     # fetch from #capture
  discord_fetch.py --operator [--limit 5]    # fetch from #operator
  discord_fetch.py --opus [--limit 5]        # fetch from #opus
  discord_fetch.py --channel-id 12345 [--limit 5]

Output: JSON array of messages with text, author, timestamp, embeds, attachments.
Auto-loads chronicle.env. Never guess env var names again.
"""
from __future__ import annotations
import argparse
import json
import os
import sys
import urllib.request
from pathlib import Path

ENV_FILE = Path.home() / "chronicle" / "chronicle.env"
USER_AGENT = "chronicle-opus/1.0"

CHANNEL_MAP = {
    "capture": "CAPTURE_CHANNEL_ID",
    "operator": "OPERATOR_CHANNEL_ID",
    "threads": "THREADS_CHANNEL_ID",
}


def _load_env() -> None:
    if not ENV_FILE.is_file():
        return
    for line in ENV_FILE.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip("'\"")
        if key and key not in os.environ:
            os.environ[key] = val


def fetch(channel: str = "capture", limit: int = 5, channel_id: str = "") -> list[dict]:
    _load_env()

    token = os.environ.get("OPUS_BOT_TOKEN", "")
    if not token:
        raise RuntimeError("OPUS_BOT_TOKEN not in env or chronicle.env")

    if not channel_id:
        env_key = CHANNEL_MAP.get(channel, "")
        if not env_key:
            raise ValueError(f"Unknown channel '{channel}'. Use: {list(CHANNEL_MAP)}")
        channel_id = os.environ.get(env_key, "")
        if not channel_id:
            raise RuntimeError(f"{env_key} not configured")

    url = f"https://discord.com/api/v10/channels/{channel_id}/messages?limit={limit}"
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bot {token}",
        "User-Agent": USER_AGENT,
    })
    with urllib.request.urlopen(req, timeout=15) as resp:
        raw = json.loads(resp.read())

    results = []
    for m in raw:
        entry = {
            "id": m["id"],
            "author": m["author"]["username"],
            "timestamp": m["timestamp"],
            "content": m.get("content", ""),
            "embeds": [],
            "attachments": [],
        }
        ref = m.get("message_reference")
        if ref:
            entry["reply_to"] = ref.get("message_id", "")
        ref_msg = m.get("referenced_message")
        if ref_msg:
            entry["reply_to_content"] = ref_msg.get("content", "")[:200]
            entry["reply_to_author"] = ref_msg.get("author", {}).get("username", "")
        elif ref and ref.get("message_id") and ref.get("channel_id"):
            try:
                ref_url = f"https://discord.com/api/v10/channels/{ref['channel_id']}/messages/{ref['message_id']}"
                ref_req = urllib.request.Request(ref_url, headers={
                    "Authorization": f"Bot {token}",
                    "User-Agent": USER_AGENT,
                })
                with urllib.request.urlopen(ref_req, timeout=10) as ref_resp:
                    ref_data = json.loads(ref_resp.read())
                entry["reply_to_content"] = ref_data.get("content", "")[:200]
                entry["reply_to_author"] = ref_data.get("author", {}).get("username", "")
            except Exception:
                pass
        for e in m.get("embeds", []):
            entry["embeds"].append({
                "url": e.get("url", ""),
                "title": e.get("title", ""),
                "description": e.get("description", ""),
                "image": (e.get("image") or {}).get("url", ""),
            })
        for a in m.get("attachments", []):
            att = {
                "url": a.get("url", ""),
                "filename": a.get("filename", ""),
                "content_type": a.get("content_type", ""),
                "size": a.get("size", 0),
            }
            entry["attachments"].append(att)
        results.append(entry)

    return results


IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".heic"}

def download_attachments(messages: list[dict], dest: str = "/tmp/discord_images") -> list[dict]:
    os.makedirs(dest, exist_ok=True)
    for m in messages:
        for att in m.get("attachments", []):
            fname = att.get("filename", "")
            ext = os.path.splitext(fname)[1].lower()
            if ext not in IMAGE_EXTS:
                continue
            local = os.path.join(dest, f"{m['id']}_{fname}")
            if os.path.exists(local):
                att["local_path"] = local
                continue
            try:
                req = urllib.request.Request(att["url"], headers={"User-Agent": USER_AGENT})
                with urllib.request.urlopen(req, timeout=15) as resp:
                    data = resp.read()
                with open(local, "wb") as f:
                    f.write(data)
                att["local_path"] = local
            except Exception:
                pass
    return messages


def _cli() -> int:
    p = argparse.ArgumentParser(description="Fetch Discord messages")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--capture", action="store_const", const="capture", dest="channel")
    g.add_argument("--operator", action="store_const", const="operator", dest="channel")
    g.add_argument("--threads", action="store_const", const="threads", dest="channel")
    p.set_defaults(channel="capture")
    p.add_argument("--channel-id", default="", help="Direct channel ID override")
    p.add_argument("--limit", type=int, default=25)
    p.add_argument("--compact", action="store_true", help="One line per message")
    p.add_argument("--download", action="store_true", help="Download image attachments to /tmp/discord_images")
    args = p.parse_args()

    msgs = fetch(channel=args.channel, limit=args.limit, channel_id=args.channel_id)
    if args.download:
        msgs = download_attachments(msgs)

    if args.compact:
        for m in msgs:
            embed_urls = " ".join(e["url"] for e in m["embeds"] if e["url"])
            reply = ""
            if m.get("reply_to_content"):
                reply = f' [replying to {m.get("reply_to_author", "?")}:"{m["reply_to_content"][:80]}"]'
            print(f'[{m["timestamp"][:16]}] {m["author"]}: {m["content"][:200]}{reply} {embed_urls}'.strip())
    else:
        json.dump(msgs, sys.stdout, indent=2)
        print()

    return 0


if __name__ == "__main__":
    sys.exit(_cli())
