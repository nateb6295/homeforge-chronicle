#!/usr/bin/env python3
"""see.py — Opus's eyes. Download and display images from anywhere.

Usage:
  see.py                          # latest image from #operator
  see.py --capture                # latest image from #capture
  see.py --channel CHANNEL_ID     # latest image from any channel
  see.py --url URL                # any direct image URL
  see.py --last N                 # last N images (default 1)

Downloads to /tmp/opus_eyes/ and prints paths for the Read tool.
"""
import json
import os
import re
import sys
import time
import urllib.request

IMG_DIR = "/tmp/opus_eyes"
os.makedirs(IMG_DIR, exist_ok=True)

def load_env():
    for envfile in ["~/chronicle/chronicle.env", "~/.hermes/.env"]:
        path = os.path.expanduser(envfile)
        if os.path.exists(path):
            with open(path) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        k = k.strip()
                        v = v.strip()
                        if k == "DISCORD_BOT_TOKEN":
                            os.environ[k] = v
                        else:
                            os.environ.setdefault(k, v)

def discord_fetch(channel_id, limit=10):
    token = os.environ.get("DISCORD_BOT_TOKEN", "") or os.environ.get("DISCORD_TOKEN", "")
    if not token:
        print("ERROR: No DISCORD_BOT_TOKEN found", file=sys.stderr)
        return []
    url = f"https://discord.com/api/v10/channels/{channel_id}/messages?limit={limit}"
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bot {token}",
        "User-Agent": "DiscordBot (https://chronicle.local, 1.0)",
    })
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())

def download_image(url, filename=None):
    if not filename:
        filename = url.split("/")[-1].split("?")[0]
        if not re.search(r'\.(jpg|jpeg|png|gif|webp|bmp)', filename, re.I):
            filename += ".jpg"
    ts = int(time.time())
    dest = os.path.join(IMG_DIR, f"{ts}_{filename}")
    req = urllib.request.Request(url, headers={
        "User-Agent": "DiscordBot (https://chronicle.local, 1.0)",
    })
    with urllib.request.urlopen(req, timeout=15) as resp:
        with open(dest, "wb") as f:
            f.write(resp.read())
    return dest

def grab_discord_images(channel_id, count=1):
    msgs = discord_fetch(channel_id)
    paths = []
    for m in msgs:
        if not isinstance(m, dict):
            continue
        attachments = m.get("attachments", [])
        for a in attachments:
            ct = a.get("content_type", "")
            if ct.startswith("image/") or re.search(r'\.(jpg|jpeg|png|gif|webp)', a.get("filename", ""), re.I):
                url = a.get("url", "")
                if url:
                    author = m.get("author", {}).get("username", "unknown")
                    fname = a.get("filename", "image.jpg")
                    dest = download_image(url, f"{author}_{fname}")
                    paths.append(dest)
                    if len(paths) >= count:
                        return paths
        embeds = m.get("embeds", [])
        for e in embeds:
            for key in ("image", "thumbnail"):
                img = e.get(key) or {}
                url = img.get("url", "")
                if url and re.search(r'\.(jpg|jpeg|png|gif|webp)', url, re.I):
                    dest = download_image(url)
                    paths.append(dest)
                    if len(paths) >= count:
                        return paths
    return paths

def main():
    load_env()
    args = sys.argv[1:]

    count = 1
    if "--last" in args:
        idx = args.index("--last")
        count = int(args[idx + 1]) if idx + 1 < len(args) else 1
        args = args[:idx] + args[idx+2:]

    if "--url" in args:
        idx = args.index("--url")
        url = args[idx + 1] if idx + 1 < len(args) else ""
        if not url:
            print("ERROR: --url needs a URL", file=sys.stderr)
            sys.exit(1)
        dest = download_image(url)
        print(dest)
        return

    channel_map = {
        "--operator": os.environ.get("OPERATOR_CHANNEL_ID", "1483843570292228213"),
        "--capture": os.environ.get("CAPTURE_CHANNEL_ID", "1477863955266535535"),
        "--opus": os.environ.get("OPUS_CHANNEL_ID", "1483843572129202427"),
    }

    channel_id = channel_map["--operator"]
    if "--channel" in args:
        idx = args.index("--channel")
        channel_id = args[idx + 1] if idx + 1 < len(args) else channel_id
    else:
        for flag, cid in channel_map.items():
            if flag in args:
                channel_id = cid
                break

    paths = grab_discord_images(channel_id, count)
    if not paths:
        print("No images found in recent messages.")
        return

    for p in paths:
        print(p)

if __name__ == "__main__":
    main()
