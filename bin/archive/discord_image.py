#!/usr/bin/env python3
"""Post an image to Discord via bot token."""

import sys
import os
import requests

TOKEN = os.environ.get("DISCORD_TOKEN", "")
CHANNEL = os.environ.get("DISCORD_CHANNEL", "1478214472786251837")  # #mind

def post_image(image_path, message=""):
    url = f"https://discord.com/api/v10/channels/{CHANNEL}/messages"
    headers = {"Authorization": f"Bot {TOKEN}"}

    filename = os.path.basename(image_path)

    with open(image_path, "rb") as f:
        files = {"file": (filename, f, "image/png")}
        data = {"content": message} if message else {}
        resp = requests.post(url, headers=headers, data=data, files=files, timeout=30)

    if resp.status_code in (200, 201):
        print(f"Posted: {filename}")
        return True
    else:
        print(f"Error {resp.status_code}: {resp.text[:200]}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: discord_image.py <image_path> [message]")
        sys.exit(1)

    image_path = sys.argv[1]
    message = sys.argv[2] if len(sys.argv) > 2 else ""
    post_image(image_path, message)
