#!/usr/bin/env python3
"""Capture Vision — image-aware capture enrichment via Pixtral.

Scans recent captures for images (tweet media, Discord attachments, uploads),
sends them to Pixtral Large for visual description, and stores the description
alongside the capture in the activity_feed.

Usage:
  capture_vision.py              # enrich captures from last 6h
  capture_vision.py --window 24  # custom lookback in hours
  capture_vision.py --dry-run    # show what would be enriched
  capture_vision.py --id 12345   # enrich a specific activity_feed entry
"""

import argparse
import base64
import json
import os
import re
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

DB = "/mnt/hdd/chronicle-data/processed.db"
XMCP = os.path.expanduser("~/chronicle/bin/xmcp_call.py")
ENV_FILE = os.path.expanduser("~/chronicle/chronicle.env")
UPLOAD_DIR = os.path.expanduser("~/chronicle/uploads")
IMAGE_CACHE = os.path.expanduser("~/chronicle/data/image_cache")

PIXTRAL_URL = "https://inference-api.nousresearch.com/v1/chat/completions"
PIXTRAL_MODEL = "mistralai/pixtral-large-2411"

VISION_MARKER = "[VISION:"
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp"}


def load_env():
    if os.path.exists(ENV_FILE):
        with open(ENV_FILE) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip().strip("'\""))


def get_api_key():
    return os.environ.get("NOUS_API_KEY", "")


def describe_image(image_url, context="", api_key=None):
    """Send an image URL to Pixtral and get a description."""
    import requests

    if not api_key:
        api_key = get_api_key()
    if not api_key:
        return None

    prompt = "Describe this image concisely (2-3 sentences). Focus on: what's shown, who's visible, any text or data, and why it might be significant."
    if context:
        prompt += f"\n\nContext: this image was shared alongside: {context[:300]}"

    content = [
        {"type": "text", "text": prompt},
        {"type": "image_url", "image_url": {"url": image_url}},
    ]

    try:
        r = requests.post(
            PIXTRAL_URL,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "model": PIXTRAL_MODEL,
                "messages": [{"role": "user", "content": content}],
                "max_tokens": 300,
                "temperature": 0.3,
            },
            timeout=60,
        )
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"]
    except Exception as e:
        print(f"  Pixtral error: {e}", file=sys.stderr)
        return None


def describe_local_image(path, context="", api_key=None):
    """Encode a local image as base64 data URL and describe it."""
    import requests

    if not api_key:
        api_key = get_api_key()
    if not api_key:
        return None

    p = Path(path)
    if not p.exists():
        return None

    ext = p.suffix.lower()
    mime = {"jpg": "image/jpeg", "jpeg": "image/jpeg", "png": "image/png",
            "gif": "image/gif", "webp": "image/webp"}.get(ext.lstrip("."), "image/jpeg")

    b64 = base64.b64encode(p.read_bytes()).decode()
    data_url = f"data:{mime};base64,{b64}"

    return describe_image(data_url, context, api_key)


def extract_tweet_id(content):
    m = re.search(r'x\.com/\w+/status/(\d+)', content)
    return m.group(1) if m else None


def fetch_tweet_media(tweet_id):
    """Fetch media URLs from a tweet via xmcp."""
    try:
        env = {**os.environ}
        result = subprocess.run(
            [sys.executable, XMCP, "getPostsById", json.dumps({
                "id": tweet_id,
                "tweet.fields": "attachments",
                "expansions": "attachments.media_keys",
                "media.fields": "url,preview_image_url,type",
            })],
            capture_output=True, text=True, timeout=30, env=env,
        )
        if result.returncode != 0:
            return []
        data = json.loads(result.stdout)
        media = data.get("includes", {}).get("media", [])
        urls = []
        for m in media:
            if m.get("type") == "photo" and m.get("url"):
                urls.append(m["url"])
            elif m.get("preview_image_url"):
                urls.append(m["preview_image_url"])
        return urls
    except Exception as e:
        print(f"  xmcp error: {e}", file=sys.stderr)
        return []


def extract_discord_image_urls(content):
    """Extract image URLs from Discord message content."""
    urls = re.findall(r'https?://[^\s<>"]+\.(?:jpg|jpeg|png|gif|webp)(?:\?[^\s<>"]*)?', content, re.IGNORECASE)
    cdn = re.findall(r'https://cdn\.discordapp\.com/attachments/[^\s<>"]+', content)
    return list(set(urls + cdn))


def extract_upload_refs(content):
    """Check if content references a local upload."""
    uploads = []
    for m in re.finditer(r'uploads?/([^\s]+\.(?:jpg|jpeg|png|gif|webp))', content, re.IGNORECASE):
        path = os.path.join(UPLOAD_DIR, m.group(1))
        if os.path.exists(path):
            uploads.append(path)
    return uploads


def get_unenriched_captures(window_hours, specific_id=None):
    """Get captures that haven't been vision-enriched yet."""
    db = sqlite3.connect(DB, timeout=30)
    db.row_factory = sqlite3.Row

    if specific_id:
        rows = db.execute(
            "SELECT id, content, metadata FROM activity_feed WHERE id = ?",
            (specific_id,)
        ).fetchall()
    else:
        cutoff = int(time.time()) - (window_hours * 3600)
        rows = db.execute(
            "SELECT id, content, metadata FROM activity_feed "
            "WHERE created_at > ? AND (source LIKE '%capture%' OR source LIKE '%upload%') "
            "ORDER BY created_at DESC LIMIT 50",
            (cutoff,)
        ).fetchall()

    db.close()

    results = []
    for r in rows:
        meta = json.loads(r["metadata"]) if r["metadata"] else {}
        if meta.get("vision_description"):
            continue
        if VISION_MARKER in (r["content"] or ""):
            continue
        results.append(dict(r))
    return results


def save_vision(entry_id, description, image_urls):
    """Store vision description in the activity_feed metadata."""
    db = sqlite3.connect(DB, timeout=30)
    db.execute("PRAGMA busy_timeout = 30000")

    row = db.execute("SELECT metadata FROM activity_feed WHERE id = ?", (entry_id,)).fetchone()
    meta = json.loads(row[0]) if row and row[0] else {}
    meta["vision_description"] = description
    meta["vision_images"] = image_urls
    meta["vision_at"] = int(time.time())

    db.execute("UPDATE activity_feed SET metadata = ? WHERE id = ?", (json.dumps(meta), entry_id))
    db.commit()
    db.close()


def process_capture(entry, api_key, dry_run=False):
    """Process a single capture for images."""
    content = entry["content"] or ""
    entry_id = entry["id"]
    image_urls = []
    descriptions = []

    tweet_id = extract_tweet_id(content)
    if tweet_id:
        media = fetch_tweet_media(tweet_id)
        image_urls.extend(media)

    image_urls.extend(extract_discord_image_urls(content))

    local_files = extract_upload_refs(content)

    if not image_urls and not local_files:
        return None

    print(f"  Entry {entry_id}: {len(image_urls)} remote image(s), {len(local_files)} local file(s)")

    for url in image_urls[:3]:
        print(f"    Describing: {url[:80]}...")
        if dry_run:
            descriptions.append(f"[DRY RUN] Would describe {url}")
            continue
        desc = describe_image(url, context=content[:200], api_key=api_key)
        if desc:
            descriptions.append(desc)
            print(f"    → {desc[:100]}...")

    for path in local_files[:2]:
        print(f"    Describing local: {path}")
        if dry_run:
            descriptions.append(f"[DRY RUN] Would describe {path}")
            continue
        desc = describe_local_image(path, context=content[:200], api_key=api_key)
        if desc:
            descriptions.append(desc)
            print(f"    → {desc[:100]}...")

    if descriptions and not dry_run:
        combined = " | ".join(descriptions)
        save_vision(entry_id, combined, image_urls + local_files)

    return descriptions


def main():
    parser = argparse.ArgumentParser(description="Image-aware capture enrichment")
    parser.add_argument("--window", type=int, default=6, help="Lookback window in hours")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--id", type=int, help="Process a specific entry")
    args = parser.parse_args()

    load_env()
    api_key = get_api_key()
    if not api_key:
        print("No NOUS_API_KEY found", file=sys.stderr)
        sys.exit(1)

    captures = get_unenriched_captures(args.window, args.id)
    print(f"Found {len(captures)} unenriched capture(s) in last {args.window}h")

    enriched = 0
    for entry in captures:
        result = process_capture(entry, api_key, args.dry_run)
        if result:
            enriched += 1

    print(f"\nEnriched: {enriched}/{len(captures)}")
    return enriched


if __name__ == "__main__":
    main()
