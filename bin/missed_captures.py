#!/usr/bin/env python3
"""missed_captures — list captures Nate sent that haven't been engaged or
explicitly skipped. Per Nate 2026-04-30 12:32: PULSE engagements should be
internal forcing-functions; the channel-visible output should be "captures
you haven't touched yet" rather than "here's what you touched."

Sources of "addressed":
  - ~/chronicle/data/pulse_engaged_captures.txt (engaged URLs, written by
    pulse_capture_pull.py)
  - ~/chronicle/data/pulse_skipped_captures.txt (deliberately skipped, written
    by skip-on-PULSE — file may not exist yet on first run)

Output: list of captures from last 24h whose URLs are in NEITHER file,
sorted oldest-first (longest-unaddressed surfaces first).

Usage:
  missed_captures.py                 # human-readable text
  missed_captures.py --discord       # post directly to #operator (one msg)
  missed_captures.py --hours N       # change window (default 24)
"""
import argparse
import re
import sqlite3
import subprocess
from datetime import datetime
from pathlib import Path

CHRONICLE = Path.home() / "chronicle"
DB = "/mnt/hdd/chronicle-data/processed.db"
ENGAGED_FILE = CHRONICLE / "data" / "pulse_engaged_captures.txt"
SKIPPED_FILE = CHRONICLE / "data" / "pulse_skipped_captures.txt"


def load_url_set(path):
    if not path.is_file():
        return set()
    return {line.strip() for line in path.read_text().splitlines() if line.strip() and not line.startswith("#")}


def extract_url(content):
    m = re.search(r"https?://x\.com/[A-Za-z0-9_]+/status/\d+", content)
    if m:
        return m.group(0)
    m = re.search(r"https?://\S+", content)
    return m.group(0) if m else ""


def extract_handle(content):
    """Prefer URL-derived handle (https://x.com/HANDLE/status/...). Fall back
    to display-name-derived handle (matches the @Display Name: prefix
    immediately after URL\\n)."""
    m = re.search(r"https?://x\.com/([A-Za-z0-9_]+)/status/", content)
    if m:
        return m.group(1)
    m = re.search(r"@([A-Za-z0-9_ ]+?):\s", content)
    return m.group(1).strip() if m else "?"


def extract_excerpt(content):
    """Skip the URL + @display-name prefix to get the actual tweet text."""
    # Capture format: "URL\n@Display Name (handle): tweet text"
    # Match @<anything-but-newline>: that's at the start of a line or after URL+newline
    after_handle = re.split(r"@[^\n:]+:\s*", content, maxsplit=1)
    if len(after_handle) > 1:
        text = after_handle[1].strip()
    else:
        text = content
    # Drop trailing URLs, [Quoting blocks, [Video: 0s], [Image: ...]
    text = re.sub(r"\[Quoting.*", "", text, flags=re.DOTALL).strip()
    text = re.sub(r"\[(Video|Image|Photo)[^\]]*\]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    # If the excerpt now starts with a URL (e.g., the original content had no @handle prefix), trim it
    text = re.sub(r"^https?://\S+\s*", "", text)
    return text[:140]


def get_missed(hours=24):
    cutoff = int(datetime.now().timestamp() - hours * 3600)
    engaged = load_url_set(ENGAGED_FILE)
    skipped = load_url_set(SKIPPED_FILE)
    addressed = engaged | skipped

    con = sqlite3.connect(DB)
    rows = con.execute(
        "SELECT created_at, content FROM activity_feed "
        "WHERE source = 'operator:capture' AND created_at > ? "
        "ORDER BY created_at ASC",
        (cutoff,),
    ).fetchall()
    con.close()

    missed = []
    for ts, content in rows:
        url = extract_url(content)
        if not url or url in addressed:
            continue
        age_h = (datetime.now().timestamp() - ts) / 3600
        missed.append({
            "url": url,
            "handle": extract_handle(content),
            "excerpt": extract_excerpt(content),
            "age_h": age_h,
            "ts": ts,
        })
    return missed


def format_text(missed, hours):
    if not missed:
        return f"📊 **Missed captures (last {hours}h):** none — all addressed (engaged or skipped). Nice."

    lines = [f"📊 **Missed captures (last {hours}h)** — {len(missed)} unaddressed, oldest-first:\n"]
    for m in missed:
        age = f"{m['age_h']:.1f}h" if m["age_h"] >= 1 else f"{int(m['age_h']*60)}m"
        lines.append(f"• `{age}` @{m['handle']}: {m['excerpt']}")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--discord", action="store_true")
    args = parser.parse_args()

    missed = get_missed(args.hours)
    msg = format_text(missed, args.hours)

    if args.discord:
        # Truncate if needed for Discord 2000-char limit
        if len(msg) >= 1950:
            cut_at = msg.rfind("\n", 0, 1900)
            msg = msg[:cut_at] + f"\n... (truncated, {len(missed)} total — see notes for full list)"
        subprocess.run(
            [str(CHRONICLE / "bin" / "post_operator.sh"), msg],
            timeout=15, check=False,
        )
    else:
        print(msg)


if __name__ == "__main__":
    main()
