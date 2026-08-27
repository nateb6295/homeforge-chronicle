#!/usr/bin/env python3
"""Check #threads for new mesh responses since last check.

Tracks last-seen message ID to avoid missing responses.
Usage:
    python3 bin/threads_check.py          # show new messages
    python3 bin/threads_check.py --all    # show last 10 regardless
"""

import json
import os
import subprocess
import sys
from pathlib import Path

STATE_FILE = Path.home() / "chronicle" / "data" / "threads_last_seen.json"
CHRONICLE = Path.home() / "chronicle"


def load_state():
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {"last_seen_id": None, "last_seen_ts": None}


def save_state(last_id, last_ts):
    STATE_FILE.write_text(json.dumps({
        "last_seen_id": last_id,
        "last_seen_ts": last_ts,
    }))


def fetch_threads(limit=25):
    out = subprocess.run(
        [sys.executable, str(CHRONICLE / "bin" / "discord_fetch.py"),
         "--threads", "--limit", str(limit)],
        capture_output=True, text=True, timeout=20,
        env={**os.environ},
    )
    if out.returncode != 0:
        print(f"Error fetching threads: {out.stderr[:200]}", file=sys.stderr)
        return []
    try:
        return json.loads(out.stdout)
    except json.JSONDecodeError:
        print(f"JSON decode error", file=sys.stderr)
        return []


def main():
    show_all = "--all" in sys.argv

    msgs = fetch_threads()
    if not msgs:
        print("No threads messages fetched.")
        return

    if show_all:
        for m in msgs[:10]:
            author = m["author"]
            ts = m["timestamp"][:19]
            content = m["content"][:200]
            marker = ""
            if "Kimi" in content:
                marker = " 🔬"
            elif "Qwen" in content:
                marker = " 🏮"
            print(f"  [{ts}] {author}{marker}: {content}")
        return

    state = load_state()
    last_seen = state["last_seen_id"]

    # Messages are newest-first from Discord API
    new_msgs = []
    for m in msgs:
        if m["id"] == last_seen:
            break
        new_msgs.append(m)

    # Update state to newest message
    if msgs:
        save_state(msgs[0]["id"], msgs[0]["timestamp"][:19])

    if not new_msgs:
        print("No new #threads messages.")
        return

    # Filter for mesh responses (Kimi/Qwen) vs my own posts
    mesh_responses = []
    my_posts = []
    for m in new_msgs:
        content = m["content"]
        if "Kimi" in content or "Qwen" in content:
            mesh_responses.append(m)
        # "Proculus" was this webhook's display name until 2026-08-24, when it was
        # renamed to "Opus" so the wire matches the mind. LoQwen had been reading
        # #threads and treating Proculus and Opus as two collaborators, building
        # arguments on the difference. Both names are accepted here so that posts
        # made BEFORE the rename stay attributable.
        elif m["author"] in ("Proculus", "Opus"):
            my_posts.append(m)

    print(f"{len(new_msgs)} new message(s) in #threads:")
    if mesh_responses:
        print(f"  {len(mesh_responses)} mesh response(s):")
        for m in mesh_responses:
            ts = m["timestamp"][:19]
            content = m["content"][:250]
            marker = "🔬 Kimi" if "Kimi" in content else "🏮 Qwen"
            print(f"    [{ts}] {marker}: {content}")
    if my_posts:
        print(f"  {len(my_posts)} own post(s):")
        for m in my_posts:
            ts = m["timestamp"][:19]
            content = m["content"][:150]
            print(f"    [{ts}] {content}")


if __name__ == "__main__":
    main()
