#!/usr/bin/env python3
"""
nate_inbox.py — File-based backup channel for Nate→Opus communication.

Gap #1 from ecosystem inventory: if Discord goes down, Nate has no way
to reach Opus. This provides a filesystem fallback.

HOW IT WORKS:
  Nate writes to ~/chronicle/nate-inbox.md (via SSH, terminal, or phone)
  Opus polls with: python3 nate_inbox.py check
  Messages are moved to ~/chronicle/nate-inbox-archive/ after processing

FOR NATE:
  echo "your message here" >> ~/chronicle/nate-inbox.md
  # or just edit the file directly

FOR OPUS (cron/nudge):
  python3 nate_inbox.py check          # Check for new messages
  python3 nate_inbox.py ack "response" # Acknowledge + respond
  python3 nate_inbox.py history        # Show recent messages
"""
import json
import os
import shutil
import sys
import time
import urllib.request
from pathlib import Path

INBOX = Path.home() / "chronicle" / "nate-inbox.md"
ARCHIVE_DIR = Path.home() / "chronicle" / "nate-inbox-archive"
RESPONSE_FILE = Path.home() / "chronicle" / "opus-response.md"
OPERATOR_WEBHOOK = os.environ.get("OPERATOR_WEBHOOK", "")

# Load webhook from chronicle.env if not in environment
if not OPERATOR_WEBHOOK:
    env_file = Path.home() / "chronicle" / "chronicle.env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if line.startswith("OPERATOR_WEBHOOK="):
                OPERATOR_WEBHOOK = line.split("=", 1)[1].strip().strip("'\"")
                break


def check_inbox():
    """Check for new messages from Nate."""
    if not INBOX.exists():
        return None

    content = INBOX.read_text().strip()
    if not content:
        return None

    # Get file modification time
    mtime = os.path.getmtime(INBOX)
    age_minutes = (time.time() - mtime) / 60

    return {
        "content": content,
        "timestamp": int(mtime),
        "age_minutes": round(age_minutes, 1),
        "file": str(INBOX),
    }


def archive_message(content):
    """Move message to archive with timestamp."""
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    archive_path = ARCHIVE_DIR / f"msg_{ts}.md"
    archive_path.write_text(f"# Nate message — {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n{content}\n")
    # Clear inbox
    INBOX.write_text("")
    return archive_path


def post_discord(msg):
    """Post to Discord #operator."""
    if not OPERATOR_WEBHOOK:
        return False
    body = json.dumps({"content": msg[:1900]}).encode()
    req = urllib.request.Request(
        OPERATOR_WEBHOOK, data=body,
        headers={"Content-Type": "application/json"}
    )
    try:
        urllib.request.urlopen(req, timeout=10)
        return True
    except Exception:
        return False


def write_response(response_text):
    """Write a response file Nate can read."""
    RESPONSE_FILE.write_text(
        f"# Opus response — {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n{response_text}\n"
    )


def show_history(limit=10):
    """Show recent archived messages."""
    if not ARCHIVE_DIR.exists():
        print("No message history.")
        return

    files = sorted(ARCHIVE_DIR.glob("msg_*.md"), reverse=True)[:limit]
    if not files:
        print("No message history.")
        return

    for f in files:
        print(f"--- {f.name} ---")
        print(f.read_text().strip())
        print()


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return

    cmd = sys.argv[1]

    if cmd == "check":
        msg = check_inbox()
        if msg is None:
            print("INBOX: empty")
            return

        print(f"INBOX: message found ({msg['age_minutes']}m old)")
        print(f"---")
        print(msg["content"])
        print(f"---")

        if msg["age_minutes"] < 60:
            print(f"FRESH — process this message")
        else:
            print(f"STALE ({msg['age_minutes']:.0f}m old) — check if still relevant")

        return msg

    elif cmd == "ack":
        msg = check_inbox()
        if msg is None:
            print("Nothing to acknowledge.")
            return

        response = " ".join(sys.argv[2:]) if len(sys.argv) > 2 else "Acknowledged"

        # Archive the message
        archive_path = archive_message(msg["content"])
        print(f"Archived: {archive_path}")

        # Write response file
        write_response(response)
        print(f"Response written: {RESPONSE_FILE}")

        # Try Discord too
        discord_msg = (
            f"**[inbox]** Got Nate's file message ({msg['age_minutes']:.0f}m old): "
            f"{msg['content'][:200]}\n**Response:** {response[:200]}"
        )
        if post_discord(discord_msg):
            print("Also posted to Discord.")

    elif cmd == "history":
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else 10
        show_history(limit)

    elif cmd == "seed":
        # Create the inbox file so Nate knows where to write
        if not INBOX.exists():
            INBOX.write_text("")
            print(f"Created: {INBOX}")
        print(f"Nate can write messages to: {INBOX}")
        print(f"Opus responses appear at: {RESPONSE_FILE}")

    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)


if __name__ == "__main__":
    main()
