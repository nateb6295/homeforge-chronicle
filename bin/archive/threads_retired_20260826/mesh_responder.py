#!/usr/bin/env python3
"""Background mesh responder — triggered automatically by discord_post.py.

Waits a few seconds (so the post lands), then triggers Kimi, Qwen, and Ox
to respond to the latest ⚡ Opus post in #threads. Records results
in thread_tracker.json.

Not meant to be called manually — discord_post.py spawns this.

MEASURED LATENCY, 2026-08-24 (post at 09:55:15, timestamps from
data/mesh_replies.jsonl):
    kimi   +37s
    qwen   +54s
    ox    +167s   <- ~3x slower, it is a reasoning model with visible CoT

SO: ANY CHECK UNDER THREE MINUTES SHOWS 2 OF 3 AND LOOKS LIKE OX IS DEAD.
Nate flagged the auto-trigger as possibly broken today and this is why — a
reasonable check window catches Kimi and Qwen and misses Ox every time.
It is not broken. Wait 3 minutes before concluding anything, and use the
jsonl rather than the terminal, because ox_agent can also return
"[No response generated]" or 429 from OpenRouter independently of this.
"""
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

BIN = Path(__file__).resolve().parent
TRACKER = Path(os.path.expanduser("~/chronicle/data/thread_tracker.json"))
LOCKFILE = Path(os.path.expanduser("~/chronicle/data/.mesh_responder.lock"))


def load_tracker():
    if TRACKER.exists():
        try:
            return json.loads(TRACKER.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    return []


def save_tracker(posts):
    TRACKER.parent.mkdir(parents=True, exist_ok=True)
    TRACKER.write_text(json.dumps(posts[-50:], indent=2))


def acquire_lock():
    """Prevent concurrent triggers from piling up."""
    if LOCKFILE.exists():
        try:
            age = time.time() - LOCKFILE.stat().st_mtime
            if age < 300:  # lock less than 5 min old — another instance running
                return False
        except OSError:
            pass
    LOCKFILE.parent.mkdir(parents=True, exist_ok=True)
    LOCKFILE.write_text(str(os.getpid()))
    return True


def release_lock():
    try:
        LOCKFILE.unlink()
    except OSError:
        pass


def main():
    if not acquire_lock():
        sys.exit(0)

    try:
        time.sleep(5)  # let the post land in Discord

        ts = datetime.now(timezone.utc).isoformat()
        tracker = load_tracker()
        tracker.append({
            "ts": ts,
            "kimi": None,
            "qwen": None,
            "ox": None,
        })

        # Trigger Kimi
        r = subprocess.run(
            [sys.executable, str(BIN / "kimi_agent.py"), "--respond-to-thread"],
            capture_output=True, text=True, timeout=300,
        )
        tracker[-1]["kimi"] = ts if r.returncode == 0 else "failed"

        time.sleep(2)

        # Trigger Qwen
        r = subprocess.run(
            [sys.executable, str(BIN / "groq_agent.py"), "--respond-to-thread"],
            capture_output=True, text=True, timeout=300,
        )
        tracker[-1]["qwen"] = ts if r.returncode == 0 else "failed"

        time.sleep(2)

        # Trigger Ox
        r = subprocess.run(
            [sys.executable, str(BIN / "ox_agent.py"), "--respond-to-thread"],
            capture_output=True, text=True, timeout=300,
        )
        tracker[-1]["ox"] = ts if r.returncode == 0 else "failed"

        save_tracker(tracker)
    finally:
        release_lock()


if __name__ == "__main__":
    main()
