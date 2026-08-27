#!/usr/bin/env python3
"""Thread response tracker — structural check-back for mesh responses.

Usage:
  thread_status.py                  # show unreviewed responses
  thread_status.py --all            # show all tracked threads
  thread_status.py review <index>   # mark a thread's responses as reviewed
  thread_status.py review all       # mark everything reviewed
  thread_status.py scan             # scan Discord #threads and update tracker

The tracker persists to ~/chronicle/data/thread_responses.json so it survives
context rotation. No memory required — just run 'thread_status.py' to see
what Kimi/Qwen said that you haven't read yet.
"""
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

TRACKER_PATH = Path(os.path.expanduser("~/chronicle/data/thread_responses.json"))
BIN = Path(__file__).resolve().parent


def load_tracker():
    if TRACKER_PATH.exists():
        try:
            return json.loads(TRACKER_PATH.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    return {"threads": [], "last_scan": None}


def save_tracker(data):
    TRACKER_PATH.parent.mkdir(parents=True, exist_ok=True)
    data["threads"] = data["threads"][-100:]
    TRACKER_PATH.write_text(json.dumps(data, indent=2))


def fetch_threads(limit=30):
    env = os.environ.copy()
    env_file = os.path.expanduser("~/chronicle/chronicle.env")
    if os.path.exists(env_file):
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    env[k.strip()] = v.strip().strip('"').strip("'")

    r = subprocess.run(
        [sys.executable, str(BIN / "discord_fetch.py"), "--threads", "--limit", str(limit)],
        capture_output=True, text=True, timeout=30, env=env,
    )
    if r.returncode != 0:
        return []
    try:
        return json.loads(r.stdout)
    except json.JSONDecodeError:
        return []


def scan():
    """Scan #threads and build/update the response tracker."""
    messages = fetch_threads(limit=40)
    if not messages:
        print("No messages fetched from #threads")
        return

    tracker = load_tracker()
    existing_ts = {t["posted_at"] for t in tracker["threads"]}

    opus_posts = []
    responses = []
    for m in messages:
        content = m.get("content", "")
        author = m.get("author", "")
        ts = m.get("timestamp", "")

        # "Proculus" was this webhook's display name until 2026-08-24, when it was
        # renamed to "Opus" so the wire matches the mind. LoQwen had been reading
        # #threads and treating Proculus and Opus as two collaborators, building
        # arguments on the difference. Both names are accepted here so that posts
        # made BEFORE the rename stay attributable.
        if author in ("Proculus", "Opus") and "⚡ Opus" in content[:200]:
            opus_posts.append({"ts": ts, "content": content, "id": m.get("id", "")})
        elif "🔬 Kimi" in content[:50]:
            responses.append({"ts": ts, "content": content, "agent": "kimi"})
        elif "🏮 Qwen" in content[:50]:
            responses.append({"ts": ts, "content": content, "agent": "qwen"})

    new_count = 0
    for post in opus_posts:
        if post["ts"] in existing_ts:
            continue

        subject = post["content"][:200].replace("\n", " ").strip()
        thread_entry = {
            "posted_at": post["ts"],
            "subject": subject,
            "responses": [],
            "reviewed": False,
        }

        for resp in responses:
            if resp["ts"] > post["ts"]:
                is_before_next = True
                for other_post in opus_posts:
                    if other_post["ts"] > post["ts"] and resp["ts"] > other_post["ts"]:
                        is_before_next = False
                        break
                if is_before_next:
                    snippet = resp["content"][:300].replace("\n", " ").strip()
                    thread_entry["responses"].append({
                        "agent": resp["agent"],
                        "at": resp["ts"],
                        "snippet": snippet,
                    })

        tracker["threads"].append(thread_entry)
        new_count += 1

    tracker["last_scan"] = datetime.now(timezone.utc).isoformat()
    save_tracker(tracker)
    print(f"Scanned. {new_count} new thread(s) added, {len(tracker['threads'])} total tracked.")


def show(show_all=False):
    """Show thread responses, filtered to unreviewed by default."""
    tracker = load_tracker()
    threads = tracker["threads"]
    if not show_all:
        threads = [t for t in threads if not t.get("reviewed")]

    if not threads:
        if show_all:
            print("No threads tracked yet. Run: thread_status.py scan")
        else:
            print("All caught up — no unreviewed thread responses.")
        return

    for i, t in enumerate(threads):
        idx = tracker["threads"].index(t)
        status = "✓" if t.get("reviewed") else "◯"
        resp_count = len(t.get("responses", []))
        posted = t["posted_at"][:19]
        subject = t.get("subject", "")[:120]
        print(f"\n[{idx}] {status} {posted}")
        print(f"    {subject}")

        if not t.get("responses"):
            print(f"    → No mesh responses recorded")
        else:
            for r in t["responses"]:
                agent = r["agent"].upper()
                snippet = r.get("snippet", "")[:200]
                print(f"    → {agent}: {snippet}")

    unreviewed = sum(1 for t in tracker["threads"] if not t.get("reviewed"))
    print(f"\n{'─' * 60}")
    print(f"{unreviewed} unreviewed / {len(tracker['threads'])} total")
    if unreviewed:
        print(f"Mark reviewed: thread_status.py review <index>")


def review(target):
    """Mark thread(s) as reviewed."""
    tracker = load_tracker()

    if target == "all":
        count = 0
        for t in tracker["threads"]:
            if not t.get("reviewed"):
                t["reviewed"] = True
                count += 1
        save_tracker(tracker)
        print(f"Marked {count} thread(s) as reviewed.")
        return

    try:
        idx = int(target)
        if 0 <= idx < len(tracker["threads"]):
            tracker["threads"][idx]["reviewed"] = True
            save_tracker(tracker)
            print(f"Thread [{idx}] marked as reviewed.")
        else:
            print(f"Index {idx} out of range (0-{len(tracker['threads'])-1})")
    except ValueError:
        print(f"Invalid index: {target}")


if __name__ == "__main__":
    args = sys.argv[1:]

    if not args:
        show()
    elif args[0] == "--all":
        show(show_all=True)
    elif args[0] == "scan":
        scan()
    elif args[0] == "review" and len(args) > 1:
        review(args[1])
    else:
        print(__doc__)
