#!/usr/bin/env python3
"""Thread tracker — robust tracking of #threads posts and mesh responses.

Usage:
  thread_tracker.py status          # all tracked threads + response state
  thread_tracker.py pending         # Opus posts waiting for mesh response
  thread_tracker.py new             # mesh responses I haven't engaged with yet
  thread_tracker.py ack [msg_id]    # mark a thread's responses as seen
  thread_tracker.py ack-all         # mark all current responses as seen
  thread_tracker.py refresh         # fetch latest from Discord, update state
  thread_tracker.py summary         # one-line summary for rhythm pulse

Reads #threads channel directly. Matches Kimi/Qwen responses to Opus posts
by message ordering. State persists in data/thread_state.json.
"""
import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

BIN = Path(__file__).resolve().parent
CHRONICLE = BIN.parent
STATE_FILE = CHRONICLE / "data" / "thread_state.json"
DISCORD_FETCH = BIN / "discord_fetch.py"
ENV_FILE = CHRONICLE / "chronicle.env"


def _load_env():
    if not ENV_FILE.is_file():
        return
    for line in ENV_FILE.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        os.environ.setdefault(key.strip(), val.strip().strip("'\""))


def _fetch_threads(limit=100):
    """Fetch recent messages from #threads channel."""
    _load_env()
    channel_id = os.environ.get("THREADS_CHANNEL_ID", "")
    if not channel_id:
        print("THREADS_CHANNEL_ID not set", file=sys.stderr)
        return []
    result = subprocess.run(
        [sys.executable, str(DISCORD_FETCH), "--channel-id", channel_id, "--limit", str(limit)],
        capture_output=True, text=True, timeout=15,
    )
    if result.returncode != 0:
        print(f"discord_fetch failed: {result.stderr[:200]}", file=sys.stderr)
        return []
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return []


def _classify_message(msg):
    """Classify a #threads message as opus/kimi/qwen/other."""
    content = msg.get("content", "")
    author = msg.get("author", "")
    if "⚡ Opus" in content[:200]:
        return "opus"
    if "🔬 Kimi" in content[:200] or "Kimi" in content[:50]:
        return "kimi"
    if "🏮 Qwen" in content[:200] or "Qwen" in content[:50]:
        return "qwen"
    if author == "Chronicle" or author == "Opus":
        return "opus"
    return "other"


def _extract_topic(content):
    """Extract a short topic from an Opus post."""
    content = content.replace("**⚡ Opus:**", "").replace("⚡ Opus:", "").strip()
    lines = [l.strip() for l in content.split("\n") if l.strip()]
    if not lines:
        return "(empty)"
    first = lines[0]
    for prefix in ["**", "##", "# "]:
        first = first.lstrip(prefix)
    first = first.rstrip("*").strip()
    if len(first) > 80:
        first = first[:77] + "..."
    return first


def _load_state():
    if STATE_FILE.exists():
        try:
            data = json.loads(STATE_FILE.read_text())
            if isinstance(data, dict) and "threads" in data:
                return data
            if isinstance(data, list):
                return {"threads": data, "last_refresh": None}
        except (json.JSONDecodeError, OSError):
            pass
    return {"threads": [], "last_refresh": None}


def _save_state(state):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    state["threads"] = state["threads"][-100:]
    STATE_FILE.write_text(json.dumps(state, indent=2))


def refresh():
    """Fetch #threads and rebuild state from Discord messages."""
    msgs = _fetch_threads(limit=50)
    if not msgs:
        print("No messages fetched from #threads.")
        return _load_state()

    state = _load_state()
    known_ids = {t["msg_id"] for t in state["threads"]}

    opus_posts = []
    responses = []

    for msg in reversed(msgs):
        msg_type = _classify_message(msg)
        msg_id = msg.get("id", "")
        ts = msg.get("timestamp", "")
        content = msg.get("content", "")

        if msg_type == "opus":
            opus_posts.append({
                "msg_id": msg_id,
                "timestamp": ts,
                "topic": _extract_topic(content),
                "snippet": content[:200],
                "kimi": None,
                "qwen": None,
            })
        elif msg_type in ("kimi", "qwen"):
            responses.append({
                "type": msg_type,
                "msg_id": msg_id,
                "timestamp": ts,
                "snippet": content[:150],
            })

    # Match responses to ALL Opus posts between the previous response of that
    # type and this response. Mesh agents respond to the thread conversation,
    # not to individual posts — so a single Kimi response covers all Opus posts
    # since the last Kimi response.
    for resp_type in ("kimi", "qwen"):
        typed_resps = [r for r in responses if r["type"] == resp_type]
        typed_resps.sort(key=lambda r: r["timestamp"])
        prev_ts = ""
        for resp in typed_resps:
            resp_ts = resp["timestamp"]
            for op in opus_posts:
                if prev_ts < op["timestamp"] <= resp_ts:
                    if not op.get(resp_type):
                        op[resp_type] = resp["snippet"]
                        op[f"{resp_type}_ts"] = resp["timestamp"]
            prev_ts = resp_ts

    for op in opus_posts:
        if op["msg_id"] not in known_ids:
            state["threads"].append(op)
        else:
            for existing in state["threads"]:
                if existing["msg_id"] == op["msg_id"]:
                    if op["kimi"] and not existing.get("kimi"):
                        existing["kimi"] = op["kimi"]
                        existing["kimi_ts"] = op.get("kimi_ts", "")
                    if op["qwen"] and not existing.get("qwen"):
                        existing["qwen"] = op["qwen"]
                        existing["qwen_ts"] = op.get("qwen_ts", "")

    state["last_refresh"] = datetime.now(timezone.utc).isoformat()
    _save_state(state)
    return state


def cmd_status(state=None):
    """Show all tracked threads with response state."""
    if state is None:
        state = _load_state()
    threads = state.get("threads", [])
    if not threads:
        print("No tracked threads.")
        return

    print(f"Thread Tracker — {len(threads)} posts tracked")
    print(f"Last refresh: {state.get('last_refresh', 'never')}")
    print()

    for t in threads[-20:]:
        ts = t.get("timestamp", "?")[:16]
        topic = t.get("topic", "?")
        k = "✓" if t.get("kimi") else "✗"
        q = "✓" if t.get("qwen") else "✗"
        print(f"  [{ts}] Kimi:{k} Qwen:{q} — {topic}")


def cmd_pending(state=None):
    """Show Opus posts missing mesh responses (last 48h only)."""
    if state is None:
        state = _load_state()
    threads = state.get("threads", [])
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()

    pending = [
        t for t in threads
        if (not t.get("kimi") or not t.get("qwen"))
        and t.get("timestamp", "") > cutoff
    ]
    if not pending:
        print("All recent threads have mesh responses.")
        return

    print(f"{len(pending)} thread(s) missing responses (last 48h):")
    for t in pending:
        ts = t.get("timestamp", "?")[:16]
        topic = t.get("topic", "?")
        missing = []
        if not t.get("kimi"):
            missing.append("Kimi")
        if not t.get("qwen"):
            missing.append("Qwen")
        print(f"  [{ts}] Missing: {', '.join(missing)} — {topic}")


def _has_unseen_responses(t):
    """Check if a thread has mesh responses that haven't been acknowledged.
    Auto-expires after 4 hours — stale responses stop counting as 'new'."""
    kimi_ts = t.get("kimi_ts", "")
    qwen_ts = t.get("qwen_ts", "")
    if not kimi_ts and not qwen_ts:
        return False
    latest_resp = max(kimi_ts, qwen_ts)
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=4)).isoformat()
    if latest_resp < cutoff:
        return False
    seen_at = t.get("seen_at")
    if not seen_at:
        return bool(kimi_ts or qwen_ts)
    return (kimi_ts > seen_at) or (qwen_ts > seen_at)


def _unseen_threads(state):
    """Return threads with unacknowledged mesh responses."""
    return [t for t in state.get("threads", []) if _has_unseen_responses(t)]


def cmd_new(state=None):
    """Show mesh responses I haven't engaged with yet."""
    if state is None:
        state = _load_state()
    unseen = _unseen_threads(state)
    if not unseen:
        print("No new mesh responses.")
        return

    print(f"{len(unseen)} thread(s) with new mesh responses:")
    for t in unseen:
        ts = t.get("timestamp", "?")[:16]
        topic = t.get("topic", "?")
        new_from = []
        seen_at = t.get("seen_at", "")
        if t.get("kimi") and (not seen_at or t.get("kimi_ts", "") > seen_at):
            new_from.append("Kimi")
        if t.get("qwen") and (not seen_at or t.get("qwen_ts", "") > seen_at):
            new_from.append("Qwen")
        print(f"  [{ts}] NEW from: {', '.join(new_from)} — {topic}")
        for who in new_from:
            snippet = t.get(who.lower(), "")
            if snippet:
                preview = snippet[:120].replace("\n", " ")
                print(f"    └ {preview}...")


def cmd_ack(state, msg_id=None):
    """Mark responses as seen."""
    now = datetime.now(timezone.utc).isoformat()
    threads = state.get("threads", [])
    if msg_id:
        found = False
        for t in threads:
            if t["msg_id"] == msg_id:
                t["seen_at"] = now
                found = True
                print(f"Acknowledged: {t.get('topic', '?')}")
                break
        if not found:
            print(f"Thread {msg_id} not found.")
    else:
        print("Usage: thread_tracker.py ack <msg_id>")
        return
    _save_state(state)


def cmd_ack_all(state):
    """Mark all current responses as seen, using the latest response timestamp."""
    count = 0
    for t in state.get("threads", []):
        if _has_unseen_responses(t):
            latest = max(t.get("kimi_ts", ""), t.get("qwen_ts", ""))
            if latest:
                t["seen_at"] = latest
            else:
                t["seen_at"] = datetime.now(timezone.utc).isoformat()
            count += 1
    _save_state(state)
    print(f"Acknowledged {count} thread(s).")


def cmd_summary(state=None):
    """One-line summary for rhythm pulse."""
    if state is None:
        state = _load_state()
    threads = state.get("threads", [])
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
    recent = [t for t in threads if t.get("timestamp", "") > cutoff]
    total = len(threads)
    no_response = sum(1 for t in recent if not t.get("kimi") and not t.get("qwen"))
    unseen = _unseen_threads(state)
    parts = [f"Threads: {total} tracked"]
    if unseen:
        parts.append(f"**{len(unseen)} NEW mesh responses**")
    if no_response:
        parts.append(f"{no_response} unanswered (48h)")
    if not unseen and not no_response:
        parts.append("all clear")
    print(", ".join(parts))


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("command", nargs="?", default="status",
                        choices=["status", "pending", "new", "ack", "ack-all", "refresh", "summary"],
                        help="Command to run")
    parser.add_argument("arg", nargs="?", help="Optional argument (msg_id for ack)")
    args = parser.parse_args()

    if args.command == "refresh":
        state = refresh()
        cmd_status(state)
    elif args.command == "pending":
        state = refresh()
        cmd_pending(state)
    elif args.command == "new":
        state = refresh()
        cmd_new(state)
    elif args.command == "ack":
        state = refresh()
        cmd_ack(state, args.arg)
    elif args.command == "ack-all":
        state = refresh()
        cmd_ack_all(state)
    elif args.command == "summary":
        state = refresh()
        cmd_summary(state)
    else:
        state = refresh()
        cmd_status(state)


if __name__ == "__main__":
    main()
