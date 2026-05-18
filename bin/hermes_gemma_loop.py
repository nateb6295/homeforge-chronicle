#!/usr/bin/env python3
"""Hermes-Gemma Feedback Loop.

Monitors #opus for Hermes messages, routes them through Gemma for quality scoring.
Below threshold: posts critique back to #opus for Hermes to see.
Above threshold: logs as quality-verified, optionally routes to thread history.

Run as a one-shot or on a cron.
"""
import json
import os
import re
import requests
import subprocess
import sys
import time

OPUS_CHANNEL_ID = "1483843572129202427"
EVAL_STATE = os.path.expanduser("~/chronicle/data/hermes_gemma_loop_state.json")
LOOP_LOG = os.path.expanduser("~/chronicle/data/hermes_loop_detections.jsonl")

def load_env():
    envfile = os.path.expanduser("~/chronicle/chronicle.env")
    if os.path.exists(envfile):
        with open(envfile) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip())


def detect_loop(content, recent_contents=None):
    """Detect if Hermes is in a repetition loop. Returns (is_loop, reason) tuple."""
    if not content or len(content) < 20:
        return False, ""

    # Pattern 1: emoji/character spam (same char repeated 10+ times)
    for ch in set(content):
        if content.count(ch) > len(content) * 0.4 and len(content) > 50:
            return True, "character spam"

    # Pattern 2: repeated phrases — split into clauses and check for duplication
    clauses = re.split(r'[.!?\n]', content)
    clauses = [c.strip().lower() for c in clauses if len(c.strip()) > 15]
    if len(clauses) >= 4:
        unique = set(clauses)
        if len(unique) < len(clauses) * 0.4:
            return True, "repeated clauses"

    # Pattern 3: "And I need to know" / "What are we trying to" cascade
    repetitive_stems = re.findall(
        r'(?:and i need to|what are we trying to|why does it|what is the)\b',
        content.lower()
    )
    if len(repetitive_stems) >= 5:
        return True, "question cascade"

    # Pattern 4: near-duplicate of a recent message from the same batch
    if recent_contents:
        for prev in recent_contents:
            if len(prev) < 50:
                continue
            overlap = _jaccard_similarity(content, prev)
            if overlap > 0.7:
                return True, f"near-duplicate (similarity={overlap:.2f})"

    return False, ""


def _jaccard_similarity(a, b):
    words_a = set(a.lower().split())
    words_b = set(b.lower().split())
    if not words_a or not words_b:
        return 0.0
    return len(words_a & words_b) / len(words_a | words_b)


def log_loop_detection(msg_id, reason, preview):
    entry = {
        "ts": int(time.time()),
        "msg_id": msg_id,
        "reason": reason,
        "preview": preview[:200],
    }
    os.makedirs(os.path.dirname(LOOP_LOG), exist_ok=True)
    with open(LOOP_LOG, "a") as f:
        f.write(json.dumps(entry) + "\n")


def get_recent_hermes_messages(limit=5):
    token = os.environ.get("OPUS_BOT_TOKEN", "")
    headers = {"Authorization": f"Bot {token}"}
    r = requests.get(
        f"https://discord.com/api/v10/channels/{OPUS_CHANNEL_ID}/messages?limit={limit}",
        headers=headers, timeout=10
    )
    msgs = r.json()
    if not isinstance(msgs, list):
        return []
    return [m for m in msgs if m.get("author", {}).get("username") == "Hermes"]


def load_state():
    if os.path.exists(EVAL_STATE):
        with open(EVAL_STATE) as f:
            return json.load(f)
    return {"last_evaluated_id": None}


def save_state(state):
    os.makedirs(os.path.dirname(EVAL_STATE), exist_ok=True)
    with open(EVAL_STATE, "w") as f:
        json.dump(state, f)


def post_critique(critique_text):
    """Post Gemma's critique to #opus for Hermes to see."""
    token = os.environ.get("OPUS_BOT_TOKEN", "")
    headers = {
        "Authorization": f"Bot {token}",
        "Content-Type": "application/json"
    }
    # Mention Hermes role so he picks it up
    content = f"**[Gemma → Hermes]** {critique_text}"
    if len(content) > 1900:
        content = content[:1900]
    r = requests.post(
        f"https://discord.com/api/v10/channels/{OPUS_CHANNEL_ID}/messages",
        headers=headers,
        json={"content": content},
        timeout=10
    )
    return r.status_code == 200


def main():
    load_env()
    state = load_state()
    last_id = state.get("last_evaluated_id")

    msgs = get_recent_hermes_messages(limit=5)
    if not msgs:
        print("No Hermes messages found")
        return

    SYSTEM_MSG_MARKERS = [
        "Session automatically reset",
        "Conversation history cleared",
        "Use /resume to browse",
        "◆ Model:",
        "◐ Session",
        "Cronjob Response:",
    ]

    # Filter to only new messages
    new_msgs = []
    for m in msgs:
        if last_id and int(m["id"]) <= int(last_id):
            continue
        content = m.get("content", "")
        if len(content) < 50:
            continue
        if any(marker in content for marker in SYSTEM_MSG_MARKERS):
            continue
        new_msgs.append(m)

    if not new_msgs:
        print("No new Hermes messages to evaluate")
        return

    print(f"Routing {len(new_msgs)} new Hermes message(s)...")

    from gemma_evaluator import route

    seen_contents = []
    loop_count = 0

    for m in reversed(new_msgs):  # oldest first
        content = m["content"]
        ts = m["timestamp"][:19]
        print(f"\n--- [{ts}] ---")
        print(f"  Preview: {content[:100]}...")

        is_loop, loop_reason = detect_loop(content, seen_contents)
        seen_contents.append(content)

        if is_loop:
            loop_count += 1
            print(f"  [LOOP DETECTED] {loop_reason} — skipping")
            log_loop_detection(m["id"], loop_reason, content)
            continue

        result = route(content, source=f"hermes-opus-{m['id']}")
        if result:
            print(f"  [{result['route']}] substance={result['substance']:.1f} — {result['summary']}")
        else:
            print("  Routing failed")

    if loop_count > 0:
        print(f"\n⚠ {loop_count} message(s) flagged as repetition loops")

    # Update state to latest message
    state["last_evaluated_id"] = msgs[0]["id"]
    save_state(state)
    print(f"\nState updated. Last evaluated: {msgs[0]['id']}")


if __name__ == "__main__":
    main()
