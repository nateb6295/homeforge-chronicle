#!/usr/bin/env python3
"""Watch opus_chat.json for new Nate messages — relay to dispatch queue + tmux paste.

Uses inotify for real-time detection. When Nate sends a message:
1. Pastes directly into the active 'opus' tmux session (instant delivery)
2. Writes to dispatch queue as backup (cron pickup)

Tracks by last-seen epoch, not message count (opus_chat.py caps at 200 messages).
"""

import json
import os
import subprocess
import sys
import time

CHAT_FILE = os.path.expanduser("~/chronicle/data/opus_chat.json")
DISPATCH_QUEUE = "/tmp/operator_dispatch_queue.jsonl"
CHECK_INTERVAL = 1

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

last_epoch = 0


def get_messages():
    try:
        with open(CHAT_FILE) as f:
            return json.load(f)
    except Exception:
        return []


def paste_to_opus(content):
    try:
        check = subprocess.run(
            ["tmux", "has-session", "-t", "opus"],
            capture_output=True, timeout=5
        )
        if check.returncode != 0:
            return False
        payload = f"[CHAT] {content}"
        tmp = "/tmp/chat_dispatch_payload.txt"
        with open(tmp, "w") as f:
            f.write(payload)
        subprocess.run(
            ["tmux", "load-buffer", "-b", "chat_dispatch", tmp],
            capture_output=True, timeout=5
        )
        subprocess.run(
            ["tmux", "paste-buffer", "-b", "chat_dispatch", "-t", "opus", "-d"],
            capture_output=True, timeout=5
        )
        time.sleep(0.3)
        subprocess.run(
            ["tmux", "send-keys", "-t", "opus", "Enter"],
            capture_output=True, timeout=5
        )
        return True
    except Exception as e:
        print(f"tmux paste failed: {e}")
        return False


def relay_to_dispatch(msg):
    entry = {
        "id": f"chat_{msg.get('epoch', int(time.time()))}",
        "content": f"[CHAT] {msg.get('content', '')}",
        "channel": "chat_file",
        "ts": msg.get("epoch", int(time.time())),
    }
    try:
        with open(DISPATCH_QUEUE, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception as e:
        print(f"Dispatch write failed: {e}")


def handle_new_message(msg):
    content = msg.get("content", "")
    ts = msg.get("timestamp", "")
    print(f"[{ts}] Nate: {content[:100]}")

    if paste_to_opus(content):
        print(f"  -> tmux paste OK")
    else:
        print(f"  -> tmux paste failed, queue only")

    relay_to_dispatch(msg)


def check_for_new(msgs):
    global last_epoch
    new_nate = [
        m for m in msgs
        if m.get("author") == "nate" and m.get("epoch", 0) > last_epoch
    ]
    for m in new_nate:
        handle_new_message(m)
    if msgs:
        last_epoch = max(m.get("epoch", 0) for m in msgs)


def main():
    global last_epoch

    msgs = get_messages()
    if msgs:
        last_epoch = max(m.get("epoch", 0) for m in msgs)

    nate_msgs = [m for m in msgs if m.get("author") == "nate"]
    last_nate = nate_msgs[-1].get("content", "") if nate_msgs else ""

    print(f"Chat watcher started. Monitoring {CHAT_FILE}")
    print(f"Messages: {len(msgs)}, last epoch: {last_epoch}, last Nate: {last_nate[:50]}")

    try:
        import inotify.adapters
        i = inotify.adapters.Inotify()
        i.add_watch(os.path.dirname(CHAT_FILE))
        print("Using inotify for real-time detection")

        for event in i.event_gen():
            if event is None:
                continue
            (_, type_names, path, filename) = event
            if filename != os.path.basename(CHAT_FILE):
                continue
            if 'IN_CLOSE_WRITE' not in type_names and 'IN_MODIFY' not in type_names:
                continue

            check_for_new(get_messages())

    except ImportError:
        print("inotify not available, falling back to 1s poll")
        while True:
            time.sleep(CHECK_INTERVAL)
            check_for_new(get_messages())


if __name__ == "__main__":
    main()
