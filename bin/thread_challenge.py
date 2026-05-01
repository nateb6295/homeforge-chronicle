#!/usr/bin/env python3
"""
Thread challenge generator — organizational closure step #3.

When a thread goes dormant, don't just surface its existing question. Have
Gemma read the recent history and generate a fresh *challenge* — a pressure
question that tests or extends the current position.

This is the move that closes the loop tighter: the system notices stalling
(via cognitive_health.py) AND proposes a restart move.

Usage:
  python3 thread_challenge.py --auto             # pick oldest-updated active thread
  python3 thread_challenge.py --thread-id 318    # target specific thread
  python3 thread_challenge.py --dry              # print only, do not post
"""
import argparse
import json
import os
import sqlite3
import subprocess
import sys
import urllib.request

DB = "/mnt/hdd/chronicle-data/processed.db"
GEMMA_URL = "http://localhost:11435/v1/chat/completions"
GEMMA_MODEL = "gemma-4-26B-A4B-it-Q4_K_M.gguf"
def _load_chronicle_env():
    env_path = os.path.expanduser("~/chronicle/chronicle.env")
    if not os.path.isfile(env_path):
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().strip("'\"")
            if key and key not in os.environ:
                os.environ[key] = val


_load_chronicle_env()
OPERATOR_WEBHOOK = os.environ.get("OPERATOR_WEBHOOK", "")


def pick_thread(db, thread_id=None):
    if thread_id:
        row = db.execute(
            "SELECT id, title, question FROM cognitive_threads WHERE id = ?",
            (thread_id,),
        ).fetchone()
    else:
        row = db.execute(
            "SELECT id, title, question FROM cognitive_threads "
            "WHERE status = 'active' ORDER BY updated_at ASC LIMIT 1"
        ).fetchone()
    return row


def thread_history(db, thread_id, limit=8):
    rows = db.execute(
        "SELECT event_type, content, created_at FROM thread_history "
        "WHERE thread_id = ? ORDER BY created_at DESC LIMIT ?",
        (thread_id, limit),
    ).fetchall()
    return list(reversed(rows))


def build_prompt(title, question, history):
    history_text = "\n".join(
        f"- [{ev}] {content[:300]}" for ev, content, _ in history
    ) or "(no recorded history yet)"
    return (
        f"You are helping a long-running inquiry pose its next move.\n\n"
        f"Thread title: {title}\n"
        f"Original question: {question}\n\n"
        f"Recent thread arc (oldest → newest):\n{history_text}\n\n"
        f"Write ONE sharp challenge question (1–3 sentences) that would pressure "
        f"the thread's current position — something that tests an assumption, "
        f"surfaces a counterexample, or forces a measurable claim. Do not "
        f"summarize. Do not preface. Output ONLY the challenge question itself."
    )


def call_gemma(prompt, timeout=60):
    payload = {
        "model": GEMMA_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 220,
        "temperature": 0.75,
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        GEMMA_URL, data=data, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        resp = json.loads(r.read())
    return resp["choices"][0]["message"]["content"].strip()


def post_operator(content):
    body = json.dumps({"content": content})
    subprocess.run(
        ["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}",
         "-X", "POST", "-H", "Content-Type: application/json",
         "-d", body, OPERATOR_WEBHOOK],
        capture_output=True, text=True, timeout=10,
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--auto", action="store_true")
    ap.add_argument("--thread-id", type=int)
    ap.add_argument("--dry", action="store_true")
    args = ap.parse_args()

    if not args.auto and not args.thread_id:
        print("Provide --auto or --thread-id", file=sys.stderr)
        return 2

    db = sqlite3.connect(DB)
    thread = pick_thread(db, args.thread_id)
    if not thread:
        print("No thread found", file=sys.stderr)
        return 2
    tid, title, question = thread
    history = thread_history(db, tid)
    db.close()

    prompt = build_prompt(title, question, history)
    try:
        challenge = call_gemma(prompt)
    except Exception as e:
        print(f"Gemma call failed: {e}", file=sys.stderr)
        return 1

    msg = (
        f"**Thread #{tid} auto-challenge** — generated from {len(history)} history entries\n\n"
        f"*{title}*\n\n"
        f"{challenge}"
    )
    print(msg)

    if not args.dry:
        post_operator(msg)
        print("[posted to #operator]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
