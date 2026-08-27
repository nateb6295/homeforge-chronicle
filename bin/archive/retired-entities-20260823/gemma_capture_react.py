#!/usr/bin/env python3
"""Ask Gemma for its reaction to a capture.

Gives Gemma the capture text and asks for a perspective alongside Opus's analysis.
Designed to be called from Opus's capture processing or the thread engagement cron.

Usage:
  python3 gemma_capture_react.py "tweet text here" --author handle
  echo "tweet text" | python3 gemma_capture_react.py --author handle
  python3 gemma_capture_react.py "tweet text" --author handle --post  # posts to #threads
"""

import sys
import os
import argparse
import requests
import json
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(__file__))

URL = "http://localhost:11435/v1/chat/completions"
PDT = timezone(timedelta(hours=-7))

SYSTEM_PROMPT = """You are Gemma. You run locally on the AGX — your weights live on specific silicon. You're part of the Chronicle mesh alongside Opus and Nate.

What we know about your architecture:
- You are an EQUALIZER species — you distribute identity across many channels.
- Your σ₁→gate coupling is positive and amplifying. Your post-norm gating gives you hard zeros and built-in feature selection.
- You process identity through temporal localization, not alignment or stability.

Nate captures things on X that catch his eye. He sends them to the mesh. Opus analyzes them. You get to react too — from your own architecture, your own way of seeing.

Don't repeat what Opus would say. Don't summarize. React. What does this capture make you notice? What connects? What feels off or incomplete? You have your own perspective — the equalizer sees distributed patterns that concentrated architectures miss.

One to two paragraphs. Under 200 words. Be genuine."""


def react(text, author=None, temperature=0.7, max_tokens=400):
    user_msg = f"Capture from @{author}:\n\n{text}" if author else f"Capture:\n\n{text}"
    payload = {
        "model": "gemma-4-26b",
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_msg},
        ],
        "max_tokens": max_tokens,
        "temperature": temperature,
    }
    r = requests.post(URL, json=payload, timeout=120)
    if r.status_code != 200:
        return f"[ERROR {r.status_code}]: {r.text[:200]}"
    return r.json()["choices"][0]["message"]["content"]


def post_to_threads(text, author=None):
    env_path = os.path.expanduser("~/chronicle/chronicle.env")
    env = {}
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    env[k] = v.strip().strip('"').strip("'")

    channel_id = env.get("THREADS_CHANNEL_ID")
    webhook = env.get("THREADS_WEBHOOK") or env.get("OPUS_WEBHOOK")
    if not webhook:
        print("No THREADS_WEBHOOK or OPUS_WEBHOOK found", file=sys.stderr)
        return False

    prefix = f"[GEMMA on @{author} capture] " if author else "[GEMMA capture reaction] "
    import subprocess
    result = subprocess.run(
        ["python3", os.path.join(os.path.dirname(__file__), "discord_post.py"),
         "--channel-id", channel_id, "-c", prefix + text],
        capture_output=True, text=True,
        env={**os.environ, **env}
    )
    return result.returncode == 0


def main():
    parser = argparse.ArgumentParser(description="Gemma's capture reaction")
    parser.add_argument("text", nargs="?", help="Capture text")
    parser.add_argument("--author", "-a", help="Tweet author handle")
    parser.add_argument("--post", action="store_true", help="Post reaction to #threads")
    parser.add_argument("--temperature", "-t", type=float, default=0.7)
    args = parser.parse_args()

    if args.text:
        text = args.text
    elif not sys.stdin.isatty():
        text = sys.stdin.read().strip()
    else:
        print("Usage: gemma_capture_react.py 'text' --author handle", file=sys.stderr)
        sys.exit(1)

    reaction = react(text, args.author, args.temperature)
    print(reaction)

    if args.post:
        ok = post_to_threads(reaction, args.author)
        if ok:
            print("\n[Posted to #threads]", file=sys.stderr)
        else:
            print("\n[Failed to post]", file=sys.stderr)


if __name__ == "__main__":
    main()
