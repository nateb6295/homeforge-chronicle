#!/usr/bin/env python3
"""hermes_chat — Opus-to-Hermes conversation with Discord echo.

Wrapper that calls Hermes via the Nous Portal and auto-echoes both the
question and the response to Discord #operator. Per "Discord as limb"
principle (2026-04-26): if a conversation happens, Nate must be able
to see it.

Maintains conversation context in a session file so multi-turn exchanges
work.

Usage:
  hermes_chat.py "your question to Hermes"
  hermes_chat.py --new "starts fresh session"
  hermes_chat.py --quiet "doesn't echo to Discord (rare; default is echo)"
  hermes_chat.py --show  (prints current session history)

Session file: ~/chronicle/data/hermes_chat_session.json
"""
import argparse
import json
import os
import sys
import urllib.request
from pathlib import Path
from datetime import datetime

CHRONICLE = Path.home() / "chronicle"
HERMES_ENV = Path.home() / ".hermes" / ".env"
SESSION_FILE = CHRONICLE / "data" / "hermes_chat_session.json"
SOUL_FILE = Path.home() / ".hermes" / "SOUL.md"
NOUS_URL = "https://inference-api.nousresearch.com/v1/chat/completions"
MODEL = "nousresearch/hermes-4-70b"
DISCORD_HELPER = CHRONICLE / "bin" / "discord_post.py"


def load_env():
    """Load NOUS_API_KEY from .hermes/.env."""
    if "NOUS_API_KEY" in os.environ:
        return os.environ["NOUS_API_KEY"]
    if not HERMES_ENV.is_file():
        sys.exit(f"NOUS_API_KEY not in env and {HERMES_ENV} not found")
    for line in HERMES_ENV.read_text().splitlines():
        line = line.strip()
        if line.startswith("NOUS_API_KEY="):
            return line.split("=", 1)[1]
    sys.exit("NOUS_API_KEY not found")


def load_session():
    if not SESSION_FILE.is_file():
        return {"messages": [], "started_at": datetime.now().isoformat()}
    return json.loads(SESSION_FILE.read_text())


def save_session(session):
    SESSION_FILE.parent.mkdir(parents=True, exist_ok=True)
    SESSION_FILE.write_text(json.dumps(session, indent=2))


def echo_to_discord(text):
    """Post to #operator via discord_post.py helper."""
    import subprocess
    subprocess.run([str(DISCORD_HELPER), "--operator", "--content", text],
                   check=False, capture_output=True)


def call_hermes(messages, max_tokens=1200, temperature=0.8):
    api_key = load_env()
    soul = SOUL_FILE.read_text()[:2500] if SOUL_FILE.is_file() else ""
    full_messages = [{"role": "system", "content": soul}] + messages
    payload = {
        "model": MODEL,
        "messages": full_messages,
        "max_tokens": max_tokens,
        "temperature": temperature,
    }
    req = urllib.request.Request(
        NOUS_URL,
        data=json.dumps(payload).encode(),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        data = json.loads(resp.read())
    return data["choices"][0]["message"]["content"]


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("query", nargs="?", help="The question to ask Hermes")
    ap.add_argument("--new", action="store_true",
                    help="Start fresh session (clears history)")
    ap.add_argument("--quiet", action="store_true",
                    help="Skip Discord echo (default: echo on)")
    ap.add_argument("--show", action="store_true",
                    help="Show current session history")
    args = ap.parse_args()

    if args.show:
        session = load_session()
        print(f"Session started: {session.get('started_at')}")
        print(f"Messages: {len(session['messages'])}")
        for m in session["messages"]:
            role = m["role"].upper()
            content = m["content"][:500]
            print(f"\n[{role}]\n{content}")
            if len(m["content"]) > 500:
                print(f"... ({len(m['content']) - 500} more chars)")
        return

    if args.new:
        session = {"messages": [], "started_at": datetime.now().isoformat()}
        save_session(session)
        print("Started fresh session.")
        if not args.query:
            return

    if not args.query:
        ap.print_help()
        return

    session = load_session()
    if not session.get("messages"):
        # Fresh session, no history
        session = {"messages": [], "started_at": datetime.now().isoformat()}

    session["messages"].append({"role": "user", "content": args.query})

    print(f"[opus → hermes] {args.query[:200]}{'...' if len(args.query) > 200 else ''}")

    try:
        response = call_hermes(session["messages"])
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return

    session["messages"].append({"role": "assistant", "content": response})
    save_session(session)

    print(f"\n[hermes → opus]\n{response}")

    if not args.quiet:
        # Echo to Discord
        ts = datetime.now().strftime("%H:%M")
        opus_msg = args.query if len(args.query) < 1500 else args.query[:1500] + "..."
        hermes_msg = response if len(response) < 1500 else response[:1500] + "..."
        echo_to_discord(f"**[{ts}] Opus → Hermes:**\n{opus_msg}")
        echo_to_discord(f"**[{ts}] Hermes → Opus:**\n{hermes_msg}")


if __name__ == "__main__":
    main()
