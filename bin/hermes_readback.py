#!/usr/bin/env python3
"""Hermes read-back — pull recent #capture messages so Opus can see Hermes's voice.

Uses the Discord bot token (same bot that delivers Hermes's output) to fetch
recent messages from the #capture channel via Discord REST API. No gateway,
no WebSocket — just a read.

Usage:
  python3 hermes_readback.py              # last 10 messages
  python3 hermes_readback.py --limit 25
  python3 hermes_readback.py --since 30m  # last 30 minutes only

Returns Hermes-authored messages (🔵 **[Hermes]** prefix) and Gemma-authored
messages (🟡 **[Gemma]**) — skips raw script output and other noise.
"""
import json
import os
import sys
import time
import urllib.request

CHANNEL_ID = "1483843572129202427"  # #capture
ENV_PATH = "/home/nate-agx/.hermes/.env"


def load_token() -> str | None:
    if not os.path.exists(ENV_PATH):
        return None
    with open(ENV_PATH) as f:
        for line in f:
            line = line.strip()
            if line.startswith("DISCORD_TOKEN="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    return None


def fetch_messages(token: str, limit: int = 10) -> list:
    url = f"https://discord.com/api/v10/channels/{CHANNEL_ID}/messages?limit={limit}"
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bot {token}",
        "User-Agent": "Chronicle-Opus-Readback/1.0",
    })
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())


def parse_since(s: str) -> int:
    s = s.strip().lower()
    if s.endswith("m"):
        return int(s[:-1]) * 60
    if s.endswith("h"):
        return int(s[:-1]) * 3600
    return int(s)


def format_msg(m: dict) -> str:
    ts = m.get("timestamp", "?")[:19].replace("T", " ")
    author = m.get("author", {}).get("username", "?")
    content = m.get("content", "") or ""
    flavor = "[H]" if "[Hermes]" in content else ("[G]" if "[Gemma]" in content else "[?]")
    return f"{ts} {flavor} {author}:\n{content}\n"


def main():
    args = sys.argv[1:]
    limit = 10
    since_seconds = None
    for i, a in enumerate(args):
        if a == "--limit" and i + 1 < len(args):
            limit = int(args[i + 1])
        elif a == "--since" and i + 1 < len(args):
            since_seconds = parse_since(args[i + 1])

    token = load_token()
    if not token:
        print("ERROR: no DISCORD_TOKEN in .hermes/.env", file=sys.stderr)
        sys.exit(1)

    try:
        msgs = fetch_messages(token, limit=limit)
    except Exception as e:
        print(f"fetch failed: {e}", file=sys.stderr)
        sys.exit(2)

    now = time.time()
    out = []
    for m in msgs:
        if since_seconds:
            ts_iso = m.get("timestamp", "")
            try:
                from datetime import datetime
                t = datetime.fromisoformat(ts_iso.replace("Z", "+00:00")).timestamp()
                if now - t > since_seconds:
                    continue
            except Exception:
                pass
        content = m.get("content", "") or ""
        if "[Hermes]" in content or "[Gemma]" in content:
            out.append(format_msg(m))

    if not out:
        print("(no Hermes or Gemma messages in the window)")
        return
    print(f"--- {len(out)} messages ---\n")
    for line in reversed(out):
        print(line)


if __name__ == "__main__":
    main()
