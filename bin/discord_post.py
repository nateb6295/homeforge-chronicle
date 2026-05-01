#!/usr/bin/env python3
"""discord_post — outbound Discord helper that feels like a limb, not a tool.

Usage as CLI:
  echo "content" | discord_post.py [--operator|--opus] [--review]
  discord_post.py --content "msg" [--operator|--opus] [--review]

Usage as module:
  from discord_post import post
  post("content here")  # defaults to operator
  post("public msg", channel="opus")

Behavior:
  - Auto-loads OPERATOR_WEBHOOK / OPUS_WEBHOOK from chronicle.env if not in env
  - Sends with User-Agent header (Discord rejects without it; silent 403 was the
    bug that hid threshold alerts for weeks)
  - Splits long content at 2000 chars at sentence/paragraph boundaries
  - On successful 204 to OPERATOR_WEBHOOK, updates ~/chronicle/data/last_opus_post.txt
    so discord_cadence_check.py reads accurately
  - Optional --review runs self_reviewer.py first; RED verdict refuses unless --force
"""
from __future__ import annotations
import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

CHRONICLE = Path.home() / "chronicle"
ENV_FILE = CHRONICLE / "chronicle.env"
LAST_POST = CHRONICLE / "data" / "last_opus_post.txt"
DISCORD_LIMIT = 2000
USER_AGENT = "chronicle-opus/1.0"


def _load_env() -> None:
    """Populate os.environ from chronicle.env if relevant keys are missing."""
    if os.environ.get("OPERATOR_WEBHOOK") and os.environ.get("OPUS_WEBHOOK"):
        return
    if not ENV_FILE.is_file():
        return
    for line in ENV_FILE.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip("'\"")
        if key and key not in os.environ:
            os.environ[key] = val


def _split_content(content: str, limit: int = DISCORD_LIMIT) -> list[str]:
    """Split content at paragraph or sentence boundaries to fit Discord's limit."""
    if len(content) <= limit:
        return [content]
    chunks: list[str] = []
    remaining = content
    while len(remaining) > limit:
        # Prefer paragraph break, then sentence end, then any whitespace.
        cut = remaining.rfind("\n\n", 0, limit)
        if cut < limit // 2:
            cut = max(remaining.rfind(". ", 0, limit), remaining.rfind("? ", 0, limit), remaining.rfind("! ", 0, limit))
            if cut < limit // 2:
                cut = remaining.rfind(" ", 0, limit)
        if cut <= 0:
            cut = limit
        chunks.append(remaining[:cut].rstrip())
        remaining = remaining[cut:].lstrip()
    if remaining:
        chunks.append(remaining)
    return chunks


def _bump_timestamp() -> None:
    LAST_POST.parent.mkdir(parents=True, exist_ok=True)
    LAST_POST.write_text(str(int(time.time())))


def _post_one(webhook: str, content: str, timeout: float = 10.0) -> int:
    req = urllib.request.Request(
        webhook,
        data=json.dumps({"content": content}).encode(),
        headers={"Content-Type": "application/json", "User-Agent": USER_AGENT},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status


def _resolve_webhook(channel: str) -> str:
    _load_env()
    if channel == "operator":
        url = os.environ.get("OPERATOR_WEBHOOK", "")
    elif channel == "opus":
        url = os.environ.get("OPUS_WEBHOOK", "")
    else:
        raise ValueError(f"unknown channel: {channel!r} (use 'operator' or 'opus')")
    if not url:
        raise RuntimeError(f"webhook for channel {channel!r} not configured")
    return url


def _review(content: str) -> str:
    """Run the self-reviewer if available. Returns verdict ('GREEN'/'YELLOW'/'RED'/'SKIP')."""
    reviewer = CHRONICLE / "bin" / "self_reviewer.py"
    if not reviewer.is_file():
        return "SKIP"
    try:
        result = subprocess.run(
            [sys.executable, str(reviewer), "--stdin"],
            input=content, capture_output=True, text=True, timeout=30,
        )
        text = (result.stdout or "") + (result.stderr or "")
        for verdict in ("RED", "YELLOW", "GREEN"):
            if verdict in text:
                return verdict
    except Exception:
        return "SKIP"
    return "SKIP"


def post(content: str, channel: str = "operator", *, review: bool = False,
         force: bool = False, dry_run: bool = False) -> dict:
    """Post `content` to Discord. Returns {'status': 204|..., 'parts': N, 'review': ..., 'webhook': channel}."""
    if not content or not content.strip():
        raise ValueError("empty content")

    review_verdict = "OFF"
    if review:
        review_verdict = _review(content)
        if review_verdict == "RED" and not force:
            return {"status": 0, "parts": 0, "review": review_verdict, "channel": channel,
                    "skipped": "RED verdict; pass force=True to send anyway"}

    webhook = _resolve_webhook(channel)
    parts = _split_content(content)
    statuses: list[int] = []
    for part in parts:
        if dry_run:
            statuses.append(204)
            continue
        statuses.append(_post_one(webhook, part))

    if not dry_run and channel == "operator" and all(s == 204 for s in statuses):
        _bump_timestamp()

    return {
        "status": statuses[-1] if statuses else 0,
        "parts": len(parts),
        "review": review_verdict,
        "channel": channel,
    }


def _cli() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0] if __doc__ else "")
    p.add_argument("--content", "-c", help="content to post (else read stdin)")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--operator", action="store_const", const="operator", dest="channel")
    g.add_argument("--opus", action="store_const", const="opus", dest="channel")
    p.set_defaults(channel="operator")
    p.add_argument("--review", action="store_true", help="run self-reviewer first")
    p.add_argument("--force", action="store_true", help="send even on RED review")
    p.add_argument("--dry-run", action="store_true", help="don't actually send")
    args = p.parse_args()

    if args.content is None:
        content = sys.stdin.read()
    else:
        content = args.content

    result = post(content, channel=args.channel, review=args.review,
                  force=args.force, dry_run=args.dry_run)
    print(json.dumps(result))
    return 0 if result.get("status") == 204 else 1


if __name__ == "__main__":
    sys.exit(_cli())
