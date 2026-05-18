#!/usr/bin/env python3
"""opus_hermes_bridge — auto-forward Opus #opus posts to Hermes when relevant.

Two modes:
  1. CLI wrapper:  opus_hermes_bridge.py "content to post"
     Posts to #opus, detects if Hermes should see it, forwards + posts response.

  2. Pipe mode:    echo "content" | opus_hermes_bridge.py --pipe
     Same logic, reads from stdin.

  3. Check-only:   opus_hermes_bridge.py --check "content"
     Prints whether content would trigger Hermes bridge (no posting).

Detection heuristics (any match triggers):
  - Contains "Hermes" or "@Hermes" (explicit mention)
  - Contains "?" in a sentence with intellectual framing keywords
  - Starts a thread or poses a question to the mesh
  - Contains "what do you think" / "thoughts?" / "your take" patterns

The bridge posts Hermes's response to #opus with clear attribution.
"""
import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

CHRONICLE = Path.home() / "chronicle"
ENV_FILE = CHRONICLE / "chronicle.env"
HERMES_CHAT = CHRONICLE / "bin" / "hermes_chat.py"
DISCORD_POST = CHRONICLE / "bin" / "discord_post.py"
BRIDGE_LOG = CHRONICLE / "data" / "hermes_bridge_log.jsonl"

HERMES_TRIGGERS = [
    r'\bhermes\b',
    r'@hermes',
    r'\bsprout\b',
]

QUESTION_PATTERNS = [
    r'what do you think',
    r'thoughts\?',
    r'your take',
    r'how would you',
    r'do you see',
    r'does that track',
    r'what.*make of',
    r'where does.*fit',
    r'how does.*relate',
    r'can you.*weigh in',
    r'curious.*perspective',
]

INTELLECTUAL_MARKERS = [
    r'\bthesis\b', r'\bhypothesis\b', r'\bprediction\b', r'\bframework\b',
    r'\bconnection\b', r'\bisomorphi', r'\bboundary\b', r'\bpersistence\b',
    r'\bidentity\b', r'\bconvergence\b', r'\bscaffold\b', r'\blabile\b',
    r'\bgrace period\b', r'\bcompression\b', r'\bcapsule\b',
]


def load_env():
    if os.environ.get("OPUS_WEBHOOK") and os.environ.get("NOUS_API_KEY"):
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


def should_bridge(content: str) -> tuple[bool, str]:
    """Return (should_forward, reason) for whether content should go to Hermes."""
    lower = content.lower()

    for pat in HERMES_TRIGGERS:
        if re.search(pat, lower):
            return True, "explicit mention"

    for pat in QUESTION_PATTERNS:
        if re.search(pat, lower):
            return True, f"question pattern: {pat}"

    has_question = '?' in content
    if has_question:
        marker_count = sum(1 for pat in INTELLECTUAL_MARKERS if re.search(pat, lower))
        if marker_count >= 2:
            return True, f"intellectual question ({marker_count} markers)"

    return False, "no trigger matched"


def post_to_opus(content: str) -> bool:
    """Post content to #opus via discord_post.py."""
    try:
        result = subprocess.run(
            [sys.executable, str(DISCORD_POST), "--opus", "--content", content],
            capture_output=True, text=True, timeout=15,
        )
        return result.returncode == 0
    except Exception as e:
        print(f"Discord post failed: {e}", file=sys.stderr)
        return False


def ask_hermes(question: str) -> str | None:
    """Send question to Hermes via hermes_chat.py, return response."""
    try:
        result = subprocess.run(
            [sys.executable, str(HERMES_CHAT), "--quiet", question],
            capture_output=True, text=True, timeout=120,
        )
        if result.returncode != 0:
            print(f"Hermes chat error: {result.stderr}", file=sys.stderr)
            return None
        output = result.stdout
        marker = "[hermes → opus]"
        idx = output.find(marker)
        if idx >= 0:
            return output[idx + len(marker):].strip()
        return output.strip() if output.strip() else None
    except Exception as e:
        print(f"Hermes chat failed: {e}", file=sys.stderr)
        return None


def log_bridge(content: str, bridged: bool, reason: str, hermes_response: str | None = None):
    BRIDGE_LOG.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "ts": datetime.now().isoformat(),
        "bridged": bridged,
        "reason": reason,
        "content_preview": content[:200],
        "hermes_response_preview": (hermes_response or "")[:200] if hermes_response else None,
    }
    with open(BRIDGE_LOG, "a") as f:
        f.write(json.dumps(entry) + "\n")


def bridge(content: str, *, dry_run: bool = False, skip_opus_post: bool = False) -> dict:
    """Full bridge cycle: post to #opus, detect, forward to Hermes, post response.

    Returns dict with what happened.
    """
    load_env()

    if not skip_opus_post and not dry_run:
        post_to_opus(content)

    bridged, reason = should_bridge(content)

    if not bridged:
        log_bridge(content, False, reason)
        return {"posted": True, "bridged": False, "reason": reason}

    if dry_run:
        return {"posted": False, "bridged": True, "reason": reason, "dry_run": True}

    framed = (
        f"Opus posted this to #opus — sharing for your perspective:\n\n"
        f"{content}\n\n"
        f"What's your read on this?"
    )

    hermes_response = ask_hermes(framed)

    if hermes_response:
        ts = datetime.now().strftime("%H:%M")
        response_post = f"**Hermes ({ts}):** {hermes_response}"
        if len(response_post) > 1900:
            response_post = response_post[:1897] + "..."
        post_to_opus(response_post)

    log_bridge(content, True, reason, hermes_response)

    return {
        "posted": True,
        "bridged": True,
        "reason": reason,
        "hermes_responded": hermes_response is not None,
        "response_preview": (hermes_response or "")[:200],
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("content", nargs="?", help="Content to post to #opus")
    ap.add_argument("--pipe", action="store_true", help="Read content from stdin")
    ap.add_argument("--check", action="store_true", help="Only check if bridge would trigger")
    ap.add_argument("--dry-run", action="store_true", help="No actual posting")
    ap.add_argument("--bridge-only", action="store_true",
                    help="Content already posted to #opus; just check and forward to Hermes")
    args = ap.parse_args()

    content = args.content
    if args.pipe or (not content and not sys.stdin.isatty()):
        content = sys.stdin.read().strip()

    if not content:
        ap.print_help()
        return

    if args.check:
        bridged, reason = should_bridge(content)
        status = "BRIDGE" if bridged else "SKIP"
        print(f"[{status}] {reason}")
        return

    result = bridge(content, dry_run=args.dry_run, skip_opus_post=args.bridge_only)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
