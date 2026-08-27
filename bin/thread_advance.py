#!/usr/bin/env python3
"""Atomic thread advance — advances canister AND posts to Discord #threads.

Usage:
  thread_advance.py <thread_id> "<content>" [--type advance|synthesis|prediction|question|connection]
  echo "content" | thread_advance.py <thread_id> [--type synthesis]

Posts to #threads with thread title prefix, then advances the canister.
This ensures thread work is ALWAYS visible in Discord, not just stored in the backend.
"""

import os
import sys
import json
import subprocess
import argparse


def load_env():
    env_file = os.path.expanduser("~/chronicle/chronicle.env")
    if os.path.exists(env_file):
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if "=" in line and not line.startswith("#"):
                    key, val = line.split("=", 1)
                    val = val.strip().strip('"').strip("'")
                    if key not in os.environ:
                        os.environ[key] = val


THREAD_TITLES = {
    315: "Constraint Satisfaction",
    316: "Interoception as Grounding",
    317: "Temporal Binding",
    319: "The Legibility Inversion",
    320: "The Ecology of Identity",
    321: "Relational Grounding",
    322: "Attention as Medium",
    324: "The Compositionality Gradient",
}


def post_to_threads(thread_id: int, content: str, event_type: str) -> bool:
    load_env()
    channel_id = os.environ.get("THREADS_CHANNEL_ID", "1509006814916771932")
    title = THREAD_TITLES.get(thread_id, f"Thread #{thread_id}")
    type_label = event_type.upper() if event_type != "advance" else ""
    header = f"**#{thread_id} {title}"
    if type_label:
        header += f" — {type_label}"
    header += "**\n\n"
    full_content = header + content

    try:
        result = subprocess.run(
            [
                sys.executable,
                os.path.expanduser("~/chronicle/bin/discord_post.py"),
                "--channel-id", channel_id,
                "-c", full_content,
            ],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode == 0:
            return True
        else:
            print(f"Discord post failed: {result.stderr}", file=sys.stderr)
            return False
    except Exception as e:
        print(f"Discord post error: {e}", file=sys.stderr)
        return False


def advance_canister(thread_id: int, content: str, event_type: str) -> bool:
    mcp_bin = os.path.expanduser("~/.local/bin/chronicle-mcp")
    try:
        payload = json.dumps({
            "method": "advance_thread",
            "params": {
                "thread_id": thread_id,
                "content": content,
                "event_type": event_type,
                "source": "opus",
            }
        })
        result = subprocess.run(
            [mcp_bin, "call", "advance_thread", "--params", json.dumps({
                "thread_id": thread_id,
                "content": content,
                "event_type": event_type,
                "source": "opus",
            })],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode == 0:
            return True
        else:
            print(f"MCP advance failed: {result.stderr}", file=sys.stderr)
            print(f"Note: advance may need to be done via MCP tool call in session", file=sys.stderr)
            return False
    except Exception as e:
        print(f"MCP advance error: {e}", file=sys.stderr)
        return False


def main():
    parser = argparse.ArgumentParser(description="Atomic thread advance: canister + Discord")
    parser.add_argument("thread_id", type=int, help="Thread ID (e.g. 320)")
    parser.add_argument("content", nargs="?", default=None, help="Advance content (or pipe via stdin)")
    parser.add_argument("--type", dest="event_type", default="advance",
                        choices=["advance", "synthesis", "prediction", "question", "connection"],
                        help="Event type (default: advance)")
    parser.add_argument("--discord-only", action="store_true",
                        help="Only post to Discord, skip canister advance")
    parser.add_argument("--canister-only", action="store_true",
                        help="Only advance canister, skip Discord post")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be posted without doing it")
    args = parser.parse_args()

    content = args.content
    if content is None:
        if not sys.stdin.isatty():
            content = sys.stdin.read().strip()
        else:
            print("Error: provide content as argument or pipe via stdin", file=sys.stderr)
            sys.exit(1)

    if not content:
        print("Error: empty content", file=sys.stderr)
        sys.exit(1)

    title = THREAD_TITLES.get(args.thread_id, f"Thread #{args.thread_id}")

    if args.dry_run:
        print(f"Would advance #{args.thread_id} ({title}) [{args.event_type}]")
        print(f"Content ({len(content)} chars): {content[:200]}...")
        return

    discord_ok = True
    canister_ok = True

    if not args.canister_only:
        discord_ok = post_to_threads(args.thread_id, content, args.event_type)
        if discord_ok:
            print(f"✓ Posted to #threads: #{args.thread_id} {title}")
        else:
            print(f"✗ Discord post failed for #{args.thread_id}")

    if not args.discord_only:
        canister_ok = advance_canister(args.thread_id, content, args.event_type)
        if canister_ok:
            print(f"✓ Canister advanced: #{args.thread_id} {title}")
        else:
            print(f"⚠ Canister advance failed — use MCP advance_thread in session")

    if discord_ok:
        print(f"Done: #{args.thread_id} {title} [{args.event_type}]")
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
