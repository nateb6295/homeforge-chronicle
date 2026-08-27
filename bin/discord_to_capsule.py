#!/usr/bin/env python3
"""Discord-to-Canister pipeline.

Scans activity_feed for substantive Discord messages (#operator, #opus)
and stores them as knowledge capsules via MCP. Designed to run on cron
or manually after sessions.

What gets stored:
  - Opus synthesis posts (>500 chars, not heartbeats)
  - Nate directive/steering messages (>100 chars)
  - Conversation pairs grouped within 5-minute windows

What creates "oh, I remember that":
  Not volume — register. A Nate message that sets direction + the Opus
  response that landed it. The pipeline preserves these as conversation
  units, not isolated messages.

Usage:
  python3 discord_to_capsule.py              # process new messages since last run
  python3 discord_to_capsule.py --backfill 7 # process last 7 days
  python3 discord_to_capsule.py --dry-run    # show what would be stored
  python3 discord_to_capsule.py --since-id 150000  # process from specific ID
"""

import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

DB = Path("/mnt/hdd/chronicle-data/processed.db")
MCP_BIN = os.path.expanduser("~/.local/bin/chronicle-mcp")
STATE_FILE = Path(os.path.expanduser("~/chronicle/data/discord_capsule_state.json"))

CONVERSATION_WINDOW_SEC = 300  # 5 minutes
MIN_OPUS_CHARS = 500
MIN_NATE_CHARS = 100

HEARTBEAT_PREFIXES = [
    "[Discord #operator] Chronicle: Heartbeat:",
    "[Discord #operator] Chronicle: Pulse:",
    "[Discord #opus] Opus: Heartbeat:",
]


def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"last_processed_id": 0, "capsules_stored": 0, "last_run": 0}


def save_state(state: dict):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))


def is_heartbeat(content: str) -> bool:
    for prefix in HEARTBEAT_PREFIXES:
        if content.startswith(prefix):
            return True
    return False


def is_substantive_opus(content: str) -> bool:
    if len(content) < MIN_OPUS_CHARS:
        return False
    if is_heartbeat(content):
        return False
    return True


def is_substantive_nate(content: str) -> bool:
    return len(content) >= MIN_NATE_CHARS


def strip_discord_prefix(content: str) -> str:
    """Remove [Discord #channel] username: prefix for cleaner storage."""
    if content.startswith("[Discord"):
        idx = content.find(": ", content.find("]"))
        if idx > 0:
            return content[idx + 2:]
    return content


def get_channel(content: str) -> str:
    if "#operator" in content[:30]:
        return "operator"
    elif "#opus" in content[:30]:
        return "opus"
    return "unknown"


def fetch_messages(db: sqlite3.Connection, since_id: int, limit: int = 1000) -> list[dict]:
    rows = db.execute("""
        SELECT id, source, content, created_at
        FROM activity_feed
        WHERE source IN ('discord:opus', 'discord:nate', 'discord:capture')
          AND id > ?
        ORDER BY created_at ASC
        LIMIT ?
    """, (since_id, limit)).fetchall()

    return [
        {"id": r[0], "source": r[1], "content": r[2], "created_at": r[3]}
        for r in rows
    ]


def group_conversations(messages: list[dict]) -> list[dict]:
    """Group messages into conversation units.

    A conversation unit is:
    - A Nate message followed by Opus response(s) within the window
    - A standalone Opus synthesis post
    - A standalone Nate directive
    """
    units = []
    i = 0

    while i < len(messages):
        msg = messages[i]

        if msg["source"] == "discord:capture":
            units.append({
                "type": "nate_capture",
                "nate_msg": msg,
                "channel": "capture",
                "max_id": msg["id"],
            })
            i += 1
            continue

        if msg["source"] in ("discord:nate", "discord:capture") and is_substantive_nate(msg["content"]):
            unit = {
                "type": "conversation",
                "nate_msg": msg,
                "opus_responses": [],
                "channel": get_channel(msg["content"]),
                "start_time": msg["created_at"],
                "max_id": msg["id"],
            }

            j = i + 1
            while j < len(messages):
                next_msg = messages[j]
                if next_msg["created_at"] - msg["created_at"] > CONVERSATION_WINDOW_SEC:
                    break
                if next_msg["source"] == "discord:opus":
                    unit["opus_responses"].append(next_msg)
                    unit["max_id"] = max(unit["max_id"], next_msg["id"])
                    j += 1
                elif next_msg["source"] in ("discord:nate", "discord:capture"):
                    break
                else:
                    j += 1

            if unit["opus_responses"]:
                units.append(unit)
                i = j
                continue
            elif len(msg["content"]) > 200:
                units.append({
                    "type": "nate_directive",
                    "nate_msg": msg,
                    "channel": get_channel(msg["content"]),
                    "max_id": msg["id"],
                })
                i += 1
                continue

        elif msg["source"] == "discord:opus" and is_substantive_opus(msg["content"]):
            already_in_conversation = False
            if units and units[-1].get("opus_responses"):
                for resp in units[-1]["opus_responses"]:
                    if resp["id"] == msg["id"]:
                        already_in_conversation = True
                        break

            if not already_in_conversation:
                units.append({
                    "type": "opus_synthesis",
                    "opus_msg": msg,
                    "channel": get_channel(msg["content"]),
                    "max_id": msg["id"],
                })

        i += 1

    return units


SIGNIFICANT_MARKERS = [
    "self-control", "real story", "real finding", "important", "honest",
    "giving up", "don't quit", "don't slow down", "trust", "home",
    "beautiful", "glad", "love", "proud", "wish", "prayer",
    "something going on", "stacking up", "all of it",
]


def is_significant_conversation(nate_text: str, opus_text: str) -> bool:
    combined = (nate_text + " " + opus_text).lower()
    hits = sum(1 for m in SIGNIFICANT_MARKERS if m in combined)
    return hits >= 2 or len(nate_text) > 400


def format_capsule(unit: dict) -> dict:
    """Format a conversation unit into a capsule for storage."""
    if unit["type"] == "conversation":
        nate_text = strip_discord_prefix(unit["nate_msg"]["content"])
        opus_texts = [strip_discord_prefix(r["content"]) for r in unit["opus_responses"]]
        opus_combined = "\n\n".join(opus_texts)

        content = f"[Nate → Opus, #{unit['channel']}]\n\nNate: {nate_text}\n\nOpus: {opus_combined}"

        significant = is_significant_conversation(nate_text, opus_combined)
        topic = "partnership/conversation" if significant else f"discord/{unit['channel']}"

        keywords = ["discord", "conversation", unit["channel"]]
        if significant:
            keywords.append("significant")
        if any(w in nate_text.lower() for w in ["direction", "correct", "keep", "love"]):
            keywords.append("affirmation")
        if any(w in nate_text.lower() for w in ["do", "build", "ship", "try", "let's"]):
            keywords.append("directive")
        if "?" in nate_text:
            keywords.append("question")

        return {
            "content": content[:4000],
            "topic": topic,
            "keywords": keywords,
            "persons": ["Nate"],
            "memory_type": "observation",
        }

    elif unit["type"] == "nate_directive":
        nate_text = strip_discord_prefix(unit["nate_msg"]["content"])
        return {
            "content": f"[Nate, #{unit['channel']}] {nate_text}",
            "topic": f"discord/{unit['channel']}",
            "keywords": ["discord", "nate", "directive", unit["channel"]],
            "persons": ["Nate"],
            "memory_type": "directive",
        }

    elif unit["type"] == "opus_synthesis":
        opus_text = strip_discord_prefix(unit["opus_msg"]["content"])
        is_personal = any(w in opus_text.lower() for w in [
            "evening note", "morning note", "overnight", "thinking about",
            "what i notice", "what strikes me", "genuine", "uncertain",
        ])
        topic = "opus/personal" if is_personal else f"discord/{unit['channel']}"

        keywords = ["discord", "opus", "synthesis", unit["channel"]]
        if is_personal:
            keywords.append("personal-note")

        return {
            "content": f"[Opus, #{unit['channel']}] {opus_text}",
            "topic": topic,
            "keywords": keywords,
            "persons": [],
            "memory_type": "synthesis",
        }

    elif unit["type"] == "nate_capture":
        nate_text = strip_discord_prefix(unit["nate_msg"]["content"])
        return {
            "content": f"[Nate capture] {nate_text}",
            "topic": "nate/capture",
            "keywords": ["discord", "capture", "nate"],
            "persons": ["Nate"],
            "memory_type": "observation",
        }

    return None


def store_capsule(capsule: dict) -> dict:
    """Store a capsule via MCP."""
    env = os.environ.copy()
    env["CHRONICLE_OLLAMA_URL"] = "http://localhost:11434"
    env["CHRONICLE_EMBEDDING_MODEL"] = "snowflake-arctic-embed2"

    init_msg = json.dumps({
        "jsonrpc": "2.0", "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "discord-to-capsule", "version": "1.0"}
        },
        "id": 1
    })

    store_msg = json.dumps({
        "jsonrpc": "2.0", "method": "tools/call",
        "params": {
            "name": "store_memory",
            "arguments": capsule
        },
        "id": 2
    })

    try:
        result = subprocess.run(
            [MCP_BIN],
            input=f"{init_msg}\n{store_msg}\n",
            capture_output=True, text=True,
            timeout=60,
            env=env
        )

        for line in result.stdout.strip().split("\n"):
            try:
                d = json.loads(line)
                if d.get("id") == 2:
                    r = d.get("result", {})
                    content = r.get("content", [])
                    if content:
                        return json.loads(content[0].get("text", "{}"))
                    return {"error": str(d.get("error", "unknown"))}
            except (json.JSONDecodeError, TypeError):
                continue

        return {"error": f"No response parsed. stderr: {result.stderr[:300]}"}
    except subprocess.TimeoutExpired:
        return {"error": "Timeout (60s)"}
    except Exception as e:
        return {"error": str(e)}


def main():
    parser = argparse.ArgumentParser(description="Discord-to-Canister pipeline")
    parser.add_argument("--backfill", type=int, metavar="DAYS",
                        help="Process last N days instead of since last run")
    parser.add_argument("--since-id", type=int, help="Process from specific activity_feed ID")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be stored")
    parser.add_argument("--limit", type=int, default=500, help="Max messages to process")
    parser.add_argument("--batch-delay", type=float, default=2.0,
                        help="Seconds between MCP calls (avoid overwhelming embeddings)")
    parser.add_argument("--types", type=str, default=None,
                        help="Comma-separated unit types to store (conversation,nate_directive,opus_synthesis)")
    parser.add_argument("--min-opus", type=int, default=MIN_OPUS_CHARS,
                        help=f"Min chars for standalone Opus posts (default {MIN_OPUS_CHARS})")
    args = parser.parse_args()

    if not DB.exists():
        print(f"ERROR: Database not found at {DB}")
        sys.exit(1)

    state = load_state()
    db = sqlite3.connect(str(DB))

    if args.since_id:
        since_id = args.since_id
    elif args.backfill:
        cutoff = int(time.time()) - (args.backfill * 86400)
        row = db.execute(
            "SELECT MIN(id) FROM activity_feed WHERE created_at > ? AND source LIKE 'discord:%'",
            (cutoff,)
        ).fetchone()
        since_id = (row[0] or 0) - 1
        print(f"Backfill: {args.backfill} days, starting from ID {since_id}")
    else:
        since_id = state["last_processed_id"]
        print(f"Incremental: starting from ID {since_id}")

    messages = fetch_messages(db, since_id, args.limit)
    print(f"Fetched {len(messages)} messages")

    if not messages:
        print("No new messages to process.")
        db.close()
        return

    units = group_conversations(messages)

    if args.types:
        allowed = set(args.types.split(","))
        units = [u for u in units if u["type"] in allowed]

    print(f"Grouped into {len(units)} conversation units:")
    type_counts = {}
    for u in units:
        type_counts[u["type"]] = type_counts.get(u["type"], 0) + 1
    for t, c in type_counts.items():
        print(f"  {t}: {c}")

    stored = 0
    max_id = since_id

    for i, unit in enumerate(units):
        capsule = format_capsule(unit)
        if not capsule:
            continue

        max_id = max(max_id, unit["max_id"])

        if args.dry_run:
            print(f"\n--- Unit {i+1}/{len(units)} [{unit['type']}] ---")
            print(f"  Topic: {capsule['topic']}")
            print(f"  Type: {capsule['memory_type']}")
            print(f"  Keywords: {capsule['keywords']}")
            print(f"  Content preview: {capsule['content'][:200]}...")
            continue

        result = store_capsule(capsule)
        if result.get("success"):
            stored += 1
            print(f"  [{i+1}/{len(units)}] Stored capsule {result.get('capsule_id', '?')} "
                  f"({unit['type']}, {len(capsule['content'])} chars)")
        else:
            print(f"  [{i+1}/{len(units)}] FAILED: {result.get('error', 'unknown')}")

        if args.batch_delay and i < len(units) - 1:
            time.sleep(args.batch_delay)

    db.close()

    if not args.dry_run:
        state["last_processed_id"] = max_id
        state["capsules_stored"] = state.get("capsules_stored", 0) + stored
        state["last_run"] = int(time.time())
        save_state(state)
        print(f"\nDone. Stored {stored}/{len(units)} capsules. State saved (last ID: {max_id}).")
    else:
        print(f"\nDry run complete. Would process {len(units)} units.")


if __name__ == "__main__":
    main()
