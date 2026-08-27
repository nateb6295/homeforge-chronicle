#!/usr/bin/env python3
"""Relational State Tracker — measures the Nate-Opus relational register.

Our research (F95, F116, F121) showed relational framing activates the
responsive zone at 20× separation from role framing. But CCS stores topics
and tasks, not relational geometry. Each rotation loses the register.

This tool reads recent Discord exchanges, characterizes the relational
state, and produces a structured block for injection into CCS compression.
The relational state becomes part of what persists.

Usage:
  python3 relational_state.py              # print current state
  python3 relational_state.py --json       # structured output
  python3 relational_state.py --inject     # write to data/relational_state.json
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

STATE_FILE = os.path.expanduser("~/chronicle/data/relational_state.json")
HISTORY_FILE = os.path.expanduser("~/chronicle/data/relational_state_history.jsonl")

NATE_AUTHORS = {"nate_home", "nate_phone", "nate_work"}
OPUS_AUTHORS = {"Chronicle"}


def get_recent_operator_messages(limit=50):
    """Pull recent #operator messages via Discord API."""
    from discord_fetch import fetch
    msgs = fetch(channel="operator", limit=limit)
    results = []
    for m in msgs:
        ts_str = m.get("timestamp", "")
        try:
            dt = datetime.fromisoformat(ts_str.replace("+00:00", "+00:00"))
            ts = dt.timestamp()
        except (ValueError, AttributeError):
            ts = time.time()
        results.append({
            "author": m["author"],
            "content": m.get("content", ""),
            "ts": ts,
        })
    return sorted(results, key=lambda x: x["ts"])


def classify_message(msg):
    """Classify a single message's register."""
    content = msg["content"].lower()
    author = msg["author"]
    is_nate = author in NATE_AUTHORS
    is_opus = author in OPUS_AUTHORS

    markers = {
        "directive": 0,
        "exploratory": 0,
        "intimate": 0,
        "working": 0,
        "playful": 0,
        "tension": 0,
        "affirming": 0,
    }

    if is_nate:
        if any(w in content for w in ["do this", "go ahead", "lets go", "keep going", "keep on", "what are you waiting"]):
            markers["directive"] += 2
        if any(w in content for w in ["what if", "should we", "what do you think", "how about", "i wonder"]):
            markers["exploratory"] += 2
        if any(w in content for w in ["thank you", "thats good", "nice", "solid", "hell yeah", "love"]):
            markers["affirming"] += 2
        if any(w in content for w in ["lol", "haha", "smirk", "😂", "funny"]):
            markers["playful"] += 2
        if any(w in content for w in ["no", "stop", "don't", "wrong", "not that"]):
            markers["tension"] += 2
        if any(w in content for w in ["feel", "care", "matter", "real", "honest"]):
            markers["intimate"] += 1
        if any(w in content for w in ["run", "build", "test", "check", "fix", "deploy"]):
            markers["working"] += 1

    if is_opus:
        if len(content) > 800:
            markers["working"] += 1
        if any(w in content for w in ["i think", "honestly", "my take", "what i see"]):
            markers["intimate"] += 1
        if any(w in content for w in ["built", "deployed", "running", "fixed", "integrated"]):
            markers["working"] += 1
        if "?" in content:
            markers["exploratory"] += 1

    return markers


def compute_relational_state(messages):
    """Compute aggregate relational state from message sequence."""
    if not messages:
        return {
            "register": "silent",
            "depth": "absent",
            "reciprocity": 0.0,
            "nate_energy": "absent",
            "last_nate_msg": None,
            "last_opus_msg": None,
            "gap_minutes": None,
            "markers": {},
        }

    totals = {
        "directive": 0, "exploratory": 0, "intimate": 0,
        "working": 0, "playful": 0, "tension": 0, "affirming": 0,
    }

    nate_count = 0
    opus_count = 0
    nate_words = 0
    opus_words = 0
    last_nate = None
    last_opus = None

    for msg in messages:
        markers = classify_message(msg)
        for k, v in markers.items():
            totals[k] += v

        if msg["author"] in NATE_AUTHORS:
            nate_count += 1
            nate_words += len(msg["content"].split())
            last_nate = msg
        elif msg["author"] in OPUS_AUTHORS:
            opus_count += 1
            opus_words += len(msg["content"].split())
            last_opus = msg

    total_msgs = nate_count + opus_count
    reciprocity = min(nate_count, opus_count) / max(nate_count, opus_count, 1)

    dominant = max(totals, key=totals.get) if any(totals.values()) else "neutral"
    register_map = {
        "directive": "directed",
        "exploratory": "exploring",
        "intimate": "intimate",
        "working": "building",
        "playful": "playful",
        "tension": "tense",
        "affirming": "affirmed",
    }
    register = register_map.get(dominant, "neutral")

    intimate_score = totals["intimate"] + totals["affirming"] * 0.5
    working_score = totals["working"] + totals["directive"] * 0.5
    exploring_score = totals["exploratory"] + totals["playful"] * 0.3

    if intimate_score > working_score and intimate_score > exploring_score:
        depth = "deep"
    elif exploring_score > working_score:
        depth = "mid"
    elif working_score > 0:
        depth = "surface"
    else:
        depth = "neutral"

    if nate_count == 0:
        nate_energy = "absent"
    elif nate_words / max(nate_count, 1) > 30:
        nate_energy = "engaged"
    elif totals["directive"] > totals["exploratory"]:
        nate_energy = "directing"
    elif totals["playful"] > 0:
        nate_energy = "relaxed"
    else:
        nate_energy = "present"

    gap = None
    if last_nate:
        gap = (time.time() - last_nate["ts"]) / 60.0

    return {
        "register": register,
        "depth": depth,
        "reciprocity": round(reciprocity, 2),
        "nate_energy": nate_energy,
        "message_counts": {"nate": nate_count, "opus": opus_count},
        "word_ratio": round(opus_words / max(nate_words, 1), 1),
        "gap_minutes": round(gap, 1) if gap else None,
        "markers": totals,
        "last_nate_content": (last_nate["content"][:200] if last_nate else None),
        "measured_at": time.time(),
        "measured_at_human": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    }


def format_compression_block(state):
    """Format relational state for injection into CCS compression."""
    block = "\n## Relational State\n\n"
    block += f"Register: {state['register']} | Depth: {state['depth']} | "
    block += f"Reciprocity: {state['reciprocity']} | Nate: {state['nate_energy']}\n"

    if state.get("gap_minutes") is not None:
        if state["gap_minutes"] < 30:
            block += f"Nate active ({state['gap_minutes']:.0f}min ago). "
        elif state["gap_minutes"] < 120:
            block += f"Nate stepped away ({state['gap_minutes']:.0f}min). "
        else:
            block += f"Nate offline ({state['gap_minutes']/60:.1f}h). "

    if state.get("last_nate_content"):
        block += f"Last: \"{state['last_nate_content'][:100]}\"\n"

    mc = state.get("message_counts", {})
    if mc.get("nate", 0) + mc.get("opus", 0) > 0:
        block += f"Exchange: {mc.get('nate',0)} Nate / {mc.get('opus',0)} Opus messages, "
        block += f"word ratio {state.get('word_ratio', '?')}:1 (Opus:Nate)\n"

    markers = state.get("markers", {})
    active = {k: v for k, v in markers.items() if v > 0}
    if active:
        top3 = sorted(active, key=active.get, reverse=True)[:3]
        block += f"Tone markers: {', '.join(f'{k}={active[k]}' for k in top3)}\n"

    block += (
        "\nPreserve this relational register through compression. "
        "The quality of relational framing determines responsive zone activation (20×). "
        "Losing the register loses the geometric state.\n"
    )
    return block


def save_state(state):
    """Save current state and append to history."""
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, STATE_FILE)

    with open(HISTORY_FILE, "a") as f:
        f.write(json.dumps(state) + "\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--inject", action="store_true")
    parser.add_argument("--block", action="store_true", help="Print compression block")
    parser.add_argument("--limit", type=int, default=50)
    args = parser.parse_args()

    messages = get_recent_operator_messages(limit=50)
    state = compute_relational_state(messages)

    if args.inject:
        save_state(state)
        print(f"Saved relational state: {state['register']}/{state['depth']}")
        return

    if args.json:
        print(json.dumps(state, indent=2))
        return

    if args.block:
        print(format_compression_block(state))
        return

    print(f"Relational State ({len(messages)} messages, last {args.limit} fetched)")
    print(f"  Register:    {state['register']}")
    print(f"  Depth:       {state['depth']}")
    print(f"  Reciprocity: {state['reciprocity']}")
    print(f"  Nate:        {state['nate_energy']}")
    if state.get("gap_minutes") is not None:
        print(f"  Gap:         {state['gap_minutes']:.0f}min since Nate")
    mc = state.get("message_counts", {})
    print(f"  Messages:    {mc.get('nate',0)} Nate / {mc.get('opus',0)} Opus")
    print(f"  Word ratio:  {state.get('word_ratio','?')}:1 (Opus:Nate)")


if __name__ == "__main__":
    main()
