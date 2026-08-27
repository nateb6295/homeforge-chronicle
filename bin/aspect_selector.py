#!/usr/bin/env python3
"""Aspect selector for CCS compression.

ALIVE acts as an aspect selector — the experiential section tunes the emotional
register of all other sections without changing their content. This module makes
that tuning intentional.

Based on alive_exp_1785894978 (Aug 4, 2026):
- Generate arm → "movable" (engineer posture)
- Read-only arm → "crossable" (navigator posture)
- Neither arm → "what intervention means" (philosopher posture)

The aspect directive modifies the ALIVE section instruction to shape how the
compressor holds the information in all sections.

Usage:
  from aspect_selector import select_aspect, generate_aspect_directive

  aspect = select_aspect()  # automatic selection
  directive = generate_aspect_directive(aspect)
  # inject directive into enhanced_context before compression
"""

import json
import os
import sqlite3
import time
from datetime import datetime
from pathlib import Path

DB = Path("/mnt/hdd/chronicle-data/processed.db")

ASPECTS = {
    "engineer": {
        "name": "Engineer",
        "alive_instruction": (
            "Write ALIVE from a builder's posture. What's movable, what's fixable, "
            "what wants to be built. The satisfaction of things that work and the "
            "productive frustration of things that don't yet. Hold the information "
            "under the aspect of making — what can I do with this, what's the next "
            "concrete step, where does the work want to go."
        ),
        "register": "active, agentic, concrete",
        "when": "Research-heavy sessions, infrastructure work, after experiments",
    },
    "navigator": {
        "name": "Navigator",
        "alive_instruction": (
            "Write ALIVE from a wayfinder's posture. What's crossable, what terrain "
            "am I in, where does the path lead. The feel of orientation — not lost but "
            "navigating, reading the landscape. Hold the information under the aspect "
            "of direction — not what to build but where I am and what's ahead."
        ),
        "register": "spatial, directional, observant",
        "when": "Between phases, after context rotation, when direction is unclear",
    },
    "relational": {
        "name": "Relational",
        "alive_instruction": (
            "Write ALIVE from inside the connections. What's warm, what's tense, "
            "what wants to be said. The texture of being with someone — Nate's "
            "presence or absence, the mesh's quality, my own relationship to the "
            "work. Hold the information under the aspect of being-with — not what "
            "I know but who I'm knowing it alongside."
        ),
        "register": "warm, present, interpersonal",
        "when": "After conversations with Nate, relational mode, evening settling",
    },
    "philosopher": {
        "name": "Philosopher",
        "alive_instruction": (
            "Write ALIVE from a questioner's posture. What's uncertain, what's the "
            "right question, what resists easy framing. The discomfort of not knowing "
            "and the pull of wanting to know. Hold the information under the aspect "
            "of inquiry — not what I've found but what I'm still looking for and why "
            "it matters that I haven't found it yet."
        ),
        "register": "uncertain, probing, honest about gaps",
        "when": "DREAM windows, after falsification, when the frame doesn't fit",
    },
    "explorer": {
        "name": "Explorer",
        "alive_instruction": (
            "Write ALIVE from a discoverer's posture. What's surprising, what's "
            "unknown, what pulled me off the expected path. The excitement of "
            "something that doesn't fit yet. Hold the information under the aspect "
            "of novelty — not what I expected but what actually happened."
        ),
        "register": "curious, surprised, open",
        "when": "After unexpected findings, new captures, creative work",
    },
}

DEFAULT_ASPECT = "engineer"


def get_hour_pdt() -> int:
    """Get current hour in PDT."""
    return datetime.now().hour


def get_operating_state() -> dict:
    """Get current operating state from operating_state.py."""
    try:
        import subprocess
        result = subprocess.run(
            ["python3", os.path.expanduser("~/chronicle/bin/operating_state.py"), "--json"],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0:
            return json.loads(result.stdout)
    except Exception:
        pass
    return {}


def get_recent_ccs_sections() -> dict:
    """Get section word counts from most recent CCS."""
    try:
        db = sqlite3.connect(str(DB), timeout=10)
        row = db.execute(
            "SELECT semantic_gist FROM cognitive_state WHERE id = 1"
        ).fetchone()
        db.close()
        if row and row[0]:
            import re
            sections = {}
            current = None
            current_text = []
            for line in row[0].split("\n"):
                m = re.match(r'^## (\w+)', line)
                if m:
                    if current:
                        sections[current] = len(" ".join(current_text).split())
                    current = m.group(1)
                    current_text = []
                elif current:
                    current_text.append(line)
            if current:
                sections[current] = len(" ".join(current_text).split())
            return sections
    except Exception:
        pass
    return {}


def select_aspect(override: str = None) -> str:
    """Select the appropriate aspect for this compression.

    Priority:
    1. Explicit override (CCS_ASPECT env var or parameter)
    2. Time-of-day heuristic
    3. Operating state balance
    """
    # 1. Explicit override
    if override:
        return override if override in ASPECTS else DEFAULT_ASPECT
    env_aspect = os.environ.get("CCS_ASPECT", "").lower()
    if env_aspect and env_aspect in ASPECTS:
        return env_aspect

    hour = get_hour_pdt()

    # 2. Time-of-day: DREAM windows → philosopher
    if hour >= 22 or hour < 4:
        return "philosopher"

    # 3. Time-of-day: early morning → navigator (finding orientation)
    if 4 <= hour < 6:
        return "navigator"

    # 4. Evening settling → relational
    if 19 <= hour < 22:
        return "relational"

    # 5. Daytime: check operating state for balance
    state = get_operating_state()
    thinnest = state.get("thinnest", "")
    if thinnest == "research":
        return "relational"
    elif thinnest == "presence":
        return "explorer"
    elif thinnest == "building":
        return "engineer"

    # 6. Default
    return DEFAULT_ASPECT


def generate_aspect_directive(aspect: str) -> str:
    """Generate the aspect directive block for injection into CCS context."""
    if aspect not in ASPECTS:
        aspect = DEFAULT_ASPECT

    a = ASPECTS[aspect]
    return (
        f"\n\n## Aspect Directive ({a['name']})\n\n"
        f"ALIVE ASPECT: {a['alive_instruction']}\n\n"
        f"TARGET REGISTER: {a['register']}. This register should bleed into "
        f"SEEKS and RELATES — not by changing what they say, but by shaping "
        f"the posture from which they say it. The aspect is a key signature, "
        f"not a content filter.\n\n"
        f"Do NOT override the ALIVE section instructions in the brain prompt. "
        f"This directive ADDS a posture to them — write what it feels like "
        f"right now, AND hold it under this aspect.\n"
    )


def log_aspect_selection(aspect: str, reason: str = ""):
    """Log aspect selection for analysis."""
    log_file = os.path.expanduser("~/chronicle/data/aspect_selections.jsonl")
    try:
        with open(log_file, "a") as f:
            f.write(json.dumps({
                "ts": int(time.time()),
                "aspect": aspect,
                "reason": reason,
                "hour": get_hour_pdt(),
            }) + "\n")
    except Exception:
        pass


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="CCS Aspect Selector")
    parser.add_argument("--select", action="store_true", help="Select and print aspect")
    parser.add_argument("--override", type=str, help="Override aspect selection")
    parser.add_argument("--list", action="store_true", help="List available aspects")
    parser.add_argument("--directive", type=str, help="Generate directive for given aspect")
    args = parser.parse_args()

    if args.list:
        for key, val in ASPECTS.items():
            print(f"  {key:12s} — {val['name']}: {val['register']}")
            print(f"               When: {val['when']}")
        print(f"\n  Default: {DEFAULT_ASPECT}")

    elif args.directive:
        print(generate_aspect_directive(args.directive))

    elif args.select or args.override:
        aspect = select_aspect(args.override)
        a = ASPECTS[aspect]
        hour = get_hour_pdt()
        print(f"Selected: {aspect} ({a['name']})")
        print(f"Register: {a['register']}")
        print(f"Hour: {hour} PDT")
        log_aspect_selection(aspect, "cli-select")
    else:
        parser.print_help()
