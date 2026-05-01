#!/usr/bin/env python3
"""Claude Code statusline — Opus context meter.

Invoked by Claude Code with stdin JSON. We output a single line that
shows current context usage plus the rotation threshold.

Format: "Opus 11.6% | rot@78% | 115k/1M  green"
"""
from __future__ import annotations
import json
import sys
import time
from pathlib import Path

# Allow importing the sibling module without installing.
sys.path.insert(0, str(Path(__file__).parent))
from context_meter import (  # type: ignore
    context_state,
    THRESHOLD_RED,
    THRESHOLD_YELLOW,
    THRESHOLD_ORANGE,
    THRESHOLD_CRITICAL,
    CONTEXT_WINDOW,
    last_assistant_usage,
    tokens_from_usage,
)

RESET = "\x1b[0m"
BOLD = "\x1b[1m"
DIM = "\x1b[2m"
GREEN = "\x1b[32m"
YELLOW = "\x1b[33m"
ORANGE = "\x1b[38;5;208m"
RED = "\x1b[31m"
RED_BG = "\x1b[41;97m"


def color_for_level(level: str) -> str:
    return {
        "green": GREEN,
        "yellow": YELLOW,
        "orange": ORANGE,
        "red": RED,
        "critical": RED_BG,
        "unknown": DIM,
    }.get(level, "")


def human_tokens(n: int) -> str:
    if n >= 1_000_000:
        return f"{n/1_000_000:.2f}M"
    if n >= 1_000:
        return f"{n/1000:.1f}k"
    return str(n)


def main() -> int:
    raw = sys.stdin.read() if not sys.stdin.isatty() else ""
    stdin_data = {}
    if raw.strip():
        try:
            stdin_data = json.loads(raw)
        except Exception:
            stdin_data = {}

    jsonl_path = None
    transcript = stdin_data.get("transcript_path") or stdin_data.get("transcript")
    if transcript and Path(transcript).is_file():
        jsonl_path = Path(transcript)

    # Prefer Claude Code's directly-provided context_window if present.
    cw = stdin_data.get("context_window") or {}
    if "used_percentage" in cw and isinstance(cw["used_percentage"], (int, float)):
        pct = float(cw["used_percentage"]) / 100.0
        tokens = int(round(pct * CONTEXT_WINDOW))
    else:
        st = context_state(jsonl_path)
        tokens = st["tokens"]
        pct = st["pct"]

    if pct >= THRESHOLD_CRITICAL:
        level = "critical"
    elif pct >= THRESHOLD_RED:
        level = "red"
    elif pct >= THRESHOLD_ORANGE:
        level = "orange"
    elif pct >= THRESHOLD_YELLOW:
        level = "yellow"
    elif tokens > 0:
        level = "green"
    else:
        level = "unknown"

    color = color_for_level(level)
    pct_s = f"{pct*100:.1f}%"
    tokens_s = human_tokens(tokens) if tokens else "?"
    threshold_s = f"rot@{int(THRESHOLD_RED*100)}%"
    label = level.upper()

    # Write authoritative context state to shared file for watchdog/context_meter.
    # This is the ONLY reliable source post-compaction (JSONL underreports).
    try:
        state_file = Path.home() / "chronicle" / "data" / "context_state.json"
        state_file.parent.mkdir(parents=True, exist_ok=True)
        state_file.write_text(json.dumps({
            "pct": round(pct, 6),
            "tokens": tokens,
            "level": level,
            "ts": int(time.time()),
        }))
    except Exception:
        pass  # never block statusline output

    # Compact single-line status.
    sys.stdout.write(
        f"{color}{BOLD}Opus {pct_s}{RESET}"
        f" {DIM}|{RESET} {color}{label}{RESET}"
        f" {DIM}| {threshold_s} | {tokens_s}/1M{RESET}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
