#!/usr/bin/env python3
"""Shared context-meter utilities for statusline + watchdog.

Claude Code writes every session to ~/.claude/projects/<slug>/<session-id>.jsonl.
Each assistant message line contains message.usage with token counts. The last
assistant turn's input_tokens + cache_creation_input_tokens + cache_read_input_tokens
is the current in-context token load.

KNOWN LIMITATION (2026-04-17): After auto-compaction, the JSONL usage block only
reflects the post-compaction context size (e.g., 140K), NOT the actual context
window consumption that Claude Code tracks internally (e.g., 580K). The terminal
statusline is authoritative. This meter will underreport after compaction events.
Trust the statusline over this script's output.

Window size: the Opus session uses a 1M-token context window.
"""
from __future__ import annotations
import json
import os
from pathlib import Path
from typing import Optional

CONTEXT_WINDOW = 1_000_000

# Auto-compact triggers around 83-84% on Claude Code.
# Nate's observation (2026-04-23): startup ritual uses ~20% of context,
# breakthroughs start at 40-50%, peak flow is 50-80%. Rotating too early
# wastes the productive zone. Push rotation close to compaction with a
# small buffer for clean checkpoint.
THRESHOLD_YELLOW = 0.50
THRESHOLD_ORANGE = 0.70
THRESHOLD_RED = 0.80   # Our auto-rotate point (was 0.78, raised for flow)
THRESHOLD_CRITICAL = 0.83

PROJECTS_ROOT = Path.home() / ".claude" / "projects"


def find_active_session_jsonl(project_slug: str = "-home-nate-agx-chronicle") -> Optional[Path]:
    """Return the most-recently-modified JSONL in the project dir."""
    d = PROJECTS_ROOT / project_slug
    if not d.is_dir():
        return None
    jsonls = list(d.glob("*.jsonl"))
    if not jsonls:
        return None
    return max(jsonls, key=lambda p: p.stat().st_mtime)


def last_assistant_usage(jsonl_path: Path) -> Optional[dict]:
    """Walk the JSONL backward and return the most recent assistant usage block."""
    try:
        with jsonl_path.open("rb") as f:
            f.seek(0, 2)
            size = f.tell()
            chunk_size = 65536
            tail = b""
            pos = size
            while pos > 0:
                read_size = min(chunk_size, pos)
                pos -= read_size
                f.seek(pos)
                tail = f.read(read_size) + tail
                lines = tail.split(b"\n")
                # Keep partial first line for next iter unless we've hit start
                if pos > 0:
                    tail = lines[0]
                    candidates = lines[1:]
                else:
                    candidates = lines
                for line in reversed(candidates):
                    if not line.strip():
                        continue
                    try:
                        d = json.loads(line)
                    except Exception:
                        continue
                    if d.get("type") != "assistant":
                        continue
                    msg = d.get("message") or {}
                    usage = msg.get("usage")
                    if usage and isinstance(usage, dict):
                        return usage
        return None
    except Exception:
        return None


def tokens_from_usage(usage: dict) -> int:
    """Effective in-context tokens for the most recent turn."""
    return (
        int(usage.get("input_tokens", 0) or 0)
        + int(usage.get("cache_creation_input_tokens", 0) or 0)
        + int(usage.get("cache_read_input_tokens", 0) or 0)
    )


def _read_statusline_state() -> Optional[dict]:
    """Read the authoritative context state written by statusline.py.

    The statusline receives context_window.used_percentage directly from Claude Code,
    which is correct even after compaction. It writes this to a shared JSON file on
    every invocation. This is the preferred source — JSONL parsing is the fallback.
    """
    state_file = Path.home() / "chronicle" / "data" / "context_state.json"
    if not state_file.is_file():
        return None
    try:
        import time as _time
        data = json.loads(state_file.read_text())
        # Only trust if written within the last 5 minutes (statusline fires often)
        age = _time.time() - data.get("ts", 0)
        if age > 300:
            return None
        return data
    except Exception:
        return None


def context_state(jsonl_path: Optional[Path] = None) -> dict:
    """Return {tokens, pct, level, jsonl} where level is one of
    green | yellow | orange | red | critical.

    Prefers the authoritative statusline state file (correct post-compaction)
    over JSONL parsing (underreports after compaction).
    """
    # Try statusline-written state first (authoritative post-compaction)
    sl_state = _read_statusline_state()
    if sl_state and sl_state.get("pct", 0) > 0:
        return {
            "tokens": sl_state.get("tokens", 0),
            "pct": sl_state["pct"],
            "level": sl_state.get("level", "unknown"),
            "jsonl": str(jsonl_path) if jsonl_path else None,
            "source": "statusline",
        }

    # Fallback to JSONL parsing (may underreport after compaction)
    if jsonl_path is None:
        jsonl_path = find_active_session_jsonl()
    if jsonl_path is None:
        return {"tokens": 0, "pct": 0.0, "level": "unknown", "jsonl": None, "source": "none"}
    usage = last_assistant_usage(jsonl_path)
    if not usage:
        return {"tokens": 0, "pct": 0.0, "level": "unknown", "jsonl": str(jsonl_path), "source": "none"}
    tokens = tokens_from_usage(usage)
    pct = tokens / CONTEXT_WINDOW
    if pct >= THRESHOLD_CRITICAL:
        level = "critical"
    elif pct >= THRESHOLD_RED:
        level = "red"
    elif pct >= THRESHOLD_ORANGE:
        level = "orange"
    elif pct >= THRESHOLD_YELLOW:
        level = "yellow"
    else:
        level = "green"
    return {"tokens": tokens, "pct": pct, "level": level, "jsonl": str(jsonl_path), "source": "jsonl"}


if __name__ == "__main__":
    import sys
    st = context_state()
    print(json.dumps(st, indent=2))
    sys.exit(0 if st["level"] != "unknown" else 1)
