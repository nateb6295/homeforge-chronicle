#!/usr/bin/env python3
"""Rotation watchdog — external process that keeps Opus from hitting auto-compact.

Polls the active Claude Code session JSONL every POLL_INTERVAL seconds, computes
current context load via the usage block on the latest assistant turn, and:

  1. Posts threshold crossings to Discord.
  2. At THRESHOLD_RED, drops ~/chronicle/ROTATE_NOW so Opus self-rotates on
     the next nudge (via nudge_rotation_check.sh).
  3. At THRESHOLD_CRITICAL, posts an emergency ping.

Runs as chronicle-rotation-watch.service.
"""
from __future__ import annotations
import json
import os
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from context_meter import (  # type: ignore
    find_active_session_jsonl,
    last_assistant_usage,
    tokens_from_usage,
    CONTEXT_WINDOW,
    THRESHOLD_YELLOW,
    THRESHOLD_ORANGE,
    THRESHOLD_RED,
    THRESHOLD_CRITICAL,
)

POLL_INTERVAL = int(os.environ.get("WATCHDOG_POLL_SECS", "60"))
STATE_FILE = Path.home() / "chronicle" / "logs" / "rotation_watchdog.state"
LOG_FILE = Path.home() / "chronicle" / "logs" / "rotation_watchdog.log"
ROTATE_FLAG = Path.home() / "chronicle" / "ROTATE_NOW"

OPUS_WEBHOOK = os.environ.get("OPUS_WEBHOOK", "")

LEVELS = [
    ("critical", THRESHOLD_CRITICAL, "🚨", "CRITICAL — rotate immediately"),
    ("red", THRESHOLD_RED, "🔴", "RED — auto-rotation triggered"),
    ("orange", THRESHOLD_ORANGE, "🟠", "ORANGE — start winding down"),
    ("yellow", THRESHOLD_YELLOW, "💛", "YELLOW — context filling"),
]


def log(msg: str) -> None:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    with LOG_FILE.open("a") as f:
        f.write(f"[{ts}] {msg}\n")


def load_state() -> dict:
    if STATE_FILE.is_file():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            return {}
    return {}


def save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))


def discord_post(msg: str, webhook: str = "") -> None:
    """Post to Discord. Default webhook = OPUS_WEBHOOK (#opus). Pass webhook
    arg to override (e.g. OPERATOR_WEBHOOK for #operator escalations).
    Discord requires a User-Agent — silent 403 if missing.
    """
    target = webhook or OPUS_WEBHOOK
    if not target:
        log("discord_post skipped: no webhook configured")
        return
    try:
        req = urllib.request.Request(
            target,
            data=json.dumps({"content": msg[:1900]}).encode(),
            headers={
                "Content-Type": "application/json",
                "User-Agent": "chronicle-rotation-watch/1.0",
            },
            method="POST",
        )
        urllib.request.urlopen(req, timeout=6).read()
    except Exception as e:
        log(f"discord_post error: {e}")


def level_for(pct: float) -> str:
    for name, threshold, _, _ in LEVELS:
        if pct >= threshold:
            return name
    return "green"


def highest_level_crossed(prev_pct: float, cur_pct: float) -> list[str]:
    """Return level names newly crossed UP since last poll."""
    crossed = []
    for name, threshold, _, _ in LEVELS:
        if cur_pct >= threshold and prev_pct < threshold:
            crossed.append(name)
    return crossed


def level_entry(name: str):
    for e in LEVELS:
        if e[0] == name:
            return e
    return None


def main() -> int:
    state = load_state()
    last_pct = float(state.get("last_pct", 0.0))
    last_session = state.get("last_session", "")

    log(f"watchdog start (poll {POLL_INTERVAL}s, window {CONTEXT_WINDOW})")

    while True:
        try:
            jsonl = find_active_session_jsonl()
            if jsonl is None:
                log("no active session jsonl found; sleeping")
                time.sleep(POLL_INTERVAL)
                continue

            session_id = jsonl.stem
            if session_id != last_session:
                log(f"new session detected: {session_id} (was {last_session!r})")
                # Fresh session: clear ROTATE_NOW and reset last_pct.
                if ROTATE_FLAG.is_file():
                    ROTATE_FLAG.unlink()
                    log("cleared stale ROTATE_NOW on session change")
                last_pct = 0.0
                last_session = session_id

            usage = last_assistant_usage(jsonl)
            if not usage:
                time.sleep(POLL_INTERVAL)
                continue

            tokens = tokens_from_usage(usage)
            pct = tokens / CONTEXT_WINDOW
            lvl = level_for(pct)

            crossed = highest_level_crossed(last_pct, pct)
            for name in crossed:
                entry = level_entry(name)
                if not entry:
                    continue
                _, thr, emoji, text = entry
                msg = (
                    f"{emoji} **Opus context {pct*100:.1f}%** "
                    f"({tokens:,} / {CONTEXT_WINDOW:,} tokens) — {text}"
                )
                discord_post(msg)
                log(f"crossed {name}: {pct*100:.1f}%")

            # At or above RED, drop the rotation flag so Opus auto-rotates on next nudge.
            if pct >= THRESHOLD_RED and not ROTATE_FLAG.is_file():
                ROTATE_FLAG.write_text(
                    json.dumps(
                        {
                            "dropped_at": int(time.time()),
                            "pct": pct,
                            "tokens": tokens,
                            "session": session_id,
                            "reason": f"watchdog crossed {lvl} threshold",
                        },
                        indent=2,
                    )
                )
                log(f"dropped ROTATE_NOW at {pct*100:.1f}%")

            # Escalation: if ROTATE_NOW has been sitting unhonored for >5min,
            # post to #operator so Nate can intervene. Today's failure mode:
            # silent flag drops piled up while Opus kept building, until
            # Anthropic's auto-compactor fired well past 100%.
            escalated_at = state.get("escalated_at")
            escalated_session = state.get("escalated_session")
            if ROTATE_FLAG.is_file():
                flag_data = {}
                try:
                    flag_data = json.loads(ROTATE_FLAG.read_text())
                except Exception:
                    pass
                flag_age = int(time.time()) - int(flag_data.get("dropped_at", 0))
                already_escalated_this_drop = (
                    escalated_session == session_id and escalated_at
                    and escalated_at >= flag_data.get("dropped_at", 0)
                )
                if flag_age >= 300 and not already_escalated_this_drop:
                    operator_webhook = os.environ.get("OPERATOR_WEBHOOK", "")
                    msg = (
                        f"⚠️ **ROTATE_NOW unhonored {flag_age // 60}m** — Opus context "
                        f"at {pct*100:.1f}%, flag dropped at "
                        f"{flag_data.get('pct', 0)*100:.1f}% but session hasn't "
                        f"rotated. If Opus isn't responding, intervene before "
                        f"auto-compact fires."
                    )
                    discord_post(msg, webhook=operator_webhook)
                    log(f"escalated to #operator at {flag_age}s flag-age, pct {pct*100:.1f}%")
                    state["escalated_at"] = int(time.time())
                    state["escalated_session"] = session_id

            last_pct = pct
            state["last_pct"] = pct
            state["last_session"] = session_id
            state["last_tokens"] = tokens
            save_state(state)

        except Exception as e:
            log(f"loop error: {e}")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    sys.exit(main())
