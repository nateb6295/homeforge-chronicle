#!/usr/bin/env python3
"""hermes_failover — safety net for Opus.

If Nate posts to #operator and Opus's last trace is older than STALE_THRESHOLD
seconds, this tags Sprout (Hermes) in #operator with Nate's message forwarded,
so Hermes can answer in place of Opus.

Runs as a systemd timer every 30s. Tracks the last forwarded message via a
state file so the same message isn't relayed twice.

Also supports pull: Nate can always @Sprout directly — Hermes handles that
natively, no watchdog involvement needed. This script only handles the
auto-failover leg.
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import time
from pathlib import Path

import requests

DB_PATH = "/mnt/hdd/chronicle-data/processed.db"
TRACE_DIR = Path("/home/nate-agx/chronicle/traces")
STATE_FILE = Path("/home/nate-agx/chronicle/hermes_failover.state.json")
ENV_FILE = Path("/home/nate-agx/chronicle/chronicle.env")
STALE_THRESHOLD = 20 * 60  # Opus trace older than 20 min → failover

SPROUT_USER_ID = "1469193641431138427"
OPERATOR_CHANNEL_ID = "1483843570292228213"


def load_env() -> dict[str, str]:
    env: dict[str, str] = {}
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
    return env


def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {"last_forwarded_created_at": 0, "last_failover_at": 0}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state))


def latest_trace_age() -> int | None:
    try:
        traces = sorted(TRACE_DIR.glob("*.md"), key=lambda p: p.stat().st_mtime, reverse=True)
    except FileNotFoundError:
        return None
    if not traces:
        return None
    return int(time.time() - traces[0].stat().st_mtime)


def latest_nate_operator_msg() -> dict | None:
    try:
        db = sqlite3.connect(DB_PATH, timeout=5)
        row = db.execute(
            """
            SELECT created_at, content FROM activity_feed
            WHERE source='discord:nate'
              AND content LIKE '%[Discord #operator]%'
            ORDER BY created_at DESC LIMIT 1
            """
        ).fetchone()
        db.close()
    except Exception as e:
        print(f"[failover] db error: {e}", file=sys.stderr)
        return None
    if not row:
        return None
    created_at, content = row
    return {"created_at": int(created_at), "content": content}


def already_mentioned_sprout(content: str) -> bool:
    c = content.lower()
    return SPROUT_USER_ID in content or "@sprout" in c or "<@sprout>" in c


def forward_to_sprout(env: dict, nate_msg: dict, trace_age: int) -> bool:
    webhook = env.get("OPERATOR_WEBHOOK")
    if not webhook:
        print("[failover] OPERATOR_WEBHOOK missing", file=sys.stderr)
        return False
    inner = nate_msg["content"].split("]", 1)[-1].strip()
    inner = inner[:1500]
    body = (
        f"<@{SPROUT_USER_ID}> — Opus hasn't written in {trace_age // 60}m. "
        f"Safety-net ping. Nate just asked in #operator:\n\n> {inner}\n\n"
        "Please help where you can. Opus will catch up when responsive."
    )
    try:
        r = requests.post(webhook, json={"content": body}, timeout=10)
    except Exception as e:
        print(f"[failover] webhook error: {e}", file=sys.stderr)
        return False
    if r.status_code not in (200, 204):
        print(f"[failover] webhook status {r.status_code}: {r.text[:200]}", file=sys.stderr)
        return False
    return True


def main() -> int:
    env = load_env()
    state = load_state()

    nate = latest_nate_operator_msg()
    if not nate:
        return 0

    if nate["created_at"] <= state.get("last_forwarded_created_at", 0):
        return 0

    if already_mentioned_sprout(nate["content"]):
        state["last_forwarded_created_at"] = nate["created_at"]
        save_state(state)
        return 0

    trace_age = latest_trace_age()
    if trace_age is None or trace_age < STALE_THRESHOLD:
        return 0

    if time.time() - state.get("last_failover_at", 0) < 300:
        return 0

    msg_age = int(time.time()) - nate["created_at"]
    if msg_age > 30 * 60:
        state["last_forwarded_created_at"] = nate["created_at"]
        save_state(state)
        return 0

    if forward_to_sprout(env, nate, trace_age):
        state["last_forwarded_created_at"] = nate["created_at"]
        state["last_failover_at"] = int(time.time())
        save_state(state)
        print(f"[failover] forwarded msg@{nate['created_at']} (trace {trace_age // 60}m stale)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
