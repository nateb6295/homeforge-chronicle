#!/usr/bin/env python3
"""responsiveness_watchdog — detect when agent is unresponsive to cycle nudges.

The 65-minute gap on 2026-04-28 (10:38 → 11:43 PDT) exposed a real
architectural gap: cycle nudges fire on schedule, cron jobs continue,
anchor files stay fresh via auto-refresh, but there's no system-level
signal for "agent is processing the events."

This watchdog tracks the most recent agent-side activity (trace files
written, discord posts via webhook log, self-model entries added) and
posts a Discord alert if no signal in N minutes.

Designed to run every 5 min as a session cron. Rate-limited to avoid
spam: once it alerts, it sits silent for 30 min before alerting again
(unless agent has resumed in the interim).

Usage:
  python3 responsiveness_watchdog.py             # check + alert if needed
  python3 responsiveness_watchdog.py --threshold 20  # custom min threshold
"""
from __future__ import annotations
import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

HOME = Path.home() / "chronicle"
TRACES = HOME / "traces"
ALERT_FLAG = HOME / "data" / "responsiveness_alert_posted"
DB_PATH = "/mnt/hdd/chronicle-data/processed.db"


def latest_agent_signal_min() -> tuple[float, str]:
    """Find newest agent-side signal across multiple sources.
    Returns (age_in_minutes, source_name).

    Sources:
    - Most recent trace file mtime (agent writing traces)
    - Most recent self_model entry timestamp (agent writing observations)

    Discord webhook posts not directly trackable from filesystem; rely
    on traces + self-model as proxies.
    """
    candidates = []

    # 1. Latest trace file
    if TRACES.exists():
        traces = list(TRACES.glob("2026*.md"))
        if traces:
            latest = max(traces, key=lambda p: p.stat().st_mtime)
            age = (time.time() - latest.stat().st_mtime) / 60
            candidates.append((age, f"trace:{latest.name}"))

    # 2. Latest self_model entry
    try:
        conn = sqlite3.connect(DB_PATH, timeout=5)
        row = conn.execute(
            "SELECT MAX(created_at) FROM self_model"
        ).fetchone()
        conn.close()
        if row and row[0]:
            age = (time.time() - row[0]) / 60
            candidates.append((age, "self_model_entry"))
    except Exception:
        pass

    # 3. Latest discord:opus operator post (Chronicle-webhook, ingested by
    # discord_presence cron 2026-04-30 09:02 fix). Tracks what Opus has been
    # posting to #operator. Was missing as a signal source before today —
    # caused false-positive watchdog fires when Opus shifted to journal-default
    # for PULSE engagements (see 2026-04-30 14:38 watchdog noise).
    try:
        conn = sqlite3.connect(DB_PATH, timeout=5)
        row = conn.execute(
            "SELECT MAX(created_at) FROM activity_feed WHERE source='discord:opus'"
        ).fetchone()
        conn.close()
        if row and row[0]:
            age = (time.time() - row[0]) / 60
            candidates.append((age, "operator_post"))
    except Exception:
        pass

    # 4. Latest journal mtime (PULSE engagements + skips + fetch-worthy notes)
    try:
        journal = HOME / "notes" / "pulse_journal.md"
        if journal.is_file():
            age = (time.time() - journal.stat().st_mtime) / 60
            candidates.append((age, "pulse_journal"))
    except Exception:
        pass

    if not candidates:
        return (999999.0, "no_sources_available")

    return min(candidates, key=lambda x: x[0])


def post_alert(message: str) -> None:
    """Post alert to Discord via OPERATOR_WEBHOOK from chronicle.env."""
    env_file = HOME / "chronicle.env"
    webhook = ""
    try:
        for line in env_file.read_text().splitlines():
            if line.startswith("OPERATOR_WEBHOOK="):
                webhook = line.split("=", 1)[1].strip().strip('"').strip("'")
                break
    except Exception:
        return
    if not webhook:
        return
    try:
        payload = json.dumps({"content": f"⚠️ {message}"})
        subprocess.run(
            ["curl", "-sS", "-X", "POST", "-H", "Content-Type: application/json",
             "-A", "chronicle-watchdog/1.0", "-d", payload, webhook],
            capture_output=True, timeout=10,
        )
    except Exception:
        pass


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--threshold", type=float, default=25.0,
                    help="alert threshold in minutes (default 25)")
    ap.add_argument("--cooldown", type=float, default=30.0,
                    help="alert cooldown in minutes (default 30)")
    args = ap.parse_args()

    age_min, source = latest_agent_signal_min()

    if age_min < args.threshold:
        # Agent responsive recently — clear any alert flag
        if ALERT_FLAG.exists():
            try:
                ALERT_FLAG.unlink()
            except Exception:
                pass
        print(f"responsive: latest signal {age_min:.1f}m old ({source})")
        return

    # Threshold exceeded — check cooldown before alerting
    now = int(time.time())
    if ALERT_FLAG.exists():
        try:
            last_alert = int(ALERT_FLAG.read_text().strip() or 0)
            if now - last_alert < args.cooldown * 60:
                print(f"unresponsive {age_min:.1f}m but in cooldown "
                      f"({(now - last_alert) / 60:.1f}m since last alert)")
                return
        except Exception:
            pass

    # Post alert + drop flag
    msg = (f"Responsiveness watchdog: latest agent signal is {age_min:.1f}m old "
           f"(source: {source}). Threshold {args.threshold:.0f}m exceeded. "
           f"Architecture may be functioning but agent unresponsive.")
    post_alert(msg)
    ALERT_FLAG.parent.mkdir(parents=True, exist_ok=True)
    ALERT_FLAG.write_text(str(now))
    print(f"alert posted: {msg}")


if __name__ == "__main__":
    main()
