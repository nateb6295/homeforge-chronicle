#!/usr/bin/env python3
"""Pulse check — consolidated signal scanner.

Replaces separate capture-alert, discord-poll, service-health, and CCS-freshness
crons with one fast pass. Only outputs actionable items. Quiet = one-line summary.

Checks:
  1. New captures (last 15 min, unprocessed)
  2. Discord messages from Nate or Hermes (via discord_presence.py poll)
  3. Service health (hermes, gemma, sentinel)
  4. CCS age (flag if > 45 min with activity)

Exit codes:
  0 = something needs attention (output describes what)
  1 = all quiet (one-line summary only)

Usage:
  pulse_check.py              # full check, human-readable output
  pulse_check.py --json       # machine-readable output
  pulse_check.py --quiet      # suppress "all quiet" output
"""
import json
import os
import sqlite3
import subprocess
import sys
import time

DB = "/mnt/hdd/chronicle-data/processed.db"
CAPTURE_WINDOW = 900  # 15 min
CCS_STALE_THRESHOLD = 2700  # 45 min
ACTIVITY_MIN = 3

# Track captures we've already flagged so we don't re-alert
STATE_FILE = os.path.expanduser("~/chronicle/data/pulse_last_capture_id.txt")
NATE_STATE_FILE = os.path.expanduser("~/chronicle/data/pulse_last_nate_id.txt")


def get_last_alerted_capture():
    try:
        with open(STATE_FILE) as f:
            return int(f.read().strip())
    except (FileNotFoundError, ValueError):
        return 0


def save_last_alerted_capture(cap_id):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        f.write(str(cap_id))


def get_last_alerted_nate():
    try:
        with open(NATE_STATE_FILE) as f:
            return int(f.read().strip())
    except (FileNotFoundError, ValueError):
        return 0


def save_last_alerted_nate(msg_id):
    os.makedirs(os.path.dirname(NATE_STATE_FILE), exist_ok=True)
    with open(NATE_STATE_FILE, "w") as f:
        f.write(str(msg_id))


def check_captures(db):
    """Check for new captures since last alert."""
    last_alerted = get_last_alerted_capture()
    cutoff = int(time.time()) - CAPTURE_WINDOW
    rows = db.execute(
        "SELECT id, content FROM activity_feed "
        "WHERE source IN ('discord:capture','operator:capture','nate:capture') "
        "AND created_at > ? AND id > ? "
        "ORDER BY created_at DESC LIMIT 10",
        (cutoff, last_alerted),
    ).fetchall()
    if rows:
        save_last_alerted_capture(rows[0][0])
    return [{"id": r[0], "content": r[1]} for r in rows]


def check_discord():
    """Poll Discord for new messages. Returns parsed output."""
    try:
        result = subprocess.run(
            ["python3", os.path.expanduser("~/chronicle/bin/discord_presence.py"), "poll"],
            capture_output=True, text=True, timeout=15,
            env={**os.environ},
        )
        output = result.stdout.strip()
        if "No new" in output or not output:
            return []
        messages = []
        for line in output.split("\n"):
            if line.strip().startswith("#"):
                messages.append(line.strip())
        return messages
    except (subprocess.TimeoutExpired, Exception):
        return []


def check_services():
    """Check critical services. Returns list of down services."""
    down = []
    for svc in ["chronicle-hermes", "chronicle-gemma", "chronicle-sentinel"]:
        try:
            result = subprocess.run(
                ["systemctl", "--user", "is-active", svc],
                capture_output=True, text=True, timeout=5,
            )
            if result.stdout.strip() != "active":
                down.append(svc)
        except Exception:
            down.append(svc)
    return down


def check_ccs(db):
    """Check CCS freshness. Returns age in minutes or None if fresh."""
    row = db.execute("SELECT updated_at FROM cognitive_state LIMIT 1").fetchone()
    if not row:
        return None
    age = time.time() - row[0]
    age_min = age / 60
    if age < CCS_STALE_THRESHOLD:
        return None
    activity = db.execute(
        "SELECT COUNT(*) FROM activity_feed WHERE created_at > ? "
        "AND source IN ('discord:nate', 'discord:capture', 'discord:nate:crosschain')",
        (int(time.time()) - max(int(age), 1800),)
    ).fetchone()
    if activity and activity[0] >= ACTIVITY_MIN:
        return round(age_min)
    return None


def check_nate_messages(db):
    """Check for recent Nate messages that might need response."""
    last_id = get_last_alerted_nate()
    rows = db.execute(
        "SELECT id, content FROM activity_feed "
        "WHERE source LIKE '%nate%' AND source != 'discord:nate:to_gemma' AND id > ? "
        "ORDER BY created_at DESC LIMIT 3",
        (last_id,)
    ).fetchall()
    if rows:
        save_last_alerted_nate(max(r[0] for r in rows))
    return [r[1][:200] for r in rows]


def main():
    args = sys.argv[1:]
    quiet = "--quiet" in args
    as_json = "--json" in args

    db = sqlite3.connect(DB, timeout=10)

    captures = check_captures(db)
    discord = check_discord()
    down_services = check_services()
    ccs_stale = check_ccs(db)
    nate_msgs = check_nate_messages(db)

    db.close()

    alerts = []

    if down_services:
        alerts.append(("SERVICE_DOWN", down_services))

    if nate_msgs:
        for msg in nate_msgs:
            if "nate" in msg.lower()[:50] or "chat" in msg.lower()[:20]:
                alerts.append(("NATE_MESSAGE", nate_msgs))
                break

    if captures:
        alerts.append(("NEW_CAPTURES", captures))

    if discord:
        # Filter out our own messages
        external = [m for m in discord if "[Opus]" not in m and "[Chronicle]" not in m]
        if external:
            alerts.append(("DISCORD_MESSAGES", external))

    if ccs_stale:
        alerts.append(("CCS_STALE", ccs_stale))

    if as_json:
        print(json.dumps({"alerts": alerts, "quiet": len(alerts) == 0}))
        return 0 if alerts else 1

    if not alerts:
        if not quiet:
            print(f"All quiet. Services: OK | Captures: none new | CCS: fresh | Discord: no messages")
        return 1

    # Output actionable items
    for atype, data in alerts:
        if atype == "SERVICE_DOWN":
            print(f"⚠️  SERVICES DOWN: {', '.join(data)}")
        elif atype == "NEW_CAPTURES":
            print(f"📥 {len(data)} new capture(s):")
            for c in data:
                print(f"  [{c['id']}] {c['content'][:150]}")
        elif atype == "DISCORD_MESSAGES":
            print(f"💬 Discord messages:")
            for m in data:
                print(f"  {m[:150]}")
        elif atype == "NATE_MESSAGE":
            print(f"👤 Nate message detected")
        elif atype == "CCS_STALE":
            print(f"🧠 CCS stale: {data}min old with activity")

    return 0


if __name__ == "__main__":
    sys.exit(main())
