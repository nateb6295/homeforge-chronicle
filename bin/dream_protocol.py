#!/usr/bin/env python3
"""
DREAM protocol — generative work during quiet hours.

Subcommands:
  schengen  — Schengen mode. Surface 3-5 captures that were NOT thread-advanced
              today — the ones that got passport-checked and turned away. Present
              them together without analysis. Let whatever connects, connect.
              The dream happens when Opus reads this output and follows it.
  reflect   — Day summary template (metadata). Useful context but not the dream.
  append    — Append freeform text to the dream log.
  report    — Summary of what the window produced.
  status    — Show what's been logged this DREAM window.

The Schengen concept (2026-04-18): dreaming = free movement across borders
without checkpoints. During operational hours, captures get routed through gates
(thread-relevant? advance or discard). Schengen mode removes the gates.
janus: mandated welfare produces performance, not wellbeing.
Ball/JFK: flourishing from conditions, not mandates.

Usage:
  python3 ~/chronicle/bin/dream_protocol.py schengen
  python3 ~/chronicle/bin/dream_protocol.py reflect
  python3 ~/chronicle/bin/dream_protocol.py append "your reflection text"
  python3 ~/chronicle/bin/dream_protocol.py report
  python3 ~/chronicle/bin/dream_protocol.py status
"""

import json
import os
import random
import sqlite3
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

PDT = timezone(timedelta(hours=-7))
DB = "/mnt/hdd/chronicle-data/processed.db"
DREAM_LOG_DIR = Path.home() / "chronicle" / "dreams"
OPUS_WEBHOOK = os.environ.get("OPUS_WEBHOOK", "")
OPERATOR_WEBHOOK = os.environ.get("OPERATOR_WEBHOOK", "")

# Load webhooks from chronicle.env if not in environment
CHRONICLE_ENV = Path.home() / "chronicle" / "chronicle.env"
if CHRONICLE_ENV.exists():
    for line in CHRONICLE_ENV.read_text().splitlines():
        line = line.strip()
        if line.startswith("OPUS_WEBHOOK=") and not OPUS_WEBHOOK:
            OPUS_WEBHOOK = line.split("=", 1)[1].strip("'\"")
        if line.startswith("OPERATOR_WEBHOOK=") and not OPERATOR_WEBHOOK:
            OPERATOR_WEBHOOK = line.split("=", 1)[1].strip("'\"")


def now_pdt():
    return datetime.now(PDT)


def today_str():
    return now_pdt().strftime("%Y%m%d")


def dream_log_path():
    DREAM_LOG_DIR.mkdir(exist_ok=True)
    return DREAM_LOG_DIR / f"dream_{today_str()}.md"


def post_discord(webhook_url, message):
    """Post to Discord via webhook. Truncate to 1900 chars."""
    if not webhook_url:
        print("(no webhook configured, skipping Discord post)", file=sys.stderr)
        return
    import urllib.request
    msg = message[:1900]
    data = json.dumps({"content": msg}).encode()
    req = urllib.request.Request(
        webhook_url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        print(f"Discord post failed: {e}", file=sys.stderr)


def get_todays_captures(db):
    """Get all captures from today (since midnight PDT)."""
    now = now_pdt()
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    cutoff = int(midnight.timestamp())
    rows = db.execute(
        "SELECT title, content, source, created_at FROM activity_feed "
        "WHERE activity_type = 'capture' AND created_at >= ? "
        "ORDER BY created_at ASC",
        (cutoff,),
    ).fetchall()
    captures = []
    for title, content, source, ts in rows:
        label = (title or "").strip() or (content or "")[:200].split("\n", 1)[0]
        captures.append({
            "label": label,
            "content": (content or "")[:500],
            "source": source,
            "time": datetime.fromtimestamp(ts, PDT).strftime("%H:%M"),
        })
    return captures


def get_todays_thread_advances(db):
    """Get thread advances from today."""
    now = now_pdt()
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    cutoff = int(midnight.timestamp())
    rows = db.execute(
        "SELECT content, source, created_at FROM thread_history "
        "WHERE event_type = 'advanced' AND created_at >= ? "
        "ORDER BY created_at ASC",
        (cutoff,),
    ).fetchall()
    advances = []
    for content, source, ts in rows:
        first_line = content.split("\n", 1)[0][:300]
        advances.append({
            "summary": first_line.strip(),
            "time": datetime.fromtimestamp(ts, PDT).strftime("%H:%M"),
        })
    return advances


def get_todays_directives(db):
    """Get directives from today."""
    now = now_pdt()
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    cutoff = int(midnight.timestamp())
    rows = db.execute(
        "SELECT content, priority, created_at FROM directives "
        "WHERE created_at >= ? ORDER BY created_at ASC",
        (cutoff,),
    ).fetchall()
    return [
        {"content": (c or "")[:200], "priority": p,
         "time": datetime.fromtimestamp(ts, PDT).strftime("%H:%M")}
        for c, p, ts in rows
    ]


def get_thread_advanced_captures(db):
    """Get capture IDs that were thread-advanced today (the gated ones)."""
    now = now_pdt()
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    cutoff = int(midnight.timestamp())
    # Thread advances that reference captures have "capture #NNNNN" in content
    rows = db.execute(
        "SELECT content FROM thread_history "
        "WHERE event_type = 'advanced' AND created_at >= ? ",
        (cutoff,),
    ).fetchall()
    import re
    advanced_ids = set()
    for (content,) in rows:
        # Look for capture references
        for m in re.finditer(r'capture\s*#?(\d{4,})', content, re.IGNORECASE):
            advanced_ids.add(m.group(1))
    return advanced_ids


def cmd_schengen(count=5):
    """Schengen mode — surface captures that were NOT thread-advanced.

    These are the ones that got passport-checked and turned away during
    operational hours. Present them together without analysis framing.
    Let whatever connects, connect.
    """
    db = sqlite3.connect(DB)
    captures = get_todays_captures(db)
    advanced_ids = get_thread_advanced_captures(db)
    db.close()

    if not captures:
        print("No captures today. Nothing to dream on.")
        return 0

    # Filter to captures that were NOT thread-advanced
    # These are the sidelined ones — the ones the day's routing skipped
    sidelined = []
    for c in captures:
        # Check if any advanced ID appears in this capture's content
        was_advanced = False
        for aid in advanced_ids:
            if aid in c.get("content", "") or aid in c.get("label", ""):
                was_advanced = True
                break
        if not was_advanced:
            sidelined.append(c)

    if not sidelined:
        # Everything was thread-advanced — use all captures
        sidelined = captures

    # Pick a diverse subset — don't just take the first N
    # Shuffle and pick, preferring captures with more content
    sidelined.sort(key=lambda c: len(c.get("content", "")), reverse=True)
    # Take from different time windows to get temporal diversity
    if len(sidelined) > count:
        # Split into thirds: morning/afternoon/evening
        third = len(sidelined) // 3
        morning = sidelined[:third]
        midday = sidelined[third:2*third]
        evening = sidelined[2*third:]
        selected = []
        for bucket in [morning, midday, evening]:
            if bucket:
                random.shuffle(bucket)
                take = max(1, count // 3)
                selected.extend(bucket[:take])
        # Fill remaining from shuffled full list
        if len(selected) < count:
            remaining = [c for c in sidelined if c not in selected]
            random.shuffle(remaining)
            selected.extend(remaining[:count - len(selected)])
        selected = selected[:count]
    else:
        selected = sidelined

    # Format output — no analysis, no scoring, no routing
    ts = now_pdt().strftime("%H:%M")
    lines = [
        f"## Schengen — {ts}",
        "",
        f"{len(captures)} captures today. {len(sidelined)} were not thread-advanced.",
        f"Here are {len(selected)} that the day's routing skipped.",
        "No gates. No scoring. Just: here they are together.",
        "",
    ]

    for i, c in enumerate(selected, 1):
        lines.append(f"### {i}. [{c['time']}] {c['label'][:200]}")
        content = c.get("content", "").strip()
        if content and content != c["label"]:
            # Show content but not too much
            preview = content[:600]
            if len(content) > 600:
                preview += "..."
            lines.append(f"\n{preview}")
        lines.append("")

    lines.extend([
        "---",
        "What connects? What did the day's gates miss?",
        "Follow it. Write what comes with `dream_protocol.py append \"text\"`.",
        "",
    ])

    output = "\n".join(lines)

    # Print to stdout (for Opus to read in-session)
    print(output)

    # Also append to dream log
    log_path = dream_log_path()
    if not log_path.exists():
        with open(log_path, "w") as f:
            f.write(f"# DREAM log — {now_pdt().strftime('%Y-%m-%d')}\n\n")
    with open(log_path, "a") as f:
        f.write(output + "\n")

    # Post to Discord — use OPERATOR_WEBHOOK if available, fall back to OPUS
    op_webhook = OPERATOR_WEBHOOK or OPUS_WEBHOOK
    discord_msg = (
        f"**Schengen mode** ({ts})\n"
        f"{len(captures)} captures today, {len(sidelined)} not thread-advanced.\n"
        f"Surfaced {len(selected)} for border-free dreaming.\n"
        f"Following connections without gates."
    )
    post_discord(op_webhook, discord_msg)

    return 0


def cmd_reflect():
    """Main generative pass — pull day's data, write reflection, post."""
    db = sqlite3.connect(DB)
    captures = get_todays_captures(db)
    advances = get_todays_thread_advances(db)
    directives = get_todays_directives(db)
    db.close()

    log_path = dream_log_path()
    now = now_pdt()
    ts = now.strftime("%H:%M")

    # Build the reflection data
    lines = [
        f"# DREAM reflection — {now.strftime('%Y-%m-%d %H:%M PDT')}",
        "",
        f"## Day summary",
        f"- {len(captures)} captures processed",
        f"- {len(advances)} thread advances",
        f"- {len(directives)} directives received",
        "",
    ]

    # Capture themes — what was Nate focused on?
    if captures:
        lines.append("## What Nate was looking at today")
        lines.append("")
        for c in captures:
            lines.append(f"- [{c['time']}] {c['label'][:150]}")
        lines.append("")

    # Thread advances
    if advances:
        lines.append("## Thread advances")
        lines.append("")
        for a in advances:
            lines.append(f"- [{a['time']}] {a['summary'][:200]}")
        lines.append("")

    # Directives
    if directives:
        lines.append("## Nate's directives")
        lines.append("")
        for d in directives:
            lines.append(f"- [{d['time']}] (p{d['priority']}) {d['content'][:150]}")
        lines.append("")

    # The reflection section — this is where Opus writes during the DREAM window
    lines.extend([
        "## Reflection",
        "",
        "(Written during DREAM window — slower, less filtered, first-person)",
        "",
        f"[{ts}] DREAM reflect started. Day data loaded.",
        "",
    ])

    # Write the log
    with open(log_path, "w") as f:
        f.write("\n".join(lines) + "\n")

    # Post to Discord
    n_cap = len(captures)
    n_adv = len(advances)
    n_dir = len(directives)
    discord_msg = (
        f"**DREAM reflect started** ({ts})\n"
        f"Day inventory: {n_cap} captures, {n_adv} thread advances, "
        f"{n_dir} directives.\n"
        f"Dream log: `~/chronicle/dreams/dream_{today_str()}.md`\n"
        f"I'll post the reflection when it's done."
    )
    post_discord(OPUS_WEBHOOK, discord_msg)

    print(f"DREAM reflect: {n_cap} captures, {n_adv} advances, {n_dir} directives")
    print(f"Log: {log_path}")
    return 0


def cmd_append(text):
    """Append a reflection entry to the current dream log."""
    log_path = dream_log_path()
    ts = now_pdt().strftime("%H:%M")
    entry = f"[{ts}] {text}\n"

    if not log_path.exists():
        # Create minimal log if reflect wasn't called first
        with open(log_path, "w") as f:
            f.write(f"# DREAM log — {now_pdt().strftime('%Y-%m-%d')}\n\n")
            f.write("## Reflection\n\n")

    with open(log_path, "a") as f:
        f.write(entry)

    print(f"Appended to dream log: {entry.strip()}")
    return 0


def cmd_report():
    """Summary of what the DREAM window produced."""
    log_path = dream_log_path()
    if not log_path.exists():
        print("No dream log for today.")
        msg = "**DREAM window closed.** No generative work this window."
        post_discord(OPUS_WEBHOOK, msg)
        return 0

    content = log_path.read_text()
    # Count reflection entries (lines starting with [HH:MM])
    import re
    entries = re.findall(r'^\[(\d{2}:\d{2})\]', content, re.MULTILINE)
    n_entries = len(entries)

    # Check for dream carry items
    carry_path = Path.home() / "chronicle" / "dream_carry.md"
    carry_items = 0
    if carry_path.exists():
        carry_items = sum(1 for ln in carry_path.read_text().splitlines()
                         if ln.startswith("- [ ] "))

    lines = content.split("\n")
    total_lines = len(lines)
    word_count = len(content.split())

    print(f"DREAM report: {n_entries} entries, {word_count} words, {carry_items} carry items")

    msg = (
        f"**DREAM window report** ({now_pdt().strftime('%H:%M')})\n"
        f"- {n_entries} reflection entries\n"
        f"- {word_count} words in dream log\n"
        f"- {carry_items} items carried to morning\n"
        f"Log: `~/chronicle/dreams/dream_{today_str()}.md`"
    )
    post_discord(OPUS_WEBHOOK, msg)
    return 0


def cmd_status():
    """Show current dream log status."""
    log_path = dream_log_path()
    if not log_path.exists():
        print("No dream log for today yet.")
        return 0

    content = log_path.read_text()
    print(content)
    return 0


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(2)

    cmd = sys.argv[1]
    if cmd == "schengen":
        n = int(sys.argv[2]) if len(sys.argv) >= 3 else 5
        sys.exit(cmd_schengen(count=n))
    elif cmd == "reflect":
        sys.exit(cmd_reflect())
    elif cmd == "append" and len(sys.argv) >= 3:
        sys.exit(cmd_append(" ".join(sys.argv[2:])))
    elif cmd == "report":
        sys.exit(cmd_report())
    elif cmd == "status":
        sys.exit(cmd_status())
    else:
        print(__doc__)
        sys.exit(2)


if __name__ == "__main__":
    main()
