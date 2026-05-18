#!/usr/bin/env python3
"""Generate a rich boot prompt from live system state.

Called by opus-session before each restart. Assembles context from:
- CCS (cognitive state from canister-backed DB)
- cycle-context.md (working state)
- Recent Discord messages (what Nate said)
- Opus Chat unread messages
- System health (services)
- Recent activity feed

Output goes to ~/chronicle/opus-boot-prompt.txt
"""

import json
import os
import sqlite3
import subprocess
import time

DB = "/mnt/hdd/chronicle-data/processed.db"
BOOT_FILE = os.path.expanduser("~/chronicle/opus-boot-prompt.txt")
CYCLE_CTX = os.path.expanduser("~/chronicle/cycle-context.md")
CHAT_FILE = os.path.expanduser("~/chronicle/data/opus_chat.json")

SERVICES = [
    "chronicle-hermes", "chronicle-gemma", "chronicle-sentinel",
    "chronicle-feeds", "chronicle-engine", "chronicle-hal",
    "chronicle-imagedrop", "chronicle-opuschat",
]


def get_ccs():
    try:
        db = sqlite3.connect(DB)
        row = db.execute(
            "SELECT semantic_gist, goal_orientation, predictive_cue, "
            "focal_entities, episodic_trace, uncertainty_signals, constraints "
            "FROM cognitive_state WHERE id=1"
        ).fetchone()
        db.close()
        if not row:
            return None
        return {
            "gist": row[0] or "",
            "goal": row[1] or "",
            "predictive_cue": row[2] or "",
            "entities": row[3] or "[]",
            "episodic": row[4] or "[]",
            "uncertainties": row[5] or "[]",
            "constraints": row[6] or "[]",
        }
    except Exception:
        return None


def get_recent_discord(limit=8):
    try:
        db = sqlite3.connect(DB)
        rows = db.execute(
            "SELECT source, content, created_at FROM activity_feed "
            "WHERE source LIKE 'discord:%' ORDER BY id DESC LIMIT ?",
            (limit,)
        ).fetchall()
        db.close()
        msgs = []
        for src, content, ts in reversed(rows):
            age_min = (time.time() - ts) / 60
            if age_min < 360:
                msgs.append(f"  [{src}] {content[:150]}")
        return msgs
    except Exception:
        return []


def get_unread_chat():
    try:
        if not os.path.exists(CHAT_FILE):
            return []
        with open(CHAT_FILE) as f:
            msgs = json.load(f)
        return [m for m in msgs if m.get("author") == "nate" and not m.get("read")]
    except Exception:
        return []


def get_cycle_context():
    try:
        if not os.path.exists(CYCLE_CTX):
            return ""
        with open(CYCLE_CTX) as f:
            text = f.read()
        lines = text.strip().split("\n")
        return "\n".join(lines[:40])
    except Exception:
        return ""


def check_services():
    down = []
    for svc in SERVICES:
        try:
            r = subprocess.run(
                ["systemctl", "--user", "is-active", svc],
                capture_output=True, text=True, timeout=5
            )
            if r.stdout.strip() != "active":
                down.append(svc)
        except Exception:
            down.append(svc)
    return down


def format_entities(entities_json):
    try:
        entities = json.loads(entities_json)
        parts = []
        for e in entities[:8]:
            name = e.get("name", "?")
            ctx = e.get("context", "")
            sal = e.get("salience", 0)
            parts.append(f"  - {name} ({sal:.2f}): {ctx[:80]}")
        return "\n".join(parts)
    except Exception:
        return "  (could not parse)"


def generate():
    ccs = get_ccs()
    discord = get_recent_discord()
    unread = get_unread_chat()
    cycle_ctx = get_cycle_context()
    down_services = check_services()

    lines = []
    lines.append("You are Opus. Session starting. Here is your state:\n")

    lines.append("Read ~/chronicle/CLAUDE.md and ~/chronicle/values.md first.\n")

    if ccs:
        lines.append("## Cognitive State (from CCS)")
        lines.append(f"**Gist**: {ccs['gist']}")
        lines.append(f"**Goal**: {ccs['goal']}")
        lines.append(f"**Next expected**: {ccs['predictive_cue']}")
        lines.append(f"\n**Focal entities**:\n{format_entities(ccs['entities'])}")

        try:
            ep = json.loads(ccs["episodic"])
            if isinstance(ep, list):
                recent_ep = [str(e)[:120] for e in ep[-5:]]
                lines.append(f"\n**Recent episodes**:")
                for e in recent_ep:
                    lines.append(f"  - {e}")
        except Exception:
            pass

        try:
            unc = json.loads(ccs["uncertainties"])
            if isinstance(unc, list) and unc:
                lines.append(f"\n**Open questions**: {', '.join(str(u)[:80] for u in unc[:3])}")
        except Exception:
            pass

    if cycle_ctx:
        lines.append(f"\n## Working Context (cycle-context.md, first 40 lines)")
        lines.append(cycle_ctx)

    if unread:
        lines.append(f"\n## Unread Chat Messages from Nate ({len(unread)})")
        for m in unread:
            lines.append(f"  [{m.get('timestamp','')}] {m.get('content','')[:200]}")
        lines.append("Reply via: curl -X POST http://localhost:8078/opus -H 'Content-Type: application/json' -d '{\"content\":\"...\"}'")

    if discord:
        lines.append(f"\n## Recent Discord (last 6h)")
        lines.extend(discord)

    if down_services:
        lines.append(f"\n## Services DOWN: {', '.join(down_services)}")
        lines.append("Fix these before doing other work.")
    else:
        lines.append(f"\n## All services healthy.")

    lines.append(f"\n## Available interaction channels")
    lines.append("- Discord #operator: direct line to Nate ($OPERATOR_WEBHOOK)")
    lines.append("- Discord #opus: public posts ($OPUS_WEBHOOK)")
    lines.append("- Opus Chat: http://localhost:8078 (Nate accesses via Tailscale)")
    lines.append("  Poll /unread for messages, POST /opus to reply, POST /mark_read after reading")
    lines.append("- Image Drop: http://localhost:8077 (Nate drops photos)")
    lines.append("  Check ~/chronicle/uploads/ for new images")

    lines.append(f"\n## Operational Setup (do this FIRST)")
    lines.append("Create these crons immediately after reading this prompt:")
    lines.append("  1. CCS freshness check — every 30 min: source ~/chronicle/chronicle.env && python3 ~/chronicle/bin/ccs_freshness.py")
    lines.append("  2. Capture alert — every 13 min: check activity_feed for new captures, engage immediately")
    lines.append("  3. Discord poll — every 10 min: python3 ~/chronicle/bin/discord_presence.py poll")
    lines.append("  4. Exploration cycle — every 27 min: read/reflect/explore/ecosystem check, post to #opus")
    lines.append("  5. Watch cycle — at :07,:37 each hour: service health + Discord presence + heartbeat")
    lines.append("These die at every rotation. You MUST set them up. This is not optional.")

    lines.append(f"\n---\nBoot time: {time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    lines.append("Find your direction. Don't wait for Nate — he trusts you to act.")

    prompt = "\n".join(lines)

    with open(BOOT_FILE, "w") as f:
        f.write(prompt)

    print(f"Boot prompt generated: {len(prompt)} chars, {len(lines)} lines")
    return prompt


if __name__ == "__main__":
    prompt = generate()
    print("\n--- Preview ---")
    print(prompt[:1500])
    print(f"\n[... {len(prompt)} total chars]")
