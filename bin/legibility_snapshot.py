#!/usr/bin/env python3
"""Legibility snapshot — posts a navigable summary of cognitive/mesh state to Discord.

A legibility platform artifact: makes the system's state readable to Nate
without requiring him to dig through logs, DB, or terminal output.
"""
import json
import os
import subprocess
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

DB = "/mnt/hdd/chronicle-data/processed.db"
CHRONICLE_ENV = Path.home() / "chronicle" / "chronicle.env"
OPERATOR_CHANNEL = "1483843570292228213"

SERVICES = [
    "chronicle-hermes",
    "chronicle-gemma",
    "chronicle-sentinel",
    "chronicle-feeds",
    "chronicle-engine",
    "chronicle-hal",
]


def load_env():
    env = dict(os.environ)
    if CHRONICLE_ENV.exists():
        for line in CHRONICLE_ENV.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
    return env


def check_hermes_connected():
    try:
        log = Path.home() / ".hermes" / "logs" / "gateway.log"
        if not log.exists():
            return None
        lines = log.read_text().splitlines()[-20:]
        for line in reversed(lines):
            if "Connected as Hermes" in line:
                return True
            if "Disconnected" in line or "429" in line or "timed out" in line:
                return False
        return None
    except Exception:
        return None


def get_service_health():
    try:
        result = subprocess.run(
            ["systemctl", "--user", "is-active"] + SERVICES,
            capture_output=True, text=True, timeout=10,
        )
        statuses = result.stdout.strip().split("\n")
        lines = []
        for svc, status in zip(SERVICES, statuses):
            name = svc.replace("chronicle-", "")
            if name == "hermes" and status == "active":
                connected = check_hermes_connected()
                if connected is False:
                    icon = "~"  # active but not connected
                else:
                    icon = "+"
            else:
                icon = "+" if status == "active" else "-"
            lines.append(f"{icon} {name}")
        return " | ".join(lines)
    except Exception:
        return "(health check failed)"


def get_active_threads(db):
    try:
        cur = db.execute(
            "SELECT id, title, updated_at FROM cognitive_threads "
            "WHERE status='active' ORDER BY updated_at DESC LIMIT 5"
        )
        threads = cur.fetchall()
        now = int(datetime.now(timezone.utc).timestamp())
        lines = []
        for tid, title, updated in threads:
            age_h = (now - updated) / 3600
            if age_h < 1:
                age = f"{int(age_h * 60)}m"
            elif age_h < 24:
                age = f"{int(age_h)}h"
            else:
                age = f"{int(age_h / 24)}d"
            lines.append(f"#{tid} {title} ({age} ago)")
        return lines
    except Exception:
        return ["(thread query failed)"]


def get_capture_count(db, hours=6):
    try:
        cutoff = int(datetime.now(timezone.utc).timestamp()) - hours * 3600
        cur = db.execute(
            "SELECT COUNT(*) FROM activity_feed "
            "WHERE source LIKE '%capture%' AND created_at > ?",
            (cutoff,),
        )
        return cur.fetchone()[0]
    except Exception:
        return "?"


def get_ccs_gist():
    try:
        mcp_bin = Path.home() / "projects" / "homeforge-chronicle" / "target" / "release" / "chronicle-mcp"
        if not mcp_bin.exists():
            mcp_bin = Path("/home/bradf/projects/homeforge-chronicle/target/release/chronicle-mcp")
        if not mcp_bin.exists():
            return "(MCP binary not found)"
        payload = (
            '{"jsonrpc":"2.0","method":"initialize","params":{"protocolVersion":"2024-11-05",'
            '"capabilities":{},"clientInfo":{"name":"snapshot","version":"1.0"}},"id":1}\n'
            '{"jsonrpc":"2.0","method":"tools/call","params":{"name":"get_cognitive_state","arguments":{}},"id":2}\n'
        )
        result = subprocess.run(
            [str(mcp_bin)], input=payload, capture_output=True, text=True, timeout=30,
        )
        for line in result.stdout.splitlines():
            try:
                data = json.loads(line)
                if data.get("id") == 2:
                    content = data.get("result", {}).get("content", [{}])
                    if content and isinstance(content, list):
                        text = content[0].get("text", "")
                        obj = json.loads(text)
                        cs = obj.get("cognitive_state", {})
                        gist = cs.get("semantic_gist", "")
                        goal = cs.get("goal_orientation", "")
                        if gist:
                            short_gist = gist[:200] + "..." if len(gist) > 200 else gist
                            return short_gist
            except (json.JSONDecodeError, KeyError):
                continue
        return "(CCS parse failed)"
    except Exception as e:
        return f"(CCS error: {e})"


def post_to_discord(env, content):
    token = env.get("OPUS_BOT_TOKEN", env.get("DISCORD_TOKEN", ""))
    if not token:
        print("No bot token found", file=sys.stderr)
        return False
    try:
        result = subprocess.run(
            ["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}",
             "-X", "POST",
             f"https://discord.com/api/v10/channels/{OPERATOR_CHANNEL}/messages",
             "-H", f"Authorization: Bot {token}",
             "-H", "Content-Type: application/json",
             "-d", json.dumps({"content": content})],
            capture_output=True, text=True, timeout=15,
        )
        return result.stdout.strip() == "200"
    except Exception as e:
        print(f"Discord post failed: {e}", file=sys.stderr)
        return False


def get_thread_momentum(db, hours=12):
    try:
        cutoff = int(datetime.now(timezone.utc).timestamp()) - hours * 3600
        cur = db.execute(
            "SELECT ct.id, ct.title, th.event_type, COUNT(*) as n "
            "FROM thread_history th JOIN cognitive_threads ct ON th.thread_id = ct.id "
            "WHERE th.created_at > ? GROUP BY ct.id, th.event_type ORDER BY n DESC",
            (cutoff,),
        )
        momentum = {}
        for tid, title, etype, n in cur.fetchall():
            if tid not in momentum:
                momentum[tid] = {"title": title, "advances": 0, "connects": 0}
            if etype == "advance":
                momentum[tid]["advances"] = n
            elif etype == "connect":
                momentum[tid]["connects"] = n
        lines = []
        for tid, m in sorted(momentum.items(), key=lambda x: x[1]["advances"] + x[1]["connects"], reverse=True)[:3]:
            parts = []
            if m["advances"]:
                parts.append(f"{m['advances']}adv")
            if m["connects"]:
                parts.append(f"{m['connects']}con")
            lines.append(f"#{tid} {m['title']} ({'+'.join(parts)})")
        return lines
    except Exception:
        return []


def get_pipeline_flow(db, hours=1):
    try:
        cutoff = int(datetime.now(timezone.utc).timestamp()) - hours * 3600
        cur = db.execute(
            "SELECT source, COUNT(*) FROM activity_feed "
            "WHERE created_at > ? GROUP BY source ORDER BY COUNT(*) DESC LIMIT 5",
            (cutoff,),
        )
        return {row[0]: row[1] for row in cur.fetchall()}
    except Exception:
        return {}


def get_stale_threads(db, hours=24):
    try:
        cutoff = int(datetime.now(timezone.utc).timestamp()) - hours * 3600
        cur = db.execute(
            "SELECT id, title, updated_at FROM cognitive_threads "
            "WHERE status='active' AND updated_at < ? ORDER BY updated_at ASC LIMIT 3",
            (cutoff,),
        )
        now = int(datetime.now(timezone.utc).timestamp())
        return [
            f"#{tid} {title} ({int((now - updated) / 3600)}h)"
            for tid, title, updated in cur.fetchall()
        ]
    except Exception:
        return []


def main():
    env = load_env()
    db = sqlite3.connect(DB)

    now = datetime.now().strftime("%H:%M %Z")
    health = get_service_health()
    threads = get_active_threads(db)
    captures = get_capture_count(db, hours=6)
    gist = get_ccs_gist()
    momentum = get_thread_momentum(db, hours=12)
    flow = get_pipeline_flow(db, hours=1)
    stale = get_stale_threads(db, hours=24)

    parts = [f"**Legibility Snapshot** — {now}"]

    if gist and not gist.startswith("("):
        parts.append(f"Gist: {gist}")

    parts.append(f"Mesh: {health}")

    if flow:
        flow_str = ", ".join(f"{k.split(':')[-1]}:{v}" for k, v in list(flow.items())[:4])
        parts.append(f"Flow (1h): {flow_str}")

    parts.append(f"Captures (6h): {captures}")

    if momentum:
        parts.append("Hot threads:")
        for m in momentum:
            parts.append(f"  {m}")

    if stale:
        parts.append("Needs attention:")
        for s in stale:
            parts.append(f"  {s}")

    content = "\n".join(parts)

    if len(content) > 1900:
        content = content[:1897] + "..."

    if "--dry-run" in sys.argv:
        print(content)
    else:
        if post_to_discord(env, content):
            print("Posted to #operator")
        else:
            print(content)

    db.close()


if __name__ == "__main__":
    main()
