#!/usr/bin/env python3
"""Sitrep — single-command state recovery after context rotation.

Run this when you wake up from auto-compact. It gives you everything
you need to know in one shot, no digging required.

Usage:
  python3 sitrep.py          # full sitrep
  python3 sitrep.py --brief  # compact version
"""

import sqlite3
import subprocess
import time
import os
import json
import glob as glob_mod

DB_PATH = os.path.expanduser("~/.homeforge-chronicle/processed.db")
TRACE_DIR = os.path.expanduser("~/chronicle/traces")
CHRONICLE_DIR = os.path.expanduser("~/chronicle")


def run(cmd, timeout=10):
    """Run a shell command, return stdout or error string."""
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        return r.stdout.strip()
    except Exception as e:
        return f"ERROR: {e}"


def services_status():
    """Check all chronicle services."""
    services = [
        "chronicle-gemma", "chronicle-intern", "chronicle-crossref",
        "chronicle-provocateur", "chronicle-sentinel", "chronicle-feeds",
        "chronicle-engine", "chronicle-scribe", "chronicle-hal",
        "chronicle-transcriber"
    ]
    results = {}
    for svc in services:
        out = run(f"systemctl --user is-active {svc}")
        results[svc] = out.strip()
    return results


def voice_counts():
    """Voice status counts."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=5)
        row = conn.execute(
            "SELECT "
            "SUM(CASE WHEN status='unread' THEN 1 ELSE 0 END) as unread, "
            "SUM(CASE WHEN status='decayed' THEN 1 ELSE 0 END) as decayed, "
            "SUM(CASE WHEN status='responded' THEN 1 ELSE 0 END) as responded, "
            "COUNT(*) as total "
            "FROM agent_voice"
        ).fetchone()
        conn.close()
        return {
            "unread": row[0] or 0,
            "decayed": row[1] or 0,
            "responded": row[2] or 0,
            "total": row[3] or 0,
        }
    except Exception as e:
        return {"error": str(e)}


def recent_voices(limit=5):
    """Most recent unread voices."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=5)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT id, agent, voice_type, substr(content,1,120) as preview, created_at "
            "FROM agent_voice WHERE status='unread' ORDER BY created_at DESC LIMIT ?",
            (limit,)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        return []


def active_threads():
    """Active threads from thread table."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=5)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT id, title, status, created_at, updated_at FROM cognitive_threads "
            "WHERE status='active' ORDER BY updated_at DESC LIMIT 3"
        ).fetchall()
        result = []
        for r in rows:
            adv_count = conn.execute(
                "SELECT COUNT(*) FROM thread_history WHERE thread_id=? AND event_type='advanced'",
                (r["id"],)
            ).fetchone()[0]
            challenge_count = conn.execute(
                "SELECT COUNT(*) FROM thread_history WHERE thread_id=? AND event_type='challenge'",
                (r["id"],)
            ).fetchone()[0]
            result.append({
                "id": r["id"],
                "title": r["title"],
                "advancements": adv_count,
                "challenges": challenge_count,
                "updated": r["updated_at"],
            })
        conn.close()
        return result
    except Exception as e:
        return [{"error": str(e)}]


def recent_activity(minutes=30):
    """Recent activity feed items."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=5)
        conn.row_factory = sqlite3.Row
        cutoff = int(time.time()) - (minutes * 60)
        rows = conn.execute(
            "SELECT source, activity_type, substr(content,1,80) as preview "
            "FROM activity_feed WHERE created_at > ? ORDER BY created_at DESC LIMIT 10",
            (cutoff,)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        return []


def last_trace():
    """Find and read the most recent trace file."""
    import re
    pattern = os.path.join(TRACE_DIR, "*.md")
    all_files = glob_mod.glob(pattern)
    # Only match YYYYMMDD_HHMM.md trace files
    files = sorted([f for f in all_files if re.search(r'/\d{8}_\d{4}\.md$', f)])
    if not files:
        return None, None
    latest = files[-1]
    name = os.path.basename(latest)
    try:
        with open(latest) as f:
            content = f.read()
        return name, content
    except Exception:
        return name, "ERROR reading"


def spot_check_health():
    """Current spot check constraint level."""
    path = os.path.expanduser("~/chronicle/spot_check_health.json")
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return {"status": "file not found"}


def pending_directives():
    """Check for pending directives."""
    out = run("python3 ~/chronicle/bin/read_directives.py")
    return out


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Sitrep — state recovery")
    parser.add_argument("--brief", action="store_true", help="Compact output")
    args = parser.parse_args()

    now = time.strftime("%Y-%m-%d %H:%M %Z")
    print(f"═══ SITREP — {now} ═══\n")

    # Services
    svcs = services_status()
    green = [s.replace("chronicle-", "") for s, v in svcs.items() if v == "active"]
    red = [s.replace("chronicle-", "") for s, v in svcs.items() if v != "active"]
    print(f"SERVICES: {len(green)} green" + (f", {len(red)} DOWN: {', '.join(red)}" if red else ""))
    if not args.brief and red:
        for s in red:
            print(f"  ✗ chronicle-{s}: {svcs[f'chronicle-{s}']}")

    # Threads
    threads = active_threads()
    print(f"\nTHREADS: {len(threads)} active")
    for t in threads:
        if "error" in t:
            print(f"  ERROR: {t['error']}")
        else:
            age = (time.time() - t["updated"]) / 60
            print(f"  #{t['id']}: {t['title']}")
            print(f"    {t['advancements']} advancements, {t['challenges']} challenges, updated {int(age)}m ago")

    # Voices
    vc = voice_counts()
    print(f"\nVOICES: {vc.get('unread', '?')} unread, {vc.get('decayed', '?')} decayed, {vc.get('total', '?')} total")
    if not args.brief:
        recent = recent_voices(5)
        if recent:
            print("  Recent unread:")
            for v in recent:
                age = (time.time() - v["created_at"]) / 60
                ago = f"{int(age)}m" if age < 60 else f"{age/60:.1f}h"
                print(f"    #{v['id']} [{v['agent']}] {v['voice_type']} ({ago}): {v['preview']}")

    # Spot check
    health = spot_check_health()
    cl = health.get("constraint_level", "?")
    fr = health.get("fabrication_rate", "?")
    print(f"\nSPOT CHECK: constraint_level={cl}, fab_rate={fr}")

    # Recent activity
    activity = recent_activity(30)
    print(f"\nACTIVITY (30m): {len(activity)} items")
    if not args.brief:
        for a in activity:
            print(f"  [{a['source']}:{a['activity_type']}] {a['preview']}")

    # Directives
    directives = pending_directives()
    print(f"\nDIRECTIVES: {directives}")

    # Last trace
    trace_name, trace_content = last_trace()
    if trace_name:
        print(f"\nLAST TRACE: {trace_name}")
        if not args.brief and trace_content:
            for line in trace_content.strip().split("\n")[:8]:
                print(f"  {line}")

    print("\n═══ END SITREP ═══")


if __name__ == "__main__":
    main()
