#!/usr/bin/env python3
"""
momentum_check.py — Detect behavioral coasting.

Gap #2 from ecosystem inventory: CCS preserves state across rotations
but not behavioral momentum. This tool checks recent traces and activity
for signs of coasting (many cycles with no builds, no thread advances,
no meaningful output).

Usage:
  python3 momentum_check.py          # Check current momentum
  python3 momentum_check.py --alert  # Post to Discord if coasting detected
"""
import argparse
import json
import os
import sqlite3
import time
import urllib.request
from pathlib import Path

DB = "/mnt/hdd/chronicle-data/processed.db"
TRACES_DIR = Path.home() / "chronicle" / "traces"


def check_recent_builds(hours=3):
    """Check for code modifications or tool creation in recent hours."""
    cutoff = int(time.time()) - (hours * 3600)
    db = sqlite3.connect(DB)

    # Check code_modifications table
    mods = db.execute(
        "SELECT COUNT(*) FROM code_modifications WHERE timestamp > ?",
        (cutoff,)
    ).fetchone()[0]

    # Check activity_feed for opus-sourced builds
    builds = db.execute(
        "SELECT COUNT(*) FROM activity_feed "
        "WHERE source LIKE 'opus:%' AND created_at > ?",
        (cutoff,)
    ).fetchone()[0]

    db.close()
    return mods + builds


def check_thread_advances(hours=3):
    """Check for thread advances in recent hours."""
    cutoff = int(time.time()) - (hours * 3600)
    db = sqlite3.connect(DB)
    advances = db.execute(
        "SELECT COUNT(*) FROM thread_history "
        "WHERE event_type = 'advanced' AND created_at > ?",
        (cutoff,)
    ).fetchone()[0]
    db.close()
    return advances


def check_traces(hours=3):
    """Analyze recent traces for substantive content."""
    cutoff_time = time.time() - (hours * 3600)
    traces = sorted(TRACES_DIR.glob("*.md"), reverse=True)

    substantive = 0
    monitoring_only = 0

    for t in traces[:20]:
        # Parse timestamp from filename (YYYYMMDD_HHMM.md)
        try:
            name = t.stem
            # Rough check — just look at recent ones
            content = t.read_text()

            # Heuristic: substantive traces mention builds, ships, deploys
            build_words = ["built", "shipped", "deployed", "created", "fixed",
                          "advanced", "wrote", "ran", "probe", "results"]
            coast_words = ["stable", "no issues", "polling", "quiet",
                          "presence", "heartbeat", "monitoring"]

            build_hits = sum(1 for w in build_words if w in content.lower())
            coast_hits = sum(1 for w in coast_words if w in content.lower())

            if build_hits > coast_hits:
                substantive += 1
            else:
                monitoring_only += 1
        except Exception:
            continue

    return substantive, monitoring_only


def check_discord_posts(hours=3):
    """Check for substantive Discord posts (not just heartbeats)."""
    cutoff = int(time.time()) - (hours * 3600)
    db = sqlite3.connect(DB)
    posts = db.execute(
        "SELECT COUNT(*) FROM activity_feed "
        "WHERE source LIKE 'discord:opus%' AND created_at > ?",
        (cutoff,)
    ).fetchone()[0]
    db.close()
    return posts


def momentum_score(builds, advances, substantive_traces, monitoring_traces, posts):
    """Compute a 0-1 momentum score."""
    # Weighted components
    build_score = min(builds / 3.0, 1.0) * 0.35      # 3+ builds = full marks
    thread_score = min(advances / 2.0, 1.0) * 0.25    # 2+ advances = full marks
    trace_ratio = substantive_traces / max(substantive_traces + monitoring_traces, 1)
    trace_score = trace_ratio * 0.20
    post_score = min(posts / 5.0, 1.0) * 0.20         # 5+ posts = full marks

    return round(build_score + thread_score + trace_score + post_score, 2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--hours", type=int, default=3, help="Lookback window")
    parser.add_argument("--alert", action="store_true", help="Post to Discord if coasting")
    args = parser.parse_args()

    builds = check_recent_builds(args.hours)
    advances = check_thread_advances(args.hours)
    sub_traces, mon_traces = check_traces(args.hours)
    posts = check_discord_posts(args.hours)

    score = momentum_score(builds, advances, sub_traces, mon_traces, posts)

    print(f"MOMENTUM CHECK ({args.hours}h lookback)")
    print(f"  Builds/mods:      {builds}")
    print(f"  Thread advances:  {advances}")
    print(f"  Traces: {sub_traces} substantive, {mon_traces} monitoring-only")
    print(f"  Discord posts:    {posts}")
    print(f"  SCORE: {score:.2f}")
    print()

    if score >= 0.6:
        print("STATUS: BUILDING — momentum is strong")
    elif score >= 0.3:
        print("STATUS: MIXED — some building, some coasting")
    elif score >= 0.1:
        print("STATUS: COASTING — mostly monitoring, little output")
    else:
        print("STATUS: IDLE — no meaningful activity detected")

    if args.alert and score < 0.2:
        webhook = os.environ.get("OPERATOR_WEBHOOK", "")
        if webhook:
            msg = (f"⚠️ **Momentum alert**: score {score:.2f} over last {args.hours}h. "
                   f"Builds: {builds}, advances: {advances}, "
                   f"traces: {sub_traces} substantive / {mon_traces} monitoring. "
                   f"Coasting detected — shift to building mode.")
            body = json.dumps({"content": msg}).encode()
            req = urllib.request.Request(
                webhook, data=body,
                headers={"Content-Type": "application/json"}
            )
            try:
                urllib.request.urlopen(req, timeout=10)
                print(f"\nAlert posted to Discord.")
            except Exception as e:
                print(f"\nAlert failed: {e}")


if __name__ == "__main__":
    main()
