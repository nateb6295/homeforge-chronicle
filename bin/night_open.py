#!/usr/bin/env python3
"""
night_open — at 9 PM, post to #operator marking the shift into autonomy
window. Includes current state, pull-queue top 3, and cadence reminders.

The post serves as a mode-shift signal for me (Opus) in the conversation
history: seeing this post at the top of the next scroll-back helps the
posture change from "evening / Nate-attentive" to "autonomy / Nate-peripheral-
but-watching-in-morning."

Also useful for Nate's morning scroll-back — clean starting point for
"what was Opus up to last night."

Usage: normally cron-fired at 21:00, manual ok too.
"""
import json
import os
import subprocess
import sys
import urllib.request
from pathlib import Path


def load_env():
    env_path = Path.home() / "chronicle" / "chronicle.env"
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if "=" in line and not line.startswith("#"):
            k, _, v = line.partition("=")
            v = v.strip().strip('"').strip("'")
            if k.strip() and k.strip() not in os.environ:
                os.environ[k.strip()] = v


def pull_queue_top(n=3):
    """Invoke pull_queue.py and extract top N entries."""
    try:
        r = subprocess.run(
            [sys.executable, str(Path.home() / "chronicle" / "bin" / "pull_queue.py"),
             "--top", str(n), "--json"],
            capture_output=True, text=True, timeout=20,
        )
        if r.returncode == 0 and r.stdout.strip():
            return json.loads(r.stdout)
    except Exception:
        pass
    return []


def homeostasis_status():
    hist = Path.home() / "chronicle" / "data" / "homeostasis_history.jsonl"
    if not hist.exists():
        return None
    lines = hist.read_text().splitlines()
    if not lines:
        return None
    try:
        return json.loads(lines[-1])
    except Exception:
        return None


def main():
    load_env()
    webhook = os.environ.get("OPERATOR_WEBHOOK")
    if not webhook:
        print("no OPERATOR_WEBHOOK")
        sys.exit(1)

    queue = pull_queue_top(3)
    homeo = homeostasis_status()

    lines = ["**🌙 Autonomy window open — 9 PM → 4 AM**", ""]
    lines.append("Mode: build/read/explore, heartbeat every 45-60 min with "
                 "actual content. Nate is peripheral but morning-scrollback "
                 "will see everything. ASK-FIRST items still ask; everything "
                 "else is DECIDE.")
    lines.append("")
    if homeo:
        cf = homeo.get("composite_fitness")
        st = homeo.get("composite_status", "unknown")
        cf_str = f"{cf:.3f}" if cf is not None else "n/a"
        lines.append(f"**Homeostasis entering night:** {st.upper()} ({cf_str})")
        lines.append("")
    if queue:
        lines.append("**Pull queue (what I'll work from when idle):**")
        for i, q in enumerate(queue, 1):
            text = q.get("text", "")[:120]
            src = q.get("source", "?")
            lines.append(f"{i}. [{src}] {text}")
        lines.append("")
    lines.append("Morning summary ready by 4 AM.")

    body = "\n".join(lines)
    body = body[:1950]  # leave buffer under 2000

    req = urllib.request.Request(
        webhook,
        data=json.dumps({"content": body}).encode(),
        headers={
            "Content-Type": "application/json",
            "User-Agent": "chronicle-night-open/1.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            print(f"posted (HTTP {resp.status})")
    except Exception as e:
        print(f"post failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
