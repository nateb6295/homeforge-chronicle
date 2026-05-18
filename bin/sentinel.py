#!/usr/bin/env python3
"""Background sentinel — runs monitoring loops outside the conversation.

Only outputs to stdout when something needs Opus's attention.
Stdout lines get injected into the conversation via Monitor.

Checks (every 90s cycle):
- Services: systemctl status
- Discord: new messages from Nate or captures
- CCS freshness: compress if stale
- Hermes-Gemma: route new messages

Only prints when there's an alert. Silence = all good.
"""
import json
import os
import subprocess
import sys
import time
import traceback

CYCLE_SECONDS = 90
CHRONICLE_ENV = os.path.expanduser("~/chronicle/chronicle.env")
BIN = os.path.expanduser("~/chronicle/bin")


def load_env():
    """Source chronicle.env into os.environ."""
    try:
        result = subprocess.run(
            ["bash", "-c", f"set -a && source {CHRONICLE_ENV} && env"],
            capture_output=True, text=True, timeout=5
        )
        for line in result.stdout.splitlines():
            if "=" in line:
                k, _, v = line.partition("=")
                os.environ[k] = v
    except Exception:
        pass


def run_script(script, args=None, timeout=60):
    """Run a Python script, return (stdout, stderr, returncode)."""
    cmd = ["python3", os.path.join(BIN, script)]
    if args:
        cmd.extend(args)
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.stdout.strip(), r.stderr.strip(), r.returncode
    except subprocess.TimeoutExpired:
        return "", "TIMEOUT", -1
    except Exception as e:
        return "", str(e), -1


def check_pulse():
    """Run pulse_check.py, return alerts or None."""
    out, err, rc = run_script("pulse_check.py", timeout=30)
    full = out + "\n" + err
    if "All quiet" in full:
        return None
    alerts = []
    if "SERVICE_DOWN" in full:
        alerts.append("SERVICE_DOWN")
    if "NATE_MESSAGE" in full or "[NATE]" in full or "[CHAT]" in full:
        alerts.append("NATE_MESSAGE")
    if "new capture" in full.lower():
        alerts.append("NEW_CAPTURES")
    if "Discord messages" in full or "DISCORD_MESSAGES" in full:
        alerts.append("DISCORD_MESSAGES")
    if "CCS stale" in full:
        alerts.append("CCS_STALE")
    if alerts:
        return f"ALERT: {', '.join(alerts)} | {out[:300]}"
    if out and "All quiet" not in out:
        return f"ALERT: {out[:300]}"
    return None


def check_ccs():
    """Run ccs_freshness.py, return alert if compressed."""
    out, err, rc = run_script("ccs_freshness.py", timeout=120)
    if "full compress" in out:
        return f"CCS_COMPRESSED: {out[:300]}"
    return None


def check_hermes():
    """Run hermes_gemma_loop.py, return alert if interesting."""
    out, err, rc = run_script("hermes_gemma_loop.py", timeout=120)
    if "No new Hermes" in out:
        return None
    if "EXTEND" in out and "0.8" in out:
        return f"HERMES_HIGH: {out[:300]}"
    if "Routing" in out:
        # Low-substance routing, don't alert
        return None
    return None


def main():
    load_env()
    sys.stdout.reconfigure(line_buffering=True)

    cycle = 0
    while True:
        try:
            cycle += 1

            # Pulse check every cycle (90s)
            alert = check_pulse()
            if alert:
                print(alert, flush=True)

            # CCS freshness every 3rd cycle (~4.5 min)
            if cycle % 3 == 0:
                alert = check_ccs()
                if alert:
                    print(alert, flush=True)

            # Hermes-Gemma every 4th cycle (~6 min)
            if cycle % 4 == 0:
                alert = check_hermes()
                if alert:
                    print(alert, flush=True)

        except Exception as e:
            print(f"SENTINEL_ERROR: {e}", flush=True)

        time.sleep(CYCLE_SECONDS)


if __name__ == "__main__":
    main()
