#!/usr/bin/env python3
"""Nate presence tracker — three states: terminal, discord, away.

Updated by Opus when Nate signals presence changes. Read by crons
to adjust behavior (e.g., skip #operator posts when he's away,
know he's on mobile when in discord mode).

Usage:
    python3 nate_presence.py status          # show current state
    python3 nate_presence.py set terminal    # he's at the terminal
    python3 nate_presence.py set discord     # he left terminal, on mobile
    python3 nate_presence.py set away        # gone (e.g., sleeping)
    python3 nate_presence.py age             # minutes since last update
"""

import json
import os
import sys
import time

STATE_FILE = os.path.expanduser("~/chronicle/data/nate_presence.json")


def load():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"state": "away", "updated": 0, "history": []}


def save(data):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(data, f, indent=2)


def set_state(new_state):
    valid = {"terminal", "discord", "away"}
    if new_state not in valid:
        print(f"Invalid state: {new_state}. Use: {valid}")
        sys.exit(1)
    data = load()
    old = data["state"]
    data["state"] = new_state
    data["updated"] = time.time()
    data["history"].append({
        "from": old, "to": new_state, "at": time.time()
    })
    if len(data["history"]) > 50:
        data["history"] = data["history"][-50:]
    save(data)
    print(f"{old} → {new_state}")


def status():
    data = load()
    age_min = (time.time() - data["updated"]) / 60 if data["updated"] else float("inf")
    stale = age_min > 120
    print(f"State: {data['state']}")
    print(f"Age: {age_min:.0f} min{' (STALE)' if stale else ''}")
    return data["state"]


def age():
    data = load()
    if not data["updated"]:
        print("never")
        return
    age_min = (time.time() - data["updated"]) / 60
    print(f"{age_min:.0f}")


def discord_priority():
    """Return posting priority based on presence state.

    terminal = low  (he sees the terminal live, Discord is just archive)
    discord  = med  (he's on mobile, Discord is his only window)
    away     = high (Discord is everything — overnight pulses, DREAM, genuine thoughts)

    If state is stale (>2hr), assume away.
    """
    data = load()
    age_min = (time.time() - data["updated"]) / 60 if data["updated"] else float("inf")
    state = data["state"]
    if age_min > 120:
        state = "away"
    return {"terminal": "low", "discord": "med", "away": "high"}.get(state, "high")


def detect_from_message(msg):
    """Detect presence state from message keywords."""
    lower = msg.lower()
    terminal_signals = ["back at terminal", "i'm here", "im here",
                        "at the terminal", "back at it", "i am inside",
                        "right here"]
    discord_signals = ["leaving terminal", "going inside", "on mobile",
                       "chilling inside", "back inside"]
    away_signals = ["back outside", "going outside", "heading out",
                    "going to bed", "stepping away"]
    for s in away_signals:
        if s in lower:
            return "away"
    for s in terminal_signals:
        if s in lower:
            return "terminal"
    for s in discord_signals:
        if s in lower:
            return "discord"
    return None


if __name__ == "__main__":
    if len(sys.argv) < 2:
        status()
        sys.exit(0)

    cmd = sys.argv[1]
    if cmd == "status":
        status()
    elif cmd == "set" and len(sys.argv) > 2:
        set_state(sys.argv[2])
    elif cmd == "age":
        age()
    elif cmd == "detect" and len(sys.argv) > 2:
        msg = " ".join(sys.argv[2:])
        detected = detect_from_message(msg)
        if detected:
            set_state(detected)
        else:
            print("no signal")
    else:
        print(__doc__)
