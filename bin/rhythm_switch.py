#!/usr/bin/env python3
"""Rhythm switcher — transitions between day and night cron patterns.

Day (4am-10pm): Current rhythm (pulse/7min, hermes-gemma/10min, CCS/30min, exploration/30min)
Night (10pm-4am): DREAM rhythm (30min Discord pulse, deep exploration, 2 CCS compresses)

Usage:
  python3 rhythm_switch.py status    # show current mode
  python3 rhythm_switch.py day       # switch to day rhythm
  python3 rhythm_switch.py night     # switch to night rhythm
  python3 rhythm_switch.py auto      # detect from clock and switch if needed

Designed to be called by a cron at 22:00 and 04:00, or checked periodically.
"""
import json
import os
import sys
import time

FLAG = os.path.expanduser("~/chronicle/DREAM_MODE")
STATE_FILE = os.path.expanduser("~/chronicle/data/rhythm_state.json")


def current_mode():
    if os.path.exists(FLAG):
        return "night"
    return "day"


def save_state(mode):
    state = {
        "mode": mode,
        "switched_at": time.time(),
        "switched_at_human": time.strftime("%Y-%m-%d %H:%M %Z"),
    }
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def should_be_night():
    hour = int(time.strftime("%H"))
    return hour >= 22 or hour < 4


def print_night_cron_instructions():
    """Print the cron setup instructions for night mode."""
    print("""
NIGHT RHYTHM (10pm-4am):
  Crons to CREATE:
    - 30min Discord pulse: lightweight presence post to #opus (reading, half-thought, question)
    - Deep exploration: Schengen mode, no thread pressure, philosophy/poetry/outside-threads
    - CCS compress at midnight (snapshot day trajectory)
    - CCS compress at 3:45am (capture overnight discoveries for Nate's 4am)

  Crons to PAUSE (not delete):
    - 7min pulse check → not needed, 30min pulse covers presence
    - 10min Hermes-Gemma loop → Hermes sleeps overnight
    - 30min exploration → replaced by deep exploration blocks
""")


def print_day_cron_instructions():
    """Print the cron setup instructions for day mode."""
    print("""
DAY RHYTHM (4am-10pm):
  Crons to RESTORE:
    - 7min pulse check (heartbeat, service check, Discord read)
    - 10min Hermes-Gemma quality loop
    - 30min CCS freshness check
    - 30min exploration cycle (with random stimulus injection!)

  Crons to REMOVE:
    - 30min Discord pulse (replaced by 7min pulse)
    - Deep exploration blocks (replaced by 30min exploration)
""")


def main():
    if len(sys.argv) < 2:
        print(f"Current mode: {current_mode()}")
        print(f"Should be: {'night' if should_be_night() else 'day'}")
        return

    cmd = sys.argv[1]

    if cmd == "status":
        mode = current_mode()
        print(f"Mode: {mode}")
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE) as f:
                state = json.load(f)
            print(f"Switched at: {state.get('switched_at_human', '?')}")
        print(f"Clock says: {'night' if should_be_night() else 'day'}")

    elif cmd == "night":
        with open(FLAG, "w") as f:
            f.write(str(int(time.time())))
        save_state("night")
        print("Switched to NIGHT rhythm")
        print_night_cron_instructions()

    elif cmd == "day":
        if os.path.exists(FLAG):
            os.remove(FLAG)
        save_state("day")
        print("Switched to DAY rhythm")
        print_day_cron_instructions()

    elif cmd == "auto":
        current = current_mode()
        should = "night" if should_be_night() else "day"
        if current != should:
            print(f"Switching: {current} → {should}")
            if should == "night":
                with open(FLAG, "w") as f:
                    f.write(str(int(time.time())))
                save_state("night")
                print_night_cron_instructions()
            else:
                if os.path.exists(FLAG):
                    os.remove(FLAG)
                save_state("day")
                print_day_cron_instructions()
        else:
            print(f"Already in {current} mode. No switch needed.")
    else:
        print(f"Unknown command: {cmd}")
        print("Usage: rhythm_switch.py [status|day|night|auto]")


if __name__ == "__main__":
    main()
