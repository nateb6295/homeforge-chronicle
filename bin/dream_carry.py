#!/usr/bin/env python3
"""Dream carry — parked items that must survive the morning transition.

The gap this fixes: DREAM-me writes "morning register: read X" into a trace.
Morning-me (often a compacted instance) doesn't always read the trace, so
parked items get dropped. This script gives parked items their own file that
morning-me reads as part of the startup ritual.

Usage:
  dream_carry.py add "read Bennett/Suzuki autopoietic theorem against #318"
  dream_carry.py read          # list outstanding parked items
  dream_carry.py done "Bennett" # mark items matching substring as done
  dream_carry.py clear          # wipe everything (use after handoff)

Storage: ~/chronicle/dream_carry.md (plain markdown, human+machine readable).
"""

import os
import sys
from datetime import datetime, timezone, timedelta

PDT = timezone(timedelta(hours=-7))
CARRY_FILE = os.path.expanduser("~/chronicle/dream_carry.md")


def _now():
    return datetime.now(PDT).strftime("%Y-%m-%d %H:%M")


def _read_lines():
    if not os.path.exists(CARRY_FILE):
        return []
    with open(CARRY_FILE) as f:
        return [ln.rstrip("\n") for ln in f]


def _write_lines(lines):
    with open(CARRY_FILE, "w") as f:
        f.write("\n".join(lines))
        if lines and not lines[-1].endswith("\n"):
            f.write("\n")


def cmd_add(text):
    lines = _read_lines()
    if not lines:
        lines = ["# Dream carry", "", "Parked by DREAM-me for morning-me. "
                 "Read first, decide what to pick up, clear what's handled.", ""]
    lines.append(f"- [ ] {_now()} — {text}")
    _write_lines(lines)
    print(f"Carried: {text}")


def cmd_read():
    lines = _read_lines()
    if not lines:
        print("(no carry items)")
        return
    open_items = [ln for ln in lines if ln.startswith("- [ ] ")]
    done_items = [ln for ln in lines if ln.startswith("- [x] ")]
    print("\n".join(lines))
    print(f"\n{len(open_items)} open, {len(done_items)} done")


def cmd_done(substring):
    lines = _read_lines()
    marked = 0
    for i, ln in enumerate(lines):
        if ln.startswith("- [ ] ") and substring.lower() in ln.lower():
            lines[i] = ln.replace("- [ ] ", "- [x] ", 1)
            marked += 1
    _write_lines(lines)
    print(f"Marked {marked} item(s) done")


def cmd_clear():
    if os.path.exists(CARRY_FILE):
        os.remove(CARRY_FILE)
        print("Cleared.")
    else:
        print("(already empty)")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(2)
    cmd = sys.argv[1]
    if cmd == "add" and len(sys.argv) >= 3:
        cmd_add(" ".join(sys.argv[2:]))
    elif cmd == "read":
        cmd_read()
    elif cmd == "done" and len(sys.argv) >= 3:
        cmd_done(" ".join(sys.argv[2:]))
    elif cmd == "clear":
        cmd_clear()
    else:
        print(__doc__)
        sys.exit(2)


if __name__ == "__main__":
    main()
