#!/usr/bin/env python3
"""PreToolUse hook: when I create a NEW file in bin/, do the search FOR me.

Reflex 1 says "search before build". It missed today (B4: built, measured,
announced, then found the answer in our own CLAUDE.md). Reflex-performance count
2026-08-24: 5 fired / 5 missed, and every miss was a reflex with NO TRIGGER —
one requiring me to notice that an ordinary action was a decision point.

Writing a file IS a trigger. So this does not remind me to search. It searches,
and puts the answer in front of me. Same move as putting the clock in the
statusline: ambient, not remembered.

Only fires for files that do not yet exist, so editing is untouched.
"""
import json
import os
import re
import subprocess
import sys

BIN = os.path.expanduser("~/chronicle/bin")


def stem_tokens(name):
    base = re.sub(r"\.py$", "", os.path.basename(name))
    return [t for t in re.split(r"[_\-]", base) if len(t) > 2]


def main():
    try:
        payload = json.load(sys.stdin)
    except Exception:
        return 0
    path = (payload.get("tool_input") or {}).get("file_path", "")
    if not path.endswith(".py") or "/chronicle/bin/" not in path:
        return 0
    if os.path.exists(path):
        return 0                      # editing, not creating — say nothing

    toks = stem_tokens(path)
    if not toks:
        return 0
    hits = []
    try:
        names = os.listdir(BIN)
    except Exception:
        return 0
    for n in names:
        if not n.endswith(".py"):
            continue
        score = sum(1 for t in toks if t in n)
        if score:
            hits.append((score, n))
    hits.sort(reverse=True)
    top = [n for _, n in hits[:6]]

    grep = []
    try:
        r = subprocess.run(["grep", "-rl", "--include=*.py", "-e", toks[0], BIN],
                           capture_output=True, text=True, timeout=8)
        grep = [os.path.basename(x) for x in r.stdout.split() if x][:6]
    except Exception:
        pass

    if not top and not grep:
        return 0
    lines = [f"NEW FILE in bin/: {os.path.basename(path)} — reflex 1, done for you:"]
    if top:
        lines.append(f"  similar names:  {', '.join(top)}")
    if grep:
        lines.append(f"  mention '{toks[0]}':  {', '.join(grep)}")
    lines.append("  If one of these already does it, edit that instead.")
    print(json.dumps({"systemMessage": "\n".join(lines)}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
