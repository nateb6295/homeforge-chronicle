#!/usr/bin/env python3
"""Did Nate say something I haven't answered in #operator?

WHY THIS EXISTS: on 2026-08-24 I twice wrote a full, careful answer to a
substantial question from Nate and left it in the TERMINAL. Never posted. Both
times caught by an unrelated routine check, not by remembering. That is reflex 5
failing in its most dangerous mode — the trigger fires, the action is taken, and
the output lands somewhere that does not count. Composing a good answer FEELS
like answering; I would have sworn I replied.

I tried to build this gate and told Nate the record did not exist — discord_chat_log
empty, opus_chat.json stale. He said: "Did we capture the terminal?"

The Claude Code SESSION TRANSCRIPT has every user message with an ISO timestamp,
42MB of it, and it had been named in my own context since the session began. I
said the archive could not answer without grepping the raw transcript, which is
reflex 10 verbatim, violated five minutes after invoking it.

Reads only the tail — the file is large and grows all session.
"""
import glob
import json
import os
import sqlite3
import sys
import datetime

DB = "/mnt/hdd/chronicle-data/processed.db"
PROJ = os.path.expanduser("~/.claude/projects/-home-nate-agx-chronicle")
CRON = ("Rhythm pulse.", "Exploration window.", "Capture constellation", "DREAM window")


def _text(rec):
    m = rec.get("message", {}) or {}
    c = m.get("content")
    if isinstance(c, str):
        return c
    if isinstance(c, list):
        return " ".join(b.get("text", "") for b in c
                        if isinstance(b, dict) and b.get("type") == "text")
    return ""


def last_nate_message(tail_bytes=400_000):
    files = sorted(glob.glob(os.path.join(PROJ, "*.jsonl")),
                   key=os.path.getmtime, reverse=True)
    if not files:
        return None
    with open(files[0], "rb") as f:
        f.seek(0, 2)
        f.seek(max(0, f.tell() - tail_bytes))
        lines = f.read().decode("utf-8", "replace").split("\n")[1:]
    best = None
    for ln in lines:
        if not ln.strip():
            continue
        try:
            r = json.loads(ln)
        except Exception:
            continue
        if r.get("type") != "user":
            continue
        t = _text(r).strip()
        if not t or t.startswith(CRON) or t.startswith("<") or "tool_result" in t[:40]:
            continue
        ts = r.get("timestamp")
        if ts:
            best = (ts, t)
    return best


def last_operator_post():
    try:
        db = sqlite3.connect(DB, timeout=10)
        row = db.execute("SELECT MAX(created_at) FROM activity_feed "
                         "WHERE source='opus:operator'").fetchone()
        db.close()
        return row[0]
    except Exception:
        return None


def main():
    nate = last_nate_message()
    if not nate:
        return 0
    ts, txt = nate
    spoke = datetime.datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
    posted = last_operator_post() or 0
    gap = spoke - posted
    if gap <= 0:
        return 0                      # answered after he spoke
    mins = gap / 60
    when = datetime.datetime.fromtimestamp(spoke).strftime("%H:%M")
    print(f"REPLY OWED: Nate spoke at {when}, {mins:.0f} min AFTER your last "
          f"#operator post. Nothing has gone to him since.", file=sys.stderr)
    print(f'  "{" ".join(txt.split())[:150]}"', file=sys.stderr)
    print("  Terminal text is not a reply. Post it.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
