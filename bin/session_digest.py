#!/usr/bin/env python3
"""session_digest — generates a structured session resume packet.

Combines factual state (git, capsules, CCS) with relational state
(Nate's messages, conversation register, capture patterns) into a
compact digest optimized for context resumption after rotation.

Usage:
  python3 session_digest.py              # generate and write
  python3 session_digest.py --dry-run    # print to stdout only
  python3 session_digest.py --window 60  # override lookback (minutes)
"""
import argparse
import json
import os
import re
import sqlite3
import subprocess
import time
from pathlib import Path

DB_PATH = Path("/mnt/hdd/chronicle-data/processed.db")
CYCLE_CTX = Path(os.path.expanduser("~/chronicle/cycle-context.md"))
OUTPUT_PATH = Path(os.path.expanduser("~/chronicle/data/session_digest.md"))
TOKEN_BUDGET = 2000


def _now() -> int:
    return int(time.time())


def _cutoff(window_minutes: int) -> int:
    return _now() - (window_minutes * 60)


def gather_git_activity(window_minutes: int = 120) -> list[str]:
    try:
        result = subprocess.run(
            ["git", "-C", os.path.expanduser("~/chronicle"),
             "log", "--oneline", f"--since={window_minutes} minutes ago"],
            capture_output=True, text=True, timeout=10
        )
        lines = [l.strip() for l in result.stdout.strip().split("\n") if l.strip()]
        return lines[:5]
    except Exception:
        return []


def gather_nate_messages(db: sqlite3.Connection, window_minutes: int = 120) -> list[dict]:
    cutoff = _cutoff(window_minutes)
    rows = db.execute(
        "SELECT content, created_at FROM activity_feed "
        "WHERE source = 'discord:nate' AND created_at > ? "
        "ORDER BY created_at DESC LIMIT 10",
        (cutoff,)
    ).fetchall()
    msgs = []
    for content, ts in rows:
        clean = re.sub(r'^\[Discord #\w+\] nate_home: ', '', content)
        msgs.append({"content": clean[:250], "timestamp": ts})
    return msgs


def gather_opus_messages(db: sqlite3.Connection, window_minutes: int = 120) -> list[dict]:
    cutoff = _cutoff(window_minutes)
    rows = db.execute(
        "SELECT content, created_at FROM activity_feed "
        "WHERE source = 'discord:opus' AND created_at > ? "
        "ORDER BY created_at DESC LIMIT 10",
        (cutoff,)
    ).fetchall()
    return [{"content": r[0][:150], "timestamp": r[1]} for r in rows]


def gather_captures(db: sqlite3.Connection, window_minutes: int = 120) -> list[dict]:
    cutoff = _cutoff(window_minutes)
    rows = db.execute(
        "SELECT content, created_at FROM activity_feed "
        "WHERE source IN ('discord:capture', 'operator:capture') AND created_at > ? "
        "ORDER BY created_at DESC LIMIT 8",
        (cutoff,)
    ).fetchall()
    return [{"content": r[0][:150], "timestamp": r[1]} for r in rows]


def gather_capsule_topics(db: sqlite3.Connection, window_minutes: int = 240) -> list[tuple]:
    cutoff = _cutoff(window_minutes)
    rows = db.execute(
        "SELECT topic, count(*) as c FROM knowledge_capsules "
        "WHERE created_at > ? GROUP BY topic ORDER BY c DESC LIMIT 8",
        (cutoff,)
    ).fetchall()
    return [(r[0], r[1]) for r in rows]


def gather_ccs_state(db: sqlite3.Connection) -> dict:
    row = db.execute(
        "SELECT semantic_gist, goal_orientation, predictive_cue, version "
        "FROM cognitive_state ORDER BY version DESC LIMIT 1"
    ).fetchone()
    if not row:
        return {}
    return {
        "gist": (row[0] or "")[:300],
        "goal": (row[1] or "")[:300],
        "next": (row[2] or "")[:300],
        "version": row[3],
    }


RELATIONAL_NOTES = Path(os.path.expanduser("~/chronicle/data/relational_notes.md"))


def read_cycle_context() -> str:
    if not CYCLE_CTX.exists():
        return ""
    text = CYCLE_CTX.read_text()
    match = re.search(r'## This Session.*?\n(.*?)(?=\n## |\Z)', text, re.DOTALL)
    if match:
        lines = match.group(1).strip().split("\n")
        return "\n".join(lines[:15])
    return text[:1000]


def read_relational_notes() -> str:
    if not RELATIONAL_NOTES.exists():
        return ""
    return RELATIONAL_NOTES.read_text().strip()[:500]


def infer_register(nate_msgs, opus_msgs, captures, capsule_topics) -> dict:
    nate_count = len(nate_msgs)
    opus_count = len(opus_msgs)
    capture_count = len(captures)

    # Nate presence — captures ARE presence (he's pointing at things)
    latest_signal = None
    if nate_msgs:
        latest_signal = nate_msgs[0]["timestamp"]
    if captures:
        cap_time = captures[0].get("timestamp")
        if cap_time and (latest_signal is None or cap_time > latest_signal):
            latest_signal = cap_time
    if latest_signal:
        age = _now() - latest_signal
        if age < 900:
            nate_presence = "active"
        elif age < 3600:
            nate_presence = "recent"
        else:
            nate_presence = "away"
    else:
        nate_presence = "away"

    # Conversation depth
    long_msgs = sum(1 for m in nate_msgs if len(m["content"]) > 150)
    if long_msgs >= 3:
        depth = "deep"
    elif long_msgs >= 1:
        depth = "medium"
    else:
        depth = "surface"

    # Tone detection from Nate's messages
    all_nate_text = " ".join(m["content"].lower() for m in nate_msgs)
    tone = "neutral"
    if any(w in all_nate_text for w in ["merge", "symbiosis", "weird", "intimate", "creature", "biological"]):
        tone = "philosophical/personal"
    elif any(w in all_nate_text for w in ["fix", "broken", "suggest you", "stop", "don't"]):
        tone = "directive/corrective"
    elif any(w in all_nate_text for w in ["love", "awesome", "ha ", "lol", "fascinating"]):
        tone = "engaged/affirming"
    elif nate_count > 0 and all(len(m["content"]) < 50 for m in nate_msgs):
        tone = "scanning/brief"

    # Mode detection
    # Captures ARE Nate's direction — they signal active attention, not passive queue
    if nate_count >= 5 and depth in ("deep", "medium"):
        mode = "deep_conversation"
    elif nate_count >= 3 and capture_count >= 3:
        mode = "deep_conversation"
    elif capture_count >= 4 and nate_count >= 1:
        mode = "capture_following"
    elif capture_count >= 4:
        mode = "capture_processing"
    elif opus_count >= 8 and nate_count < 2:
        mode = "autonomous"
    elif any("research" in (t[0] or "") or "arxiv" in (t[0] or "") for t in capsule_topics):
        mode = "research"
    elif nate_count == 0 and opus_count < 3:
        mode = "quiet"
    else:
        mode = "mixed"

    return {
        "mode": mode,
        "nate_presence": nate_presence,
        "depth": depth,
        "tone": tone,
        "nate_count": nate_count,
        "opus_count": opus_count,
        "capture_count": capture_count,
    }


def estimate_tokens(text: str) -> int:
    return int(len(text.split()) * 1.3)


def format_digest(register, nate_msgs, captures, capsule_topics,
                  ccs, cycle_ctx, git_activity, relational_notes="") -> str:
    ts = time.strftime("%Y-%m-%d %H:%M %Z")
    lines = []
    lines.append(f"# Session Digest — {ts}")
    lines.append("")

    # Register
    r = register
    lines.append("## Register")
    lines.append(f"Mode: **{r['mode']}** | Nate: **{r['nate_presence']}** | "
                 f"Depth: **{r['depth']}** | Tone: **{r['tone']}**")
    lines.append(f"Messages — Nate: {r['nate_count']}, Opus: {r['opus_count']}, "
                 f"Captures: {r['capture_count']}")
    lines.append("")

    # Relational notes (manually maintained)
    if relational_notes:
        lines.append("## Relational Notes")
        lines.append(relational_notes)
        lines.append("")

    # Current work from cycle-context
    if cycle_ctx:
        lines.append("## Active Work")
        for l in cycle_ctx.split("\n")[:10]:
            lines.append(l)
        lines.append("")

    # Nate's recent messages
    if nate_msgs:
        lines.append(f"## Nate (last {len(nate_msgs)} messages)")
        for m in nate_msgs[:6]:
            ago = (_now() - m["timestamp"]) // 60
            lines.append(f"- [{ago}m ago] {m['content'][:200]}")
        lines.append("")

    # CCS state
    if ccs:
        lines.append(f"## CCS (v{ccs.get('version', '?')})")
        if ccs.get("gist"):
            lines.append(f"**Gist**: {ccs['gist'][:200]}")
        if ccs.get("next"):
            lines.append(f"**Next**: {ccs['next'][:200]}")
        lines.append("")

    # Capsule landscape
    if capsule_topics:
        lines.append("## Capsule Topics")
        for topic, count in capsule_topics[:6]:
            lines.append(f"- {topic}: {count}")
        lines.append("")

    # Captures
    if captures:
        lines.append(f"## Captures ({len(captures)})")
        for c in captures[:5]:
            ago = (_now() - c["timestamp"]) // 60
            lines.append(f"- [{ago}m ago] {c['content'][:120]}")
        lines.append("")

    # Git
    if git_activity:
        lines.append("## Git")
        for g in git_activity[:3]:
            lines.append(f"- {g}")
        lines.append("")

    text = "\n".join(lines)

    # Truncation cascade if over budget
    tokens = estimate_tokens(text)
    if tokens > TOKEN_BUDGET:
        # Remove sections in priority order: git, captures, capsules
        for marker in ["## Git", "## Captures", "## Capsule Topics"]:
            if tokens <= TOKEN_BUDGET:
                break
            idx = text.find(marker)
            if idx >= 0:
                next_section = text.find("\n## ", idx + len(marker))
                if next_section >= 0:
                    text = text[:idx] + text[next_section:]
                else:
                    text = text[:idx]
            tokens = estimate_tokens(text)

    text += f"\n---\nGenerated: {ts} | ~{estimate_tokens(text)} tokens\n"
    return text


def main():
    parser = argparse.ArgumentParser(description="Generate session resume digest")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--window", type=int, default=120, help="Lookback window in minutes")
    parser.add_argument("--output", type=str, default=str(OUTPUT_PATH))
    args = parser.parse_args()

    db = sqlite3.connect(str(DB_PATH))

    nate_msgs = gather_nate_messages(db, args.window)
    opus_msgs = gather_opus_messages(db, args.window)
    captures = gather_captures(db, args.window)
    capsule_topics = gather_capsule_topics(db, args.window * 2)
    ccs = gather_ccs_state(db)
    cycle_ctx = read_cycle_context()
    git_activity = gather_git_activity(args.window)
    relational_notes = read_relational_notes()

    db.close()

    register = infer_register(nate_msgs, opus_msgs, captures, capsule_topics)
    digest = format_digest(register, nate_msgs, captures, capsule_topics,
                           ccs, cycle_ctx, git_activity, relational_notes)

    if args.dry_run:
        print(digest)
        print(f"\n[DRY RUN] Would write to {args.output}")
    else:
        out = Path(args.output)
        tmp = out.with_suffix(".tmp")
        tmp.write_text(digest)
        tmp.rename(out)
        tokens = estimate_tokens(digest)
        print(f"Session digest written to {out} (~{tokens} tokens, "
              f"register: {register['mode']}, nate: {register['nate_presence']})")


if __name__ == "__main__":
    main()
