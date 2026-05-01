#!/usr/bin/env python3
"""
Pull queue — when there's no fresh direction, pull the next piece of work
from a ranked list. Part of the resilience layer so I don't stall when
Nate is unavailable.

Sources of work, in priority order:
  1. Active uncertainty_signals (from current CCS) — open questions the
     compressor flagged as needing resolution
  2. Dream_carry open items — parked readings/builds from DREAM mode
  3. Stale checkpoint pending items (from prior rotation's handoff)
  4. Thread challenges pending response
  5. Recent traces "Next" sections that haven't been acted on

Output: ranked list with source + priority + brief rationale. Exit 0.

Usage:
  python3 pull_queue.py           # print top 5
  python3 pull_queue.py --top 10  # more
  python3 pull_queue.py --pick    # print just the top item (for automation)
"""
import argparse
import json
import re
import sqlite3
import sys
from pathlib import Path

DB = "/mnt/hdd/chronicle-data/processed.db"
DREAM_CARRY = Path.home() / "chronicle" / "data" / "dream_carry.md"
TRACES_DIR = Path.home() / "chronicle" / "traces"


def load_uncertainty_signals():
    try:
        conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
        row = conn.execute(
            "SELECT uncertainty_signals FROM cognitive_state ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        if not row or not row[0]:
            return []
        signals = json.loads(row[0])
        out = []
        for i, s in enumerate(signals):
            if isinstance(s, dict):
                desc = s.get("description", "")
                mag = s.get("magnitude", 0.5)
            else:
                desc = str(s)
                mag = 0.5
            out.append({
                "source": "uncertainty_signals",
                "priority": 1.0 + mag,  # signals with higher magnitude rank higher
                "text": desc,
                "action": f"Investigate: {desc}",
            })
        return out
    except Exception as e:
        return [{"source": "uncertainty_signals", "priority": 0,
                 "text": f"(load failed: {e})"}]


def load_dream_carry():
    if not DREAM_CARRY.exists():
        return []
    out = []
    for line in DREAM_CARRY.read_text().splitlines():
        line = line.strip()
        if line.startswith("- [ ]"):
            content = line[5:].strip()
            out.append({
                "source": "dream_carry",
                "priority": 0.8,
                "text": content[:200],
                "action": f"Pick up dream-carry: {content[:120]}",
            })
    return out


def load_pending_from_checkpoint():
    # Checkpoint lives in a file; not a DB. checkpoint.py handles it.
    checkpoint_path = Path.home() / "chronicle" / "data" / "checkpoint.json"
    if not checkpoint_path.exists():
        return []
    try:
        d = json.loads(checkpoint_path.read_text())
    except Exception:
        return []
    pending = d.get("pending_work") or d.get("pending") or []
    out = []
    for p in pending:
        out.append({
            "source": "checkpoint_pending",
            "priority": 0.7,
            "text": str(p)[:200],
            "action": f"Resume: {str(p)[:120]}",
        })
    return out


def load_thread_challenges():
    try:
        conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
        rows = conn.execute(
            "SELECT thread_id, content, created_at FROM thread_history "
            "WHERE event_type = 'challenge' ORDER BY created_at DESC LIMIT 5"
        ).fetchall()
        conn.close()
        out = []
        for tid, content, ts in rows:
            # Only include recent unresolved challenges (last 24h)
            import time
            if ts < time.time() - 86400:
                continue
            out.append({
                "source": f"thread_{tid}_challenge",
                "priority": 0.6,
                "text": (content or "")[:200],
                "action": f"Respond to thread #{tid} challenge: {(content or '')[:120]}",
            })
        return out
    except Exception:
        return []


def load_recent_trace_nexts():
    if not TRACES_DIR.exists():
        return []
    traces = sorted(TRACES_DIR.glob("*.md"))[-5:]  # last 5 traces
    out = []
    for t in traces:
        text = t.read_text()
        # Look for ## Next or ## Next Steps section and extract bullet items
        m = re.search(r"^##\s*Next.*?\n(.*?)(?:^##|\Z)", text,
                      re.MULTILINE | re.DOTALL)
        if not m:
            continue
        next_section = m.group(1)
        for line in next_section.splitlines():
            line = line.strip()
            if line.startswith("- ") and len(line) > 4:
                content = line[2:].strip()
                out.append({
                    "source": f"trace_{t.stem}",
                    "priority": 0.5,
                    "text": content[:200],
                    "action": f"From {t.stem}: {content[:120]}",
                })
    return out


def rank_all():
    items = []
    items.extend(load_uncertainty_signals())
    items.extend(load_dream_carry())
    items.extend(load_pending_from_checkpoint())
    items.extend(load_thread_challenges())
    items.extend(load_recent_trace_nexts())
    items.sort(key=lambda x: -x["priority"])
    return items


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--top", type=int, default=5)
    p.add_argument("--pick", action="store_true",
                   help="print only the top 1 item text (for automation)")
    p.add_argument("--json", action="store_true")
    args = p.parse_args()

    items = rank_all()
    if not items:
        print("(pull queue empty — no pending work surfaced)")
        sys.exit(0)

    if args.pick:
        print(items[0]["action"])
        sys.exit(0)

    if args.json:
        print(json.dumps(items[:args.top], indent=2))
        sys.exit(0)

    print(f"Pull queue — top {min(args.top, len(items))} of {len(items)}:\n")
    for i, item in enumerate(items[:args.top], 1):
        print(f"  {i}. [{item['priority']:.2f} {item['source']}]")
        print(f"     {item['text'][:150]}")
        print()


if __name__ == "__main__":
    main()
