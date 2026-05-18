#!/usr/bin/env python3
"""Legibility diff — what changed since the last snapshot.

Compares current cognitive/thread state against a stored baseline
and reports what shifted. Like git diff but for cognitive state.
"""
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

DB = "/mnt/hdd/chronicle-data/processed.db"
STATE_FILE = Path.home() / "chronicle" / "data" / "legibility_baseline.json"


def get_current_state(db):
    now = int(datetime.now(timezone.utc).timestamp())
    state = {"timestamp": now, "threads": {}, "captures": 0, "thread_entries": {}}

    cur = db.execute(
        "SELECT id, title, updated_at FROM cognitive_threads WHERE status='active'"
    )
    for tid, title, updated in cur.fetchall():
        state["threads"][str(tid)] = {"title": title, "updated_at": updated}

    for tid in state["threads"]:
        cur = db.execute(
            "SELECT COUNT(*) FROM thread_history WHERE thread_id = ?", (int(tid),)
        )
        state["thread_entries"][tid] = cur.fetchone()[0]

    cur = db.execute(
        "SELECT COUNT(*) FROM activity_feed WHERE source LIKE '%capture%'"
    )
    state["captures"] = cur.fetchone()[0]

    return state


def load_baseline():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return None


def save_baseline(state):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))


def compute_diff(old, new):
    diff = {"elapsed_h": 0, "thread_changes": [], "new_threads": [], "capture_delta": 0}

    if old:
        diff["elapsed_h"] = round((new["timestamp"] - old["timestamp"]) / 3600, 1)
        diff["capture_delta"] = new["captures"] - old.get("captures", 0)

        for tid, info in new["threads"].items():
            old_entries = old.get("thread_entries", {}).get(tid, 0)
            new_entries = new["thread_entries"].get(tid, 0)
            delta = new_entries - old_entries
            if delta > 0:
                diff["thread_changes"].append(
                    f"#{tid} {info['title']}: +{delta} entries"
                )

        old_tids = set(old.get("threads", {}).keys())
        new_tids = set(new["threads"].keys())
        for tid in new_tids - old_tids:
            diff["new_threads"].append(
                f"#{tid} {new['threads'][tid]['title']}"
            )

    return diff


def format_diff(diff):
    if not diff.get("elapsed_h"):
        return "First snapshot — no baseline to diff against. Baseline saved."

    lines = [f"**Legibility Diff** — {diff['elapsed_h']}h since last snapshot"]

    if diff["capture_delta"]:
        lines.append(f"Captures: +{diff['capture_delta']}")

    if diff["new_threads"]:
        lines.append("New threads:")
        for t in diff["new_threads"]:
            lines.append(f"  {t}")

    if diff["thread_changes"]:
        lines.append("Thread activity:")
        for c in diff["thread_changes"]:
            lines.append(f"  {c}")

    if not diff["thread_changes"] and not diff["new_threads"] and not diff["capture_delta"]:
        lines.append("(nothing changed)")

    return "\n".join(lines)


def main():
    db = sqlite3.connect(DB)
    current = get_current_state(db)
    baseline = load_baseline()
    diff = compute_diff(baseline, current)

    print(format_diff(diff))

    if "--save" in sys.argv or baseline is None:
        save_baseline(current)
        if baseline is not None:
            print("\nBaseline updated.")

    db.close()


if __name__ == "__main__":
    main()
