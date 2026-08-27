#!/usr/bin/env python3
"""CCS Dose Tracker — maps position on the inverted-U therapeutic window.

F160 showed CCS dose-response follows an inverted U: peak at D2-D3,
declining at higher doses. But we don't track WHERE we are on that curve
in our own system. This tool counts CCS compression turns since context
rotation and estimates our position on the therapeutic window.

The inverted U means more CCS isn't always better. There's an optimal
dose range. Tracking this lets us modulate compression frequency and
decide when to compress vs when to let the current geometric state ride.

Usage:
  python3 dose_tracker.py                # show current dose state
  python3 dose_tracker.py --json         # structured output
  python3 dose_tracker.py --block        # compression injection block
  python3 dose_tracker.py --reset        # mark rotation boundary
"""

import argparse
import json
import os
import sqlite3
import sys
import time
from pathlib import Path

DB_PATH = "/mnt/hdd/chronicle-data/processed.db"
STATE_FILE = os.path.expanduser("~/chronicle/data/dose_state.json")
HISTORY_FILE = os.path.expanduser("~/chronicle/data/dose_history.jsonl")
COMPRESS_LOG = os.path.expanduser("~/chronicle/data/stabilized_compression.jsonl")

THERAPEUTIC_WINDOW = {
    "peak_low": 2,
    "peak_high": 3,
    "overdose_threshold": 5,
    "description": "Based on F160: 7B models peak at D2-D3, decline after D4"
}


def get_compression_history():
    """Read compression events from the stabilized compression log."""
    events = []
    if not os.path.exists(COMPRESS_LOG):
        return events
    try:
        with open(COMPRESS_LOG) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    events.append(entry)
                except json.JSONDecodeError:
                    continue
    except Exception:
        pass
    return events


def get_ccs_version():
    """Get current CCS version from DB."""
    try:
        db = sqlite3.connect(DB_PATH)
        row = db.execute(
            "SELECT updated_at FROM cognitive_state ORDER BY rowid DESC LIMIT 1"
        ).fetchone()
        db.close()
        return row[0] if row else None
    except Exception:
        return None


def load_state():
    """Load persisted dose state."""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {
        "rotation_ts": time.time(),
        "dose_count": 0,
        "compression_timestamps": [],
        "last_updated": time.time(),
    }


def save_state(state):
    """Save dose state."""
    state["last_updated"] = time.time()
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, STATE_FILE)


def count_compressions_since_rotation(state):
    """Count CCS compression events since last rotation."""
    events = get_compression_history()
    rotation_ts = state.get("rotation_ts", 0)
    count = 0
    timestamps = []
    for e in events:
        ts = e.get("timestamp", e.get("ts", 0))
        if isinstance(ts, str):
            try:
                from datetime import datetime
                dt = datetime.fromisoformat(ts)
                ts = dt.timestamp()
            except (ValueError, AttributeError):
                continue
        if ts > rotation_ts:
            count += 1
            timestamps.append(ts)
    return count, timestamps


def compute_dose_state(state=None):
    """Compute current position on the therapeutic window."""
    if state is None:
        state = load_state()

    dose_count, timestamps = count_compressions_since_rotation(state)
    state["dose_count"] = dose_count
    state["compression_timestamps"] = timestamps[-20:]

    now = time.time()
    rotation_age_hours = (now - state.get("rotation_ts", now)) / 3600

    if timestamps:
        intervals = []
        for i in range(1, len(timestamps)):
            intervals.append((timestamps[i] - timestamps[i-1]) / 60.0)
        avg_interval = sum(intervals) / len(intervals) if intervals else 0
        last_compress_ago = (now - timestamps[-1]) / 60.0
    else:
        avg_interval = 0
        last_compress_ago = rotation_age_hours * 60

    window = THERAPEUTIC_WINDOW
    if dose_count <= window["peak_low"]:
        position = "pre-peak"
        recommendation = "compress — building toward therapeutic window"
    elif dose_count <= window["peak_high"]:
        position = "peak"
        recommendation = "in therapeutic window — geometric state is optimal"
    elif dose_count <= window["overdose_threshold"]:
        position = "post-peak"
        recommendation = "declining returns — compress only if significant state change"
    else:
        position = "overdose"
        recommendation = "past therapeutic window — serial dependence may be inverting"

    return {
        "dose_count": dose_count,
        "position": position,
        "recommendation": recommendation,
        "rotation_age_hours": round(rotation_age_hours, 1),
        "avg_interval_min": round(avg_interval, 1),
        "last_compress_ago_min": round(last_compress_ago, 1),
        "therapeutic_window": window,
        "measured_at": time.time(),
        "measured_at_human": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    }


def format_compression_block(dose_state):
    """Format dose state for injection into CCS compression."""
    block = "\n## CCS Dose State\n\n"
    block += f"Dose: D{dose_state['dose_count']} | Position: {dose_state['position']}\n"
    block += f"Rotation age: {dose_state['rotation_age_hours']}h | "
    block += f"Avg interval: {dose_state['avg_interval_min']}min\n"
    block += f"Recommendation: {dose_state['recommendation']}\n"

    if dose_state["position"] == "overdose":
        block += (
            "\nWARNING: Past therapeutic window. F160 showed CCS advantage "
            "reverses at high doses on some architectures. Consider whether "
            "this compression adds genuine state or just accumulates noise.\n"
        )
    elif dose_state["position"] == "peak":
        block += (
            "\nIn the therapeutic window. This compression has maximum "
            "geometric leverage — make it count. Focus on the most "
            "significant state changes, not incremental updates.\n"
        )

    return block


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--block", action="store_true")
    parser.add_argument("--reset", action="store_true", help="Mark rotation boundary")
    args = parser.parse_args()

    if args.reset:
        state = {
            "rotation_ts": time.time(),
            "dose_count": 0,
            "compression_timestamps": [],
            "last_updated": time.time(),
        }
        save_state(state)
        with open(HISTORY_FILE, "a") as f:
            f.write(json.dumps({"event": "rotation", "ts": time.time()}) + "\n")
        print(f"Rotation boundary marked at {time.strftime('%H:%M:%S')}")
        return

    state = load_state()
    dose_state = compute_dose_state(state)
    save_state(state)

    if args.json:
        print(json.dumps(dose_state, indent=2))
        return

    if args.block:
        print(format_compression_block(dose_state))
        return

    print(f"CCS Dose State")
    print(f"  Dose count:     D{dose_state['dose_count']}")
    print(f"  Position:       {dose_state['position']}")
    print(f"  Rotation age:   {dose_state['rotation_age_hours']}h")
    print(f"  Avg interval:   {dose_state['avg_interval_min']}min")
    print(f"  Last compress:  {dose_state['last_compress_ago_min']}min ago")
    print(f"  Recommendation: {dose_state['recommendation']}")


if __name__ == "__main__":
    main()
