#!/usr/bin/env python3
"""Context pressure sensor — reads real token usage, triggers consolidation.

Primary source: context_state.json written by statusline.py (gets real
context_window.used_percentage from Claude Code). Falls back to JSONL
usage block parsing if state file is stale (>5 min).

Claude Code handles auto-compaction. No manual thresholds.

Usage:
    python3 context_pressure.py              # print pressure + update state
    python3 context_pressure.py --json       # machine-readable output
    python3 context_pressure.py --quiet      # check only, no output
    python3 context_pressure.py --should-consolidate  # exit 0 if yes, 1 if no
"""

import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

STATE_FILE = Path.home() / "chronicle" / "data" / "context_state.json"
CONSOLIDATION_LOG = Path.home() / "chronicle" / "data" / "consolidation_log.jsonl"
DB_PATH = "/mnt/hdd/chronicle-data/processed.db"

CONTEXT_WINDOW = 200_000  # Opus 4.6; was 1M for 4.7

STATE_STALENESS_SEC = 300

MIN_CONSOLIDATION_INTERVAL_SEC = 1800
MAX_NO_CONSOLIDATION_SEC = 2700  # 45 min — time-trigger regardless of pressure
MIN_BUFFER_PRIORITY = 0.5
MIN_UNCONSOLIDATED_COUNT = 3


def read_pressure() -> dict:
    """Read context pressure from the most authoritative source available."""
    state = _read_state_file()
    if state:
        return state

    try:
        from context_meter import context_state, find_active_session_jsonl
        st = context_state()
        return {
            "pct": st.get("pct", 0),
            "tokens": st.get("tokens", 0),
            "level": st.get("level", "unknown"),
            "ts": int(time.time()),
            "source": st.get("source", "fallback"),
        }
    except ImportError:
        pass

    return {
        "pct": 0,
        "tokens": 0,
        "level": "unknown",
        "ts": int(time.time()),
        "source": "none",
    }


def _read_state_file() -> dict | None:
    if not STATE_FILE.is_file():
        return None
    try:
        data = json.loads(STATE_FILE.read_text())
        age = time.time() - data.get("ts", 0)
        if age > STATE_STALENESS_SEC:
            return None
        return {
            "pct": data.get("pct", 0),
            "tokens": data.get("tokens", 0),
            "level": data.get("level", "unknown"),
            "ts": data.get("ts", 0),
            "source": "statusline",
        }
    except Exception:
        return None


def classify_level(pct: float) -> str:
    return "green"


def check_buffer_state() -> dict:
    """Check episodic_buffer for high-value unconsolidated content."""
    try:
        import sqlite3
        conn = sqlite3.connect(DB_PATH)
        high_value = conn.execute(
            "SELECT COUNT(*), AVG(priority) FROM episodic_buffer "
            "WHERE priority >= ? AND created_at > ? AND consolidated_at IS NULL",
            (MIN_BUFFER_PRIORITY, int(time.time()) - 7200)
        ).fetchone()
        total = conn.execute(
            "SELECT COUNT(*) FROM episodic_buffer WHERE consolidated_at IS NULL"
        ).fetchone()[0]
        conn.close()
        return {
            "high_value_count": high_value[0] if high_value else 0,
            "avg_priority": round(high_value[1], 3) if high_value and high_value[1] else 0,
            "total": total,
        }
    except Exception:
        return {"high_value_count": 0, "avg_priority": 0, "total": 0}


def last_consolidation_ts() -> int:
    """When did we last run proactive consolidation?"""
    if not CONSOLIDATION_LOG.is_file():
        return 0
    try:
        last_line = ""
        with open(CONSOLIDATION_LOG) as f:
            for line in f:
                if line.strip():
                    last_line = line
        if last_line:
            return json.loads(last_line).get("ts", 0)
    except Exception:
        pass
    return 0


def should_consolidate(pressure: dict) -> dict:
    """Determine whether proactive consolidation should trigger.

    Two paths to trigger (prevents do-nothing basins):
    Path A — pressure-driven: pressure >= orange (70%) + buffer has content + 30min cooldown
    Path B — time-driven: 45min since last consolidation + buffer has content (any pressure)
    """
    buffer = check_buffer_state()
    last_ts = last_consolidation_ts()
    elapsed = time.time() - last_ts
    pct = pressure.get("pct", 0)

    has_content = buffer["high_value_count"] >= MIN_UNCONSOLIDATED_COUNT
    cooldown_clear = elapsed >= MIN_CONSOLIDATION_INTERVAL_SEC
    time_overdue = elapsed >= MAX_NO_CONSOLIDATION_SEC

    if not has_content:
        return {"should": False, "reason": f"only {buffer['high_value_count']} high-value entries (need {MIN_UNCONSOLIDATED_COUNT})"}

    if not cooldown_clear:
        remaining = int(MIN_CONSOLIDATION_INTERVAL_SEC - elapsed)
        return {"should": False, "reason": f"last consolidation {int(elapsed)}s ago (wait {remaining}s)"}

    if time_overdue:
        return {
            "should": True,
            "reason": f"time-driven: {int(elapsed)}s since last + {buffer['high_value_count']} entries",
            "trigger": "time",
            "buffer": buffer,
        }

    return {"should": False, "reason": f"{int(MAX_NO_CONSOLIDATION_SEC - elapsed)}s until time trigger"}


def main():
    json_mode = "--json" in sys.argv
    quiet = "--quiet" in sys.argv
    check_consolidate = "--should-consolidate" in sys.argv

    pressure = read_pressure()

    if check_consolidate:
        result = should_consolidate(pressure)
        if json_mode:
            print(json.dumps(result, indent=2))
        elif not quiet:
            if result["should"]:
                print(f"YES — {result['reason']}")
            else:
                print(f"NO — {result['reason']}")
        sys.exit(0 if result["should"] else 1)

    if quiet:
        return

    if json_mode:
        consolidation = should_consolidate(pressure)
        pressure["consolidation"] = consolidation
        print(json.dumps(pressure, indent=2))
    else:
        pct = pressure.get("pct", 0)
        tokens = pressure.get("tokens", 0)
        source = pressure.get("source", "?")
        print(f"Context: {pct:.1%} used")
        print(f"  {pct:.1%} of {CONTEXT_WINDOW:,} tokens ({tokens:,} used)")
        print(f"  Source: {source}")

        consolidation = should_consolidate(pressure)
        if consolidation["should"]:
            print(f"  ** CONSOLIDATION RECOMMENDED: {consolidation['reason']}")
        else:
            print(f"  Consolidation: {consolidation['reason']}")


if __name__ == "__main__":
    main()
