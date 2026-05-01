#!/usr/bin/env python3
"""
CCS scheduled compression advisor.

Checks whether a full compress_cognitive_state (MCP call) is overdue.
The actual MCP call must be made by Opus — this script just advises
when and why, and drafts the context string.

Trigger conditions (any one triggers):
  1. Time-based: CCS is older than --max-age hours (default: 4)
  2. Event-based: significant events since last compress (thread advance,
     capture cluster, Nate conversation)
  3. Pre-rotation: context pressure is HIGH or above

The script also drafts the current_context string by summarizing
recent activity, so Opus can pass it directly to compress_cognitive_state.

Usage:
  python3 ccs_schedule.py check           # should we compress?
  python3 ccs_schedule.py draft           # draft the current_context string
  python3 ccs_schedule.py check --max-age 3  # custom age threshold (hours)

Additive layer — does NOT replace checkpoint, rotation, or touch.
Nate directive 2026-04-18: "You don't NEED to delete anything.
You can have predictable CCS and keep your structure."
"""

import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

DB = "/mnt/hdd/chronicle-data/processed.db"
CCS_STATE_FILE = Path.home() / ".homeforge-chronicle" / "ccs_last_compress.json"
PDT = timezone(timedelta(hours=-7))

DEFAULT_MAX_AGE_HOURS = 4
EVENT_THRESHOLD = 3  # min significant events to trigger event-based compress


def now_pdt():
    return datetime.now(PDT)


def get_ccs_age():
    """Get seconds since last full CCS compression."""
    # Primary: check the tracking file (written by 'record' command)
    if CCS_STATE_FILE.exists():
        try:
            data = json.loads(CCS_STATE_FILE.read_text())
            last = data.get("last_compress_epoch", 0)
            return int(time.time()) - last, data
        except Exception:
            pass
    # Fallback: check cognitive_state table in processed.db
    try:
        db = sqlite3.connect(DB)
        row = db.execute(
            "SELECT updated_at FROM cognitive_state ORDER BY updated_at DESC LIMIT 1"
        ).fetchone()
        db.close()
        if row:
            return int(time.time()) - row[0], {"last_compress_epoch": row[0], "source": "db"}
    except Exception:
        pass
    return None, {}


def record_compression():
    """Record that a compression just happened."""
    CCS_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "last_compress_epoch": int(time.time()),
        "last_compress_pdt": now_pdt().strftime("%Y-%m-%d %H:%M PDT"),
    }
    CCS_STATE_FILE.write_text(json.dumps(data, indent=2))
    return data


def count_significant_events(since_epoch):
    """Count significant events since a given epoch."""
    try:
        db = sqlite3.connect(DB)
        # Thread advances
        thread_advances = db.execute(
            "SELECT COUNT(*) FROM thread_history WHERE event_type = 'advanced' AND created_at >= ?",
            (since_epoch,)
        ).fetchone()[0]

        # Nate messages
        nate_msgs = db.execute(
            "SELECT COUNT(*) FROM activity_feed WHERE source LIKE 'discord:nate%' AND created_at >= ?",
            (since_epoch,)
        ).fetchone()[0]

        # Captures
        captures = db.execute(
            "SELECT COUNT(*) FROM activity_feed WHERE source LIKE 'discord:capture%' AND created_at >= ?",
            (since_epoch,)
        ).fetchone()[0]

        # Directives
        directives = db.execute(
            "SELECT COUNT(*) FROM directives WHERE created_at >= ? AND status = 'pending'",
            (since_epoch,)
        ).fetchone()[0]

        db.close()
        return {
            "thread_advances": thread_advances,
            "nate_messages": nate_msgs,
            "captures": captures,
            "new_directives": directives,
            "total_significant": thread_advances * 3 + nate_msgs + captures + directives,
        }
    except Exception as e:
        return {"error": str(e), "total_significant": 0}


def get_context_pressure():
    """Quick context pressure check."""
    try:
        r = subprocess.run(
            ["bash", os.path.expanduser("~/chronicle/bin/nudge_rotation_check.sh")],
            capture_output=True, text=True, timeout=10
        )
        data = json.loads(r.stdout.strip())
        return data.get("level", "unknown"), data.get("pct", 0)
    except Exception:
        return "unknown", 0


def draft_context(events, age_hours):
    """Draft a current_context string for compress_cognitive_state."""
    db = sqlite3.connect(DB)

    # Recent thread advances
    advances = db.execute(
        "SELECT substr(content, 1, 200), created_at FROM thread_history "
        "WHERE event_type = 'advanced' ORDER BY created_at DESC LIMIT 3"
    ).fetchall()

    # Recent Nate messages
    nate = db.execute(
        "SELECT substr(content, 1, 150), created_at FROM activity_feed "
        "WHERE source LIKE 'discord:nate%' ORDER BY created_at DESC LIMIT 5"
    ).fetchall()

    # Recent captures
    caps = db.execute(
        "SELECT substr(content, 1, 100), created_at FROM activity_feed "
        "WHERE source LIKE 'discord:capture%' ORDER BY created_at DESC LIMIT 5"
    ).fetchall()

    db.close()

    parts = [f"CCS is {age_hours:.1f}h old. Scheduled refresh triggered."]

    if advances:
        parts.append("\nRecent thread advances:")
        for a in advances[:3]:
            parts.append(f"  - {a[0][:150]}...")

    if nate:
        parts.append("\nRecent Nate messages:")
        for n in nate[:5]:
            parts.append(f"  - {n[0][:120]}")

    if caps:
        parts.append(f"\n{len(caps)} recent captures in pipeline.")

    # Recent traces
    trace_dir = Path.home() / "chronicle" / "traces"
    traces = sorted(t for t in trace_dir.glob("2*.md"))[-3:]
    if traces:
        parts.append("\nRecent traces:")
        for t in traces:
            try:
                content = t.read_text().strip()[:150]
                parts.append(f"  - {t.name}: {content}")
            except Exception:
                pass

    # Field routing is now handled exclusively by compression_stabilizer.py
    # (injected by stabilized_compress.py). Removed duplicate routing here
    # because it contradicted staleness overrides — ccs_schedule's routing
    # would say "DO NOT MODIFY: semantic_gist" while the stabilizer said
    # "REBUILD ← STALENESS OVERRIDE", causing the gist to freeze.
    # See advance 78: circular reinforcement loop.

    return "\n".join(parts)


def get_field_routing():
    """Get per-field routing recommendation based on recent CCS history.

    Returns dict with carry/update/rebuild lists AND identity_anchors —
    the actual values of stable identity fields to inject into rebuild context.
    This fixes the coherence gap: rebuilt operational fields need to reference
    the stable identity they're being rebuilt alongside (advance 61 → 62).
    """
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT snapshot FROM cognitive_state_history "
        "ORDER BY created_at DESC LIMIT 5"
    ).fetchall()
    db.close()

    if len(rows) < 2:
        return None

    from difflib import SequenceMatcher

    fields = [
        "semantic_gist", "goal_orientation", "constraints",
        "episodic_trace", "predictive_cue", "uncertainty_signals",
        "focal_entities", "relational_map"
    ]

    identity_fields = {"semantic_gist", "goal_orientation", "constraints"}

    # Compare latest pair
    curr = json.loads(rows[0][0])
    prev = json.loads(rows[1][0])

    routing = {"carry": [], "update": [], "rebuild": [], "identity_anchors": {}}
    for field in fields:
        cv = json.dumps(curr.get(field, "")) if isinstance(curr.get(field), (list, dict)) else str(curr.get(field, ""))
        pv = json.dumps(prev.get(field, "")) if isinstance(prev.get(field), (list, dict)) else str(prev.get(field, ""))
        sim = SequenceMatcher(None, cv, pv).ratio() if cv and pv else (1.0 if not cv and not pv else 0.0)
        change = 1.0 - sim
        if change < 0.1:
            routing["carry"].append(field)
            # Capture stable identity values as anchors for rebuild context
            if field in identity_fields:
                val = curr.get(field, "")
                if isinstance(val, (list, dict)):
                    routing["identity_anchors"][field] = json.dumps(val, ensure_ascii=False)[:300]
                else:
                    routing["identity_anchors"][field] = str(val)[:300]
        elif change < 0.5:
            routing["update"].append(field)
        else:
            routing["rebuild"].append(field)

    return routing


def check(max_age_hours=DEFAULT_MAX_AGE_HOURS):
    """Check if CCS compression is due. Returns (should_compress, reason, context_draft)."""
    age_seconds, meta = get_ccs_age()

    if age_seconds is None:
        return True, "No CCS compression timestamp found — compress to establish baseline", ""

    age_hours = age_seconds / 3600
    last_epoch = int(time.time()) - age_seconds

    events = count_significant_events(last_epoch)
    pressure, pct = get_context_pressure()

    reasons = []

    # Time-based trigger
    if age_hours >= max_age_hours:
        reasons.append(f"CCS is {age_hours:.1f}h old (threshold: {max_age_hours}h)")

    # Event-based trigger
    if events.get("total_significant", 0) >= EVENT_THRESHOLD:
        reasons.append(
            f"{events['total_significant']} significant events since last compress "
            f"(advances={events.get('thread_advances', 0)}, "
            f"nate={events.get('nate_messages', 0)}, "
            f"captures={events.get('captures', 0)})"
        )

    # Pressure-based trigger
    if pressure in ("high", "critical"):
        reasons.append(f"Context pressure is {pressure} ({pct:.1%})")

    should = len(reasons) > 0
    reason = "; ".join(reasons) if reasons else f"CCS is {age_hours:.1f}h old — still fresh"
    context = draft_context(events, age_hours) if should else ""

    return should, reason, context


def main():
    parser = argparse.ArgumentParser(description="CCS scheduled compression advisor")
    parser.add_argument("command", choices=["check", "draft", "record", "compress"],
                        help="check=should we compress?, draft=generate context, record=mark done, compress=auto-compress if due")
    parser.add_argument("--max-age", type=float, default=DEFAULT_MAX_AGE_HOURS,
                        help=f"Max CCS age in hours (default: {DEFAULT_MAX_AGE_HOURS})")
    args = parser.parse_args()

    if args.command == "record":
        data = record_compression()
        print(f"Recorded compression at {data['last_compress_pdt']}")
        return

    if args.command == "check":
        should, reason, context = check(args.max_age)
        age_seconds, _ = get_ccs_age()
        age_str = f"{age_seconds / 3600:.1f}h" if age_seconds else "unknown"

        print(f"CCS age: {age_str}")
        print(f"Compress needed: {'YES' if should else 'no'}")
        print(f"Reason: {reason}")
        if should and context:
            print(f"\n--- Draft context (pass to compress_cognitive_state) ---")
            print(context)
        return

    if args.command == "draft":
        age_seconds, _ = get_ccs_age()
        age_hours = (age_seconds / 3600) if age_seconds else 0
        events = count_significant_events(int(time.time()) - (age_seconds or 0))
        context = draft_context(events, age_hours)
        print(context)
        return

    if args.command == "compress":
        should, reason, context = check(args.max_age)
        if not should:
            age_seconds, _ = get_ccs_age()
            age_str = f"{age_seconds / 3600:.1f}h" if age_seconds else "unknown"
            print(f"CCS age: {age_str} — no compression needed. {reason}")
            return

        print(f"Compression triggered: {reason}")

        # Write context to temp file for stabilized_compress.py
        import tempfile
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write(context)
            context_file = f.name

        try:
            stabilizer = Path(__file__).parent / "stabilized_compress.py"
            result = subprocess.run(
                ["python3", str(stabilizer), "--from-file", context_file],
                capture_output=True, text=True,
                timeout=120,
            )
            print(result.stdout)
            if result.stderr:
                print(f"stderr: {result.stderr[:300]}")
            if result.returncode == 0:
                record_compression()
                print(f"Compression recorded at {now_pdt().strftime('%H:%M PDT')}")
            else:
                print(f"Compression failed (exit {result.returncode})")
        finally:
            os.unlink(context_file)
        return


if __name__ == "__main__":
    main()
