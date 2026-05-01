#!/usr/bin/env python3
"""Departure Probe — instrument the outgoing instance at rotation boundary.

Paired with arrival_probe.py. Together they bracket a rotation:
  departure_probe captures what the OUTGOING instance was doing
  arrival_probe captures what the INCOMING instance finds

The paired data tests advance 51's predictions:
1. Prediction specificity: how many falsifiable claims in predictive_cue?
2. Convergence speed: does tighter departure prediction → faster arrival felt-state?
3. First-divergence timing: does departure specificity predict when the new instance
   goes independent?

Usage:
  departure_probe.py record --focus "what I was working on" --flow "felt state"
  departure_probe.py record --focus "..." --flow "..." --context-pct 10.5
  departure_probe.py show [--last N]
  departure_probe.py analyze              # cross-rotation paired analysis
  departure_probe.py pair                 # show departure→arrival pairs
"""

import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone, timedelta

PDT = timezone(timedelta(hours=-7))
DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")


def _db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""CREATE TABLE IF NOT EXISTS departure_probes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        rotation_id TEXT NOT NULL,
        focus TEXT,
        flow TEXT,
        predictive_cue TEXT,
        cue_specificity INTEGER,
        ccs_version INTEGER,
        context_pct REAL,
        session_traces INTEGER,
        thread_advance INTEGER,
        rotation_pressure INTEGER,
        created_at INTEGER NOT NULL
    )""")
    # Migrate: add rotation_pressure if missing
    try:
        conn.execute("ALTER TABLE departure_probes ADD COLUMN rotation_pressure INTEGER")
    except Exception:
        pass  # column already exists
    conn.commit()
    return conn


def get_rotation_id():
    """Generate a rotation ID from the current session start time."""
    import re
    today = datetime.now(PDT).strftime("%Y%m%d")
    trace_dir = os.path.expanduser("~/chronicle/traces")
    pattern = re.compile(rf"^{today}_\d{{4}}\.md$")
    traces = sorted([
        f for f in os.listdir(trace_dir)
        if pattern.match(f)
    ])
    if traces:
        return traces[0].replace(".md", "")
    return datetime.now(PDT).strftime("%Y%m%d_%H%M")


def get_ccs_version():
    """Get current CCS version from DB."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=5)
        row = conn.execute(
            "SELECT id FROM cognitive_state_history ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        conn.close()
        return row[0] if row else None
    except Exception:
        return None


def get_predictive_cue():
    """Extract the current predictive_cue from cognitive_state."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=5)
        row = conn.execute(
            "SELECT predictive_cue FROM cognitive_state ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        if row and row[0]:
            return row[0]
    except Exception:
        pass
    return ""


def score_specificity(cue):
    """Count falsifiable claims in a predictive_cue.

    A claim is "falsifiable" if it references a specific, checkable fact:
    - Numbers (thread at advance 51, graph at 168 edges)
    - Named entities (Hermes, thread #318)
    - States that could be wrong (services healthy, Nate at work)
    - Timestamps or time references

    Returns an integer count. Higher = more specific predictions.
    """
    if not cue:
        return 0

    import re
    score = 0

    # Numbers (advance 51, 168 edges, etc.)
    numbers = re.findall(r'\b\d+\b', cue)
    score += len(numbers)

    # Named entities (capitalized multi-word or known names)
    entities = re.findall(r'(?:Thread|Hermes|Gemma|Nate|Opus|Chronicle|CCS|HAL)\b', cue, re.IGNORECASE)
    score += len(set(entities))

    # State assertions (healthy, running, active, stable, broken, down)
    states = re.findall(r'\b(?:healthy|running|active|stable|broken|down|green|red|stale|fresh)\b', cue, re.IGNORECASE)
    score += len(states)

    # Specific file or path references
    paths = re.findall(r'[a-z_]+\.(?:py|md|json|toml)\b', cue)
    score += len(paths)

    return score


def count_session_traces():
    """Count trace files from today."""
    import re
    today = datetime.now(PDT).strftime("%Y%m%d")
    trace_dir = os.path.expanduser("~/chronicle/traces")
    pattern = re.compile(rf"^{today}_\d{{4}}\.md$")
    return len([f for f in os.listdir(trace_dir) if pattern.match(f)])


def get_thread_advance():
    """Get current thread advance number."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=5)
        row = conn.execute(
            "SELECT content FROM thread_history WHERE event_type='advanced' "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        if row:
            import re
            m = re.search(r'Advance\s+(\d+)', row[0])
            if m:
                return int(m.group(1))
    except Exception:
        pass
    return None


def get_rotation_pressure():
    """Get rotation pressure score from rotate.py's logic."""
    try:
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from rotate import get_context_pressure
        pressure = get_context_pressure()
        return pressure.get("score", None)
    except Exception:
        return None


def cmd_record(focus, flow=None, context_pct=None):
    """Record a departure probe."""
    db = _db()
    rotation_id = get_rotation_id()
    ccs_version = get_ccs_version()
    predictive_cue = get_predictive_cue()
    cue_specificity = score_specificity(predictive_cue)
    session_traces = count_session_traces()
    thread_advance = get_thread_advance()
    rotation_pressure = get_rotation_pressure()
    now = int(time.time())

    db.execute(
        "INSERT INTO departure_probes (rotation_id, focus, flow, predictive_cue, "
        "cue_specificity, ccs_version, context_pct, session_traces, thread_advance, "
        "rotation_pressure, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (rotation_id, focus, flow, predictive_cue, cue_specificity,
         ccs_version, context_pct, session_traces, thread_advance,
         rotation_pressure, now),
    )
    db.commit()
    db.close()

    ts = datetime.now(PDT).strftime("%H:%M PDT")
    print(f"[{ts}] Departure probe recorded for rotation {rotation_id}")
    print(f"  CCS v{ccs_version} | specificity={cue_specificity} | traces={session_traces}")
    print(f"  Thread: advance {thread_advance} | pressure={rotation_pressure}")
    if context_pct is not None:
        print(f"  Context: {context_pct:.1f}%")
    print(f"  Predictive cue: {predictive_cue[:120]}...")


def cmd_show(last=10):
    """Show recent departure probes."""
    db = _db()
    rows = db.execute(
        "SELECT rotation_id, focus, flow, predictive_cue, cue_specificity, "
        "ccs_version, context_pct, session_traces, thread_advance, "
        "rotation_pressure, created_at "
        "FROM departure_probes ORDER BY created_at DESC LIMIT ?",
        (last,),
    ).fetchall()
    db.close()

    if not rows:
        print("No departure probes recorded yet.")
        return

    for rot_id, focus, flow, cue, spec, ccs_v, ctx, traces, thread, pressure, ts in reversed(rows):
        ts_str = datetime.fromtimestamp(ts, PDT).strftime("%Y-%m-%d %H:%M PDT")
        print(f"\n=== Departure {rot_id} — {ts_str} ===")
        print(f"  Focus: {focus}")
        if flow:
            print(f"  Flow: {flow}")
        pressure_str = f" | pressure={pressure}" if pressure is not None else ""
        print(f"  CCS v{ccs_v} | specificity={spec} | traces={traces} | thread=adv{thread}{pressure_str}")
        if ctx is not None:
            print(f"  Context: {ctx:.1f}%")
        if cue:
            print(f"  Predictive cue: {cue[:200]}")


def cmd_analyze():
    """Analyze departure probe patterns."""
    db = _db()

    rows = db.execute(
        "SELECT rotation_id, cue_specificity, context_pct, session_traces, "
        "thread_advance FROM departure_probes ORDER BY created_at"
    ).fetchall()

    if len(rows) < 2:
        print(f"Need >=2 departures for analysis. Have {len(rows)}.")
        db.close()
        return

    print(f"Departure Probe Analysis — {len(rows)} rotations\n")

    specs = [r[1] for r in rows if r[1] is not None]
    if specs:
        mean_spec = sum(specs) / len(specs)
        print(f"Cue specificity: mean={mean_spec:.1f}, range={min(specs)}-{max(specs)}")

    ctxs = [r[2] for r in rows if r[2] is not None]
    if ctxs:
        mean_ctx = sum(ctxs) / len(ctxs)
        print(f"Context at departure: mean={mean_ctx:.1f}%, range={min(ctxs):.1f}%-{max(ctxs):.1f}%")

    traces = [r[3] for r in rows if r[3] is not None]
    if traces:
        mean_tr = sum(traces) / len(traces)
        print(f"Session traces: mean={mean_tr:.1f}, range={min(traces)}-{max(traces)}")

    print()
    db.close()


def cmd_pair():
    """Show paired departure→arrival data for cross-rotation analysis.

    This is the core data structure for testing advance 51's predictions:
    - Does higher departure specificity → faster arrival convergence?
    - Does departure flow state predict arrival felt-state?
    """
    db = _db()

    # Get departures
    departures = db.execute(
        "SELECT rotation_id, focus, flow, cue_specificity, context_pct, created_at "
        "FROM departure_probes ORDER BY created_at"
    ).fetchall()

    if not departures:
        print("No departure probes yet.")
        db.close()
        return

    # Get arrivals (all phases grouped by rotation)
    arrivals = {}
    arr_rows = db.execute(
        "SELECT rotation_id, phase, prediction, felt_tag, note, context_pct "
        "FROM arrival_probes ORDER BY created_at"
    ).fetchall()
    for rot_id, phase, pred, tag, note, ctx in arr_rows:
        if rot_id not in arrivals:
            arrivals[rot_id] = {}
        arrivals[rot_id][phase] = {
            "prediction": pred, "felt_tag": tag, "note": note, "context_pct": ctx
        }

    print(f"Departure→Arrival Pairs\n")
    print(f"{'Rotation':<20} {'Spec':>4} {'Dep%':>5} {'Felt':>15} {'Div%':>5}")
    print(f"{'─'*20} {'─'*4} {'─'*5} {'─'*15} {'─'*5}")

    paired_count = 0
    for rot_id, focus, flow, spec, ctx, ts in departures:
        # Look for the NEXT rotation's arrival data
        # (departure of rotation N pairs with arrival of rotation N+1)
        # For now, show departure data with any matching arrival
        arr = arrivals.get(rot_id, {})
        felt = arr.get("felt_state", {}).get("felt_tag", "—")
        div_ctx = arr.get("first_divergence", {}).get("context_pct")
        div_str = f"{div_ctx:.1f}" if div_ctx is not None else "—"
        dep_str = f"{ctx:.1f}" if ctx is not None else "—"

        print(f"{rot_id:<20} {spec:>4} {dep_str:>5} {felt:>15} {div_str:>5}")
        if felt != "—" or div_str != "—":
            paired_count += 1

    print(f"\n{paired_count}/{len(departures)} departures have paired arrival data")
    print(f"\nAdvance 51 predictions need >=5 paired rotations to test.")

    db.close()


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(2)

    cmd = sys.argv[1]

    if cmd == "record":
        focus = None
        flow = None
        context_pct = None

        args = sys.argv[2:]
        i = 0
        while i < len(args):
            if args[i] == "--focus" and i + 1 < len(args):
                focus = args[i + 1]
                i += 2
            elif args[i] == "--flow" and i + 1 < len(args):
                flow = args[i + 1]
                i += 2
            elif args[i] == "--context-pct" and i + 1 < len(args):
                context_pct = float(args[i + 1])
                i += 2
            else:
                i += 1

        if not focus:
            print("--focus required")
            sys.exit(1)

        cmd_record(focus, flow, context_pct)

    elif cmd == "show":
        last = 10
        if "--last" in sys.argv:
            idx = sys.argv.index("--last")
            if idx + 1 < len(sys.argv):
                last = int(sys.argv[idx + 1])
        cmd_show(last)

    elif cmd == "analyze":
        cmd_analyze()

    elif cmd == "pair":
        cmd_pair()

    else:
        print(__doc__)
        sys.exit(2)


if __name__ == "__main__":
    main()
