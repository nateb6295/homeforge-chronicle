#!/usr/bin/env python3
"""Uncertainty Signal Hygiene — checks CCS uncertainty signals for staleness.

CCS uncertainty signals are the system's "epistemic tokens" — they maintain
alternative hypotheses and unresolved questions. But they freeze: empirically,
the same signals persist 8+ compressions unchanged while entities cycle 1.9x.

This tool:
1. Checks how long each uncertainty signal has persisted unchanged
2. Flags signals that appear resolved by recent work (entity/trace overlap)
3. Suggests replacement signals based on current unresolved state

Usage:
  python3 uncertainty_hygiene.py              # Check current staleness
  python3 uncertainty_hygiene.py --history N  # Check against N snapshots
"""

import json
import sqlite3
import sys
from pathlib import Path

DB = Path("/mnt/hdd/chronicle-data/processed.db")


def get_current_signals() -> list[dict]:
    """Get current CCS uncertainty signals."""
    db = sqlite3.connect(str(DB))
    row = db.execute("SELECT uncertainty_signals FROM cognitive_state WHERE id = 1").fetchone()
    db.close()
    if not row:
        return []
    try:
        signals = json.loads(row[0]) if isinstance(row[0], str) else row[0]
        return signals if isinstance(signals, list) else []
    except (json.JSONDecodeError, TypeError):
        return []


def get_signal_history(n: int = 20) -> list[list[dict]]:
    """Get uncertainty signals from last N snapshots."""
    db = sqlite3.connect(str(DB))
    rows = db.execute(
        "SELECT snapshot FROM cognitive_state_history ORDER BY created_at DESC LIMIT ?", (n,)
    ).fetchall()
    db.close()

    result = []
    for row in rows:
        try:
            snap = json.loads(row[0])
            signals = snap.get("uncertainty_signals", [])
            if isinstance(signals, str):
                signals = json.loads(signals)
            result.append(signals if isinstance(signals, list) else [])
        except (json.JSONDecodeError, TypeError):
            result.append([])

    result.reverse()
    return result


def signal_fingerprint(signal: dict) -> str:
    """Create a fingerprint for matching signals across snapshots."""
    desc = signal.get("description", "").lower().strip()
    # Use first 60 chars as fingerprint — signals rarely change partially
    return desc[:60]


def compute_staleness(history: list[list[dict]]) -> dict:
    """Compute how many consecutive snapshots each current signal has persisted.

    Returns: {signal_description: consecutive_count}
    """
    if not history:
        return {}

    current = history[-1]
    current_fps = {signal_fingerprint(s): s for s in current}

    staleness = {}
    for fp, signal in current_fps.items():
        # Count backwards from most recent
        count = 0
        for i in range(len(history) - 1, -1, -1):
            snap_fps = {signal_fingerprint(s) for s in history[i]}
            if fp in snap_fps:
                count += 1
            else:
                break
        staleness[signal.get("description", "?")] = count

    return staleness


def check_resolution_overlap(signals: list[dict]) -> list[dict]:
    """Check if any signals mention concepts that are now resolved entities."""
    db = sqlite3.connect(str(DB))

    # Get current entities
    row = db.execute("SELECT focal_entities FROM cognitive_state WHERE id = 1").fetchone()
    entities = set()
    if row:
        try:
            ents = json.loads(row[0]) if isinstance(row[0], str) else row[0]
            entities = {e.get("name", "").lower().strip() for e in ents if e.get("name")}
        except (json.JSONDecodeError, TypeError):
            pass

    # Get recent episodic trace
    row = db.execute("SELECT episodic_trace FROM cognitive_state WHERE id = 1").fetchone()
    trace_text = ""
    if row:
        try:
            trace = json.loads(row[0]) if isinstance(row[0], str) else row[0]
            if isinstance(trace, list):
                trace_text = " ".join(str(t) for t in trace).lower()
            else:
                trace_text = str(trace).lower()
        except (json.JSONDecodeError, TypeError):
            pass

    db.close()

    resolved = []
    for signal in signals:
        desc = signal.get("description", "").lower()
        resolution = signal.get("resolution_path", "").lower()

        # Check if the signal's subject appears in current entities or trace
        overlap_entities = [e for e in entities if e in desc or e in resolution]
        overlap_trace = any(word in trace_text for word in desc.split() if len(word) > 5)

        if overlap_entities or overlap_trace:
            resolved.append({
                "signal": signal.get("description", "?"),
                "magnitude": signal.get("magnitude", 0),
                "entity_overlap": overlap_entities,
                "trace_overlap": overlap_trace,
            })

    return resolved


def main():
    import argparse
    parser = argparse.ArgumentParser(description="CCS Uncertainty Signal Hygiene")
    parser.add_argument("--history", type=int, default=20,
                        help="Number of snapshots for staleness check")
    args = parser.parse_args()

    current = get_current_signals()
    if not current:
        print("No current uncertainty signals.")
        return

    print(f"=== Uncertainty Signal Hygiene ({len(current)} signals) ===\n")

    # Current signals
    print("CURRENT SIGNALS:")
    for s in current:
        mag = s.get("magnitude", 0)
        desc = s.get("description", "?")
        resolution = s.get("resolution_path", "?")
        print(f"  [{mag:.2f}] {desc}")
        print(f"         → {resolution}")
    print()

    # Staleness
    history = get_signal_history(args.history)
    staleness = compute_staleness(history)

    print(f"STALENESS (against {len(history)} snapshots):")
    for desc, count in sorted(staleness.items(), key=lambda x: -x[1]):
        status = "STALE" if count >= 5 else "OK" if count <= 2 else "AGING"
        pct = count / len(history) * 100 if history else 0
        print(f"  [{status:>5}] {count:>3} snapshots ({pct:.0f}%): {desc[:80]}")

    stale_count = sum(1 for c in staleness.values() if c >= 5)
    if stale_count:
        print(f"\n  ⚠ {stale_count}/{len(current)} signals are STALE (5+ compressions unchanged)")
        print(f"  The CCS is losing epistemic flexibility — uncertainty signals should evolve.")
    else:
        print(f"\n  ✓ All signals are fresh (< 5 compressions)")
    print()

    # Resolution overlap
    resolved = check_resolution_overlap(current)
    if resolved:
        print("POTENTIALLY RESOLVED:")
        for r in resolved:
            print(f"  {r['signal'][:80]}")
            if r["entity_overlap"]:
                print(f"    → Entity overlap: {r['entity_overlap']}")
            if r["trace_overlap"]:
                print(f"    → Trace keyword overlap")
        print()
        print(f"  {len(resolved)}/{len(current)} signals may already be answered.")
        print(f"  Consider resolving and replacing at next compression.")
    else:
        print("RESOLUTION CHECK: No signals appear resolved by current state.")


if __name__ == "__main__":
    main()
