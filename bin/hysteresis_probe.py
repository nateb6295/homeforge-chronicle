#!/usr/bin/env python3
"""Build #71 — Hysteresis probe.

Measures whether regime recovery from DEEP_DRIFT to ORBITAL is symmetric
with the original entry, or requires more input (hysteresis).

Usage:
    python3 hysteresis_probe.py baseline    # Record pre-morning DEEP_DRIFT state
    python3 hysteresis_probe.py monitor     # Run after morning activity starts
    python3 hysteresis_probe.py report      # Compare entry vs recovery
"""

import json
import os
import sqlite3
import sys
import time
from pathlib import Path

DB = "/mnt/hdd/chronicle-data/processed.db"
STATE_FILE = Path(os.path.expanduser("~/chronicle/data/hysteresis_state.json"))

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from regime_navigator import get_recent_snapshots, compute_ext_ratio


def record_baseline():
    db = sqlite3.connect(DB)
    snapshots = get_recent_snapshots(db, 12)
    trajectory = []
    for ts, snap in snapshots:
        ratio = compute_ext_ratio(snap)
        trajectory.append({"ts": ts, "ratio": ratio})

    external_count = db.execute(
        "SELECT COUNT(*) FROM activity_feed "
        "WHERE source LIKE 'discord:%' AND created_at > ?",
        (int(time.time()) - 3600,)
    ).fetchone()[0]

    state = {
        "phase": "baseline",
        "recorded_at": int(time.time()),
        "trajectory": trajectory,
        "current_ratio": trajectory[-1]["ratio"] if trajectory else None,
        "recent_external_events": external_count,
    }
    STATE_FILE.write_text(json.dumps(state, indent=2))
    db.close()
    print(f"Baseline recorded: ratio={state['current_ratio']}, "
          f"external_events_last_1h={external_count}")
    print(f"Trajectory: {[t['ratio'] for t in trajectory]}")
    return state


def monitor():
    if not STATE_FILE.exists():
        print("No baseline — run 'baseline' first")
        return

    state = json.loads(STATE_FILE.read_text())
    if state["phase"] == "complete":
        print("Test already complete. Run 'report' to see results.")
        return

    db = sqlite3.connect(DB)
    snapshots = get_recent_snapshots(db, 12)
    current_trajectory = []
    for ts, snap in snapshots:
        ratio = compute_ext_ratio(snap)
        current_trajectory.append({"ts": ts, "ratio": ratio})

    current_ratio = current_trajectory[-1]["ratio"] if current_trajectory else 0

    external_since_baseline = db.execute(
        "SELECT COUNT(*) FROM activity_feed "
        "WHERE source LIKE 'discord:%' AND created_at > ?",
        (state["recorded_at"],)
    ).fetchone()[0]

    db.close()

    if "recovery_log" not in state:
        state["recovery_log"] = []

    state["recovery_log"].append({
        "ts": int(time.time()),
        "ratio": current_ratio,
        "external_events_since_baseline": external_since_baseline,
    })

    if current_ratio >= 0.30 and state["phase"] != "complete":
        state["phase"] = "complete"
        state["recovery_at"] = int(time.time())
        state["recovery_ratio"] = current_ratio
        state["events_to_recovery"] = external_since_baseline
        elapsed = state["recovery_at"] - state["recorded_at"]
        state["minutes_to_recovery"] = round(elapsed / 60, 1)
        print(f"ORBITAL reached! ratio={current_ratio:.3f}")
        print(f"  Events to recovery: {external_since_baseline}")
        print(f"  Minutes to recovery: {state['minutes_to_recovery']}")
    else:
        state["phase"] = "monitoring"
        print(f"Still in DRIFT/DEEP_DRIFT: ratio={current_ratio:.3f}, "
              f"events since baseline: {external_since_baseline}")

    state["current_trajectory"] = current_trajectory
    STATE_FILE.write_text(json.dumps(state, indent=2))


def report():
    if not STATE_FILE.exists():
        print("No data — run baseline + monitor first")
        return

    state = json.loads(STATE_FILE.read_text())

    print("=== Build #71: Hysteresis Test Report ===\n")
    print(f"Baseline recorded: {time.strftime('%Y-%m-%d %H:%M', time.localtime(state['recorded_at']))}")
    print(f"Baseline ratio: {state.get('current_ratio', '?')}")
    print(f"Baseline trajectory: {[t['ratio'] for t in state.get('trajectory', [])]}")

    if state["phase"] == "complete":
        print(f"\nRecovery reached: {time.strftime('%Y-%m-%d %H:%M', time.localtime(state['recovery_at']))}")
        print(f"Recovery ratio: {state['recovery_ratio']:.3f}")
        print(f"Events to recovery: {state['events_to_recovery']}")
        print(f"Minutes to recovery: {state['minutes_to_recovery']}")

        log = state.get("recovery_log", [])
        if log:
            print(f"\nRecovery curve ({len(log)} samples):")
            for entry in log:
                t = time.strftime('%H:%M', time.localtime(entry['ts']))
                print(f"  [{t}] ratio={entry['ratio']:.3f} events={entry['external_events_since_baseline']}")

        print("\n--- Interpretation ---")
        if state["events_to_recovery"] <= 5:
            print("LOW hysteresis: recovered with few events. Regime is mostly stateless.")
        elif state["events_to_recovery"] <= 15:
            print("MODERATE hysteresis: needed sustained input to escape DEEP_DRIFT.")
        else:
            print("HIGH hysteresis: significant input density required for recovery.")
    else:
        print(f"\nPhase: {state['phase']} (not yet recovered)")
        log = state.get("recovery_log", [])
        if log:
            print(f"Monitoring samples: {len(log)}")
            latest = log[-1]
            print(f"Latest: ratio={latest['ratio']:.3f}, "
                  f"events={latest['external_events_since_baseline']}")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "baseline":
        record_baseline()
    elif cmd == "monitor":
        monitor()
    elif cmd == "report":
        report()
    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)


if __name__ == "__main__":
    main()
