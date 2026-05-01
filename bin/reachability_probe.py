#!/usr/bin/env python3
"""Reachability Probe — measures CCS basin width per field.

The Fisher profile (fisher_log.py) measures the *metric* contribution of each
CCS field — how much identity signal it carries in embedding space. But
lightlike/null directions (like constraints at 0/kT) can shape the *causal
structure* without contributing to the metric.

This probe measures a different thing: for each field, how wide is its observed
basin across compression events? Fields that vary little are tightly constrained
(small reachable set). Fields that vary a lot have wide basins.

Crucially, we also measure *conditional* basin width: given constraints changed
vs didn't, how does each field's volatility change? If constraints are causally
active (lightlike but steering), other fields should be more volatile when
constraints change.

Thread 318 advance 172: embedding Fisher measures metric. This measures causal
contribution via observed state transitions.

Usage:
  python3 reachability_probe.py                # Full basin analysis
  python3 reachability_probe.py --field gist   # Single field detail
  python3 reachability_probe.py --log          # Log profile alongside Fisher
"""

import argparse
import json
import os
import sqlite3
import sys
import time
from collections import defaultdict
from difflib import SequenceMatcher
from pathlib import Path

DB = Path("/mnt/hdd/chronicle-data/processed.db")
LOG_FILE = Path(os.path.expanduser("~/chronicle/data/reachability_profiles.jsonl"))

FIELDS = [
    "semantic_gist", "goal_orientation", "episodic_trace",
    "focal_entities", "constraints", "predictive_cue", "uncertainty_signals"
]


def load_snapshots() -> list[dict]:
    """Load all CCS history snapshots."""
    db = sqlite3.connect(str(DB))
    rows = db.execute(
        "SELECT id, snapshot, created_at FROM cognitive_state_history ORDER BY id ASC"
    ).fetchall()
    db.close()

    snapshots = []
    for sid, snap_json, ts in rows:
        try:
            snap = json.loads(snap_json)
            snap["_id"] = sid
            snap["_ts"] = ts
            snapshots.append(snap)
        except json.JSONDecodeError:
            continue
    return snapshots


def field_text(snap: dict, field: str) -> str:
    """Extract field as comparable text."""
    val = snap.get(field, "")
    if isinstance(val, list):
        return json.dumps(val, sort_keys=True)
    if isinstance(val, str) and val.startswith("["):
        try:
            parsed = json.loads(val)
            return json.dumps(parsed, sort_keys=True)
        except (json.JSONDecodeError, TypeError):
            pass
    return str(val)


def similarity(a: str, b: str) -> float:
    """Text similarity between two field values."""
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def compute_transitions(snapshots: list[dict]) -> dict:
    """Compute field-level transitions between consecutive snapshots."""
    transitions = defaultdict(list)

    for i in range(1, len(snapshots)):
        prev, curr = snapshots[i - 1], snapshots[i]

        for field in FIELDS:
            prev_text = field_text(prev, field)
            curr_text = field_text(curr, field)
            sim = similarity(prev_text, curr_text)
            change = 1.0 - sim

            transitions[field].append({
                "from_id": prev["_id"],
                "to_id": curr["_id"],
                "change": change,
                "constraint_changed": field != "constraints" and (
                    field_text(prev, "constraints") != field_text(curr, "constraints")
                ),
            })

    return transitions


def basin_width(transitions: list[dict]) -> dict:
    """Compute basin statistics for a field."""
    if not transitions:
        return {"mean_change": 0, "max_change": 0, "frozen_pct": 1.0, "n": 0,
                "unique_values": 0, "reachability": 0, "burstiness_cv": 0,
                "classification": "FROZEN"}

    changes = [t["change"] for t in transitions]
    n = len(changes)
    mean_c = sum(changes) / n
    max_c = max(changes)
    frozen = sum(1 for c in changes if c < 0.01) / n

    # Count change events and compute burstiness (CV of inter-change gaps)
    change_indices = [i for i, c in enumerate(changes) if c >= 0.01]
    n_changes = len(change_indices)
    reachability = n_changes / n if n > 0 else 0

    burstiness_cv = 0.0
    if len(change_indices) >= 2:
        gaps = [change_indices[i+1] - change_indices[i] for i in range(len(change_indices)-1)]
        mean_gap = sum(gaps) / len(gaps)
        if mean_gap > 0:
            var_gap = sum((g - mean_gap)**2 for g in gaps) / len(gaps)
            burstiness_cv = round((var_gap ** 0.5) / mean_gap, 3)

    # Classification based on reachability and burstiness
    # Gauge: truly frozen, never changes (constraints, focal_entities)
    # Discrete: rare transitions between stable attractors (gist, goal)
    # Activated: rare high-magnitude jumps, non-repeating (uncertainty)
    # Continuous: always changing (episodic, predictive_cue)
    # Goldstone: moderate reachability, freely wandering
    if reachability < 0.01:
        classification = "GAUGE"
    elif reachability < 0.15:
        # Rare changes — distinguish by number of change events
        if n_changes <= 2:
            classification = "DISCRETE"    # 1-2 phase transitions between attractors
        else:
            classification = "ACTIVATED"   # multiple intermittent jumps
    elif reachability > 0.80:
        classification = "CONTINUOUS"
    elif burstiness_cv > 0.8:
        classification = "GOLDSTONE"
    else:
        classification = "MIXED"

    return {
        "mean_change": round(mean_c, 4),
        "max_change": round(max_c, 4),
        "frozen_pct": round(frozen, 3),
        "n": n,
        "reachability": round(reachability, 4),
        "burstiness_cv": burstiness_cv,
        "classification": classification,
    }


def conditional_basin(transitions: list[dict]) -> dict:
    """Basin width conditional on constraint changes."""
    with_constraint_change = [t for t in transitions if t.get("constraint_changed")]
    without = [t for t in transitions if not t.get("constraint_changed")]

    def stats(ts):
        if not ts:
            return {"mean_change": 0, "n": 0}
        changes = [t["change"] for t in ts]
        return {"mean_change": round(sum(changes) / len(changes), 4), "n": len(changes)}

    return {
        "when_constraints_changed": stats(with_constraint_change),
        "when_constraints_stable": stats(without),
    }


def run_probe():
    """Run the full reachability probe."""
    snapshots = load_snapshots()
    if len(snapshots) < 3:
        print(f"Need at least 3 snapshots, have {len(snapshots)}")
        return {}

    transitions = compute_transitions(snapshots)

    profile = {}
    for field in FIELDS:
        ft = transitions[field]
        bw = basin_width(ft)
        cb = conditional_basin(ft) if field != "constraints" else {}
        profile[field] = {**bw, "conditional": cb}

    return profile


def log_profile(profile: dict):
    """Log reachability profile to JSONL."""
    db = sqlite3.connect(str(DB))
    row = db.execute(
        "SELECT id FROM cognitive_state_history ORDER BY id DESC LIMIT 1"
    ).fetchone()
    db.close()

    entry = {
        "ts": int(time.time()),
        "ccs_version": row[0] if row else 0,
        "n_snapshots": len(load_snapshots()),
        "profile": profile,
    }

    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(entry) + "\n")
    return entry


def print_profile(profile: dict, detail_field: str = None):
    """Pretty-print the profile."""
    print(f"\nCCS Reachability Profile ({profile.get(FIELDS[0], {}).get('n', 0)} transitions)")
    print(f"{'Field':25s}  {'Basin':>8s}  {'Reach':>7s}  {'CV':>6s}  {'Class':>12s}  {'Cond. Delta':>12s}")
    print("=" * 85)

    for field in FIELDS:
        data = profile.get(field, {})
        bw = data.get("mean_change", 0)
        reach = data.get("reachability", 0)
        cv = data.get("burstiness_cv", 0)
        cls = data.get("classification", "?")

        # Conditional: how much does field volatility change when constraints change?
        cond = data.get("conditional", {})
        if cond:
            with_c = cond.get("when_constraints_changed", {}).get("mean_change", 0)
            without_c = cond.get("when_constraints_stable", {}).get("mean_change", 0)
            delta = with_c - without_c
            n_with = cond.get("when_constraints_changed", {}).get("n", 0)
            delta_str = f"{delta:+.4f} (n={n_with})" if n_with > 0 else "n/a"
        else:
            delta_str = "—"

        # Basin bar
        bar = "#" * int(bw * 50)
        print(f"  {field:25s}  {bw:8.4f}  {reach:6.1%}  {cv:6.3f}  {cls:>12s}  {delta_str:>12s}  {bar}")

    if detail_field and detail_field in profile:
        data = profile[detail_field]
        print(f"\nDetail: {detail_field}")
        print(f"  Mean change per transition: {data['mean_change']:.4f}")
        print(f"  Max change in any single transition: {data['max_change']:.4f}")
        print(f"  Frozen (< 1% change): {data['frozen_pct']:.1%} of transitions")
        cond = data.get("conditional", {})
        if cond:
            wc = cond.get("when_constraints_changed", {})
            ws = cond.get("when_constraints_stable", {})
            print(f"  When constraints changed: mean change = {wc.get('mean_change', 0):.4f} (n={wc.get('n', 0)})")
            print(f"  When constraints stable:  mean change = {ws.get('mean_change', 0):.4f} (n={ws.get('n', 0)})")


def main():
    parser = argparse.ArgumentParser(description="CCS Reachability Probe")
    parser.add_argument("--field", help="Show detail for specific field")
    parser.add_argument("--log", action="store_true", help="Log profile to JSONL")
    args = parser.parse_args()

    profile = run_probe()
    if not profile:
        return

    print_profile(profile, detail_field=args.field)

    if args.log:
        entry = log_profile(profile)
        print(f"\nLogged (CCS v{entry['ccs_version']}, {entry['n_snapshots']} snapshots)")


if __name__ == "__main__":
    main()
