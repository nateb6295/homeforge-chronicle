#!/usr/bin/env python3
"""CCS Field Router — measures per-field change across CCS snapshots
and recommends routing (which fields need more compression effort).

Inspired by MoR (Mixture-of-Recursions) per-token routing and Ouro's
entropy-regularized gating. Applied to CCS: instead of uniform compression,
route effort to fields that changed most.

Thread #318 advance 59 prediction: field-level routing should outperform
uniform compression on downstream navigation scores.
"""

import json
import sqlite3
import sys
from difflib import SequenceMatcher
from pathlib import Path

DB = Path("/mnt/hdd/chronicle-data/processed.db")

IDENTITY_FIELDS = {"semantic_gist", "goal_orientation", "constraints"}
OPERATIONAL_FIELDS = {"episodic_trace", "predictive_cue", "uncertainty_signals"}
STRUCTURAL_FIELDS = {"focal_entities", "relational_map"}

ALL_FIELDS = IDENTITY_FIELDS | OPERATIONAL_FIELDS | STRUCTURAL_FIELDS


def similarity(a: str, b: str) -> float:
    """String similarity 0-1."""
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def field_value(snap: dict, field: str) -> str:
    """Extract field as string."""
    val = snap.get(field, "")
    if isinstance(val, (list, dict)):
        return json.dumps(val, sort_keys=True)
    return str(val)


def analyze(n: int = 10):
    """Analyze field-level change across last n CCS snapshots."""
    db = sqlite3.connect(str(DB))
    rows = db.execute(
        "SELECT snapshot, datetime(created_at, 'unixepoch', 'localtime') "
        "FROM cognitive_state_history ORDER BY created_at DESC LIMIT ?",
        (n,)
    ).fetchall()
    db.close()

    if len(rows) < 2:
        print("Need at least 2 snapshots for comparison.")
        return

    snapshots = [(json.loads(r[0]), r[1]) for r in rows]
    snapshots.reverse()  # oldest first

    print(f"Analyzing {len(snapshots)} CCS snapshots\n")

    # Per-field change across consecutive pairs
    field_changes: dict[str, list[float]] = {f: [] for f in ALL_FIELDS}

    for i in range(1, len(snapshots)):
        prev, prev_ts = snapshots[i-1]
        curr, curr_ts = snapshots[i]
        for field in ALL_FIELDS:
            prev_val = field_value(prev, field)
            curr_val = field_value(curr, field)
            change = 1.0 - similarity(prev_val, curr_val)
            field_changes[field].append(change)

    # Summary
    print("Field volatility (mean change per snapshot pair):")
    print("-" * 60)

    volatilities = {}
    for field in sorted(ALL_FIELDS):
        changes = field_changes[field]
        mean_change = sum(changes) / len(changes) if changes else 0
        max_change = max(changes) if changes else 0
        volatilities[field] = mean_change

        category = (
            "IDENTITY" if field in IDENTITY_FIELDS
            else "OPERATIONAL" if field in OPERATIONAL_FIELDS
            else "STRUCTURAL"
        )
        bar = "█" * int(mean_change * 40)
        print(f"  {field:25s} [{category:11s}] {mean_change:.3f} (max {max_change:.3f}) {bar}")

    # Routing recommendation
    print("\n" + "=" * 60)
    print("ROUTING RECOMMENDATION:")
    print("=" * 60)

    # Sort by volatility
    sorted_fields = sorted(volatilities.items(), key=lambda x: x[1], reverse=True)

    high_effort = [f for f, v in sorted_fields if v > 0.3]
    moderate_effort = [f for f, v in sorted_fields if 0.1 <= v <= 0.3]
    low_effort = [f for f, v in sorted_fields if v < 0.1]

    if high_effort:
        print(f"\n  HIGH effort (rebuild):    {', '.join(high_effort)}")
    if moderate_effort:
        print(f"  MODERATE effort (update): {', '.join(moderate_effort)}")
    if low_effort:
        print(f"  LOW effort (carry):       {', '.join(low_effort)}")

    # Category-level analysis
    print("\nCategory volatility:")
    for cat, fields in [
        ("IDENTITY", IDENTITY_FIELDS),
        ("OPERATIONAL", OPERATIONAL_FIELDS),
        ("STRUCTURAL", STRUCTURAL_FIELDS),
    ]:
        cat_vol = sum(volatilities.get(f, 0) for f in fields) / len(fields)
        print(f"  {cat:12s}: {cat_vol:.3f}")

    # Entropy estimate (how uniform is the effort distribution?)
    import math
    total_vol = sum(volatilities.values())
    if total_vol > 0:
        probs = [v / total_vol for v in volatilities.values() if v > 0]
        entropy = -sum(p * math.log2(p) for p in probs) if probs else 0
        max_entropy = math.log2(len(probs)) if probs else 1
        norm_entropy = entropy / max_entropy if max_entropy > 0 else 0
        print(f"\n  Effort entropy: {norm_entropy:.3f} (1.0 = uniform, 0.0 = concentrated)")
        if norm_entropy < 0.6:
            print("  → Low entropy: some fields dominate change. Routing has high payoff.")
        elif norm_entropy > 0.8:
            print("  → High entropy: changes distributed evenly. Routing has low payoff.")
        else:
            print("  → Moderate entropy: routing may help on the volatile fields.")

    # Latest pair detail
    print("\n" + "-" * 60)
    print("Latest pair change detail:")
    prev, prev_ts = snapshots[-2]
    curr, curr_ts = snapshots[-1]
    print(f"  {prev_ts} → {curr_ts}")
    for field in sorted(ALL_FIELDS):
        change = 1.0 - similarity(field_value(prev, field), field_value(curr, field))
        marker = "🔴" if change > 0.5 else "🟡" if change > 0.1 else "🟢"
        print(f"  {marker} {field:25s}: {change:.3f}")


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    analyze(n)
