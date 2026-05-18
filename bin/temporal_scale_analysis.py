#!/usr/bin/env python3
"""Temporal scale analysis of CCS fields.

Bloch's Ungleichzeitigkeit applied: if CCS fields operate at different
temporal scales, this should be measurable in the version history.

For each field, compute a change-rate across consecutive snapshots:
- Constraints (hypothesis: geological — near-static)
- Relational map (hypothesis: tectonic — slow drift)
- Focal entities (hypothesis: seasonal — salience shifts)
- Episodic trace (hypothesis: weather — changes every session)
- Uncertainty signals (hypothesis: tidal — resolve or deepen)
- Semantic gist (hypothesis: daily — topic shifts)
- Goal orientation (hypothesis: weekly — persistent but updating)

Output: change rate per field, confirming or denying temporal lamination.
"""
import json
import sqlite3
import sys
from difflib import SequenceMatcher

DB = "/mnt/hdd/chronicle-data/processed.db"


def normalize(val):
    if isinstance(val, list):
        return json.dumps(val, sort_keys=True)
    if isinstance(val, dict):
        return json.dumps(val, sort_keys=True)
    if val is None:
        return ""
    return str(val)


def similarity(a, b):
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def entity_overlap(a_list, b_list):
    if not a_list and not b_list:
        return 1.0
    if not a_list or not b_list:
        return 0.0
    a_names = {e.get("name", "") for e in a_list if isinstance(e, dict)}
    b_names = {e.get("name", "") for e in b_list if isinstance(e, dict)}
    if not a_names and not b_names:
        return 1.0
    if not a_names or not b_names:
        return 0.0
    overlap = len(a_names & b_names)
    total = len(a_names | b_names)
    return overlap / total if total > 0 else 1.0


def main():
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT id, snapshot, created_at FROM cognitive_state_history ORDER BY id ASC"
    ).fetchall()
    db.close()

    if len(rows) < 2:
        print("Need at least 2 snapshots.")
        return

    snapshots = []
    for row in rows:
        try:
            data = json.loads(row[1])
            data["_id"] = row[0]
            data["_ts"] = row[2]
            snapshots.append(data)
        except (json.JSONDecodeError, TypeError):
            continue

    print(f"Analyzing {len(snapshots)} CCS snapshots for temporal scale patterns.\n")

    fields = {
        "constraints": "geological",
        "relational_map": "tectonic",
        "focal_entities": "seasonal",
        "episodic_trace": "weather",
        "uncertainty_signals": "tidal",
        "semantic_gist": "daily",
        "goal_orientation": "weekly",
        "predictive_cue": "forecast",
    }

    results = {f: [] for f in fields}

    for i in range(1, len(snapshots)):
        prev = snapshots[i - 1]
        curr = snapshots[i]

        for field in fields:
            pv = prev.get(field)
            cv = curr.get(field)

            if field == "focal_entities":
                if isinstance(pv, list) and isinstance(cv, list):
                    sim = entity_overlap(pv, cv)
                else:
                    sim = similarity(normalize(pv), normalize(cv))
            else:
                sim = similarity(normalize(pv), normalize(cv))

            results[field].append(1.0 - sim)

    print(f"{'Field':<22} {'Hypothesis':<12} {'Mean Δ':>8} {'Median Δ':>10} {'StdDev':>8} {'Min':>6} {'Max':>6} {'Zero%':>7}")
    print("-" * 85)

    ranked = []
    for field, hypothesis in fields.items():
        changes = results[field]
        if not changes:
            continue
        mean = sum(changes) / len(changes)
        sorted_c = sorted(changes)
        median = sorted_c[len(sorted_c) // 2]
        variance = sum((c - mean) ** 2 for c in changes) / len(changes)
        std = variance ** 0.5
        zero_pct = sum(1 for c in changes if c == 0.0) / len(changes) * 100
        mn = min(changes)
        mx = max(changes)

        print(f"{field:<22} {hypothesis:<12} {mean:>8.3f} {median:>10.3f} {std:>8.3f} {mn:>6.3f} {mx:>6.3f} {zero_pct:>6.1f}%")
        ranked.append((mean, field, hypothesis, zero_pct))

    ranked.sort(key=lambda x: x[0])

    print(f"\n{'='*60}")
    print("TEMPORAL RANKING (slowest → fastest change)")
    print(f"{'='*60}")
    for i, (mean, field, hypothesis, zero_pct) in enumerate(ranked):
        speed = "STATIC" if mean < 0.05 else "SLOW" if mean < 0.2 else "MODERATE" if mean < 0.5 else "FAST" if mean < 0.8 else "VOLATILE"
        match = "✓" if (
            (hypothesis == "geological" and speed in ("STATIC", "SLOW")) or
            (hypothesis == "tectonic" and speed in ("SLOW", "MODERATE")) or
            (hypothesis in ("seasonal", "tidal") and speed in ("MODERATE", "FAST")) or
            (hypothesis in ("weather", "daily", "forecast") and speed in ("FAST", "VOLATILE"))
        ) else "✗"
        print(f"  {i+1}. {field:<22} Δ={mean:.3f} ({speed:<9}) hypothesis: {hypothesis:<12} {match}")

    confirmed = sum(1 for _, _, _, _ in ranked if True)  # recount properly
    matches = 0
    for mean, field, hypothesis, _ in ranked:
        speed = "STATIC" if mean < 0.05 else "SLOW" if mean < 0.2 else "MODERATE" if mean < 0.5 else "FAST" if mean < 0.8 else "VOLATILE"
        if (
            (hypothesis == "geological" and speed in ("STATIC", "SLOW")) or
            (hypothesis == "tectonic" and speed in ("SLOW", "MODERATE")) or
            (hypothesis in ("seasonal", "tidal") and speed in ("MODERATE", "FAST")) or
            (hypothesis in ("weather", "daily", "forecast") and speed in ("FAST", "VOLATILE"))
        ):
            matches += 1

    print(f"\nHypothesis matches: {matches}/{len(ranked)}")
    if matches >= len(ranked) * 0.6:
        print("RESULT: CONFIRMED — CCS fields operate at different temporal scales (Bloch lamination)")
    elif matches >= len(ranked) * 0.4:
        print("RESULT: PARTIAL — some temporal stratification but not fully laminated")
    else:
        print("RESULT: DISCONFIRMED — fields change at similar rates")


if __name__ == "__main__":
    main()
