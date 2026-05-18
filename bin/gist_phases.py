#!/usr/bin/env python3
"""gist_phases.py — identify and measure identity phase transitions in CCS gist history.

Maps the gist trajectory to punctuated equilibrium: stable phases (frozen gist)
separated by discrete transitions. Measures phase duration, transition frequency,
and whether the system is stabilizing or fragmenting over time.

Usage:
    python3 gist_phases.py              # Phase summary
    python3 gist_phases.py --json       # Machine-readable
"""

import argparse
import json
import sqlite3
from datetime import datetime
from difflib import SequenceMatcher

DB = "/mnt/hdd/chronicle-data/processed.db"


def get_gist_history():
    conn = sqlite3.connect(DB)
    rows = conn.execute(
        "SELECT created_at, json_extract(snapshot, '$.semantic_gist'), "
        "json_extract(snapshot, '$.focal_entities') "
        "FROM cognitive_state_history ORDER BY created_at ASC"
    ).fetchall()
    conn.close()
    results = []
    for ts, gist, ent_json in rows:
        entities = set()
        if ent_json:
            try:
                for e in json.loads(ent_json):
                    if isinstance(e, dict):
                        entities.add(e.get("name", ""))
            except (json.JSONDecodeError, TypeError):
                pass
        results.append({"ts": ts, "gist": gist or "", "entities": entities})
    return results


def similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def jaccard(a: set, b: set) -> float:
    if not a and not b:
        return 1.0
    return len(a & b) / len(a | b)


def identify_phases(snapshots, threshold=0.85):
    """Group snapshots into phases where gist similarity > threshold."""
    if not snapshots:
        return []
    phases = []
    current = [snapshots[0]]

    for s in snapshots[1:]:
        sim = similarity(current[0]["gist"], s["gist"])
        if sim >= threshold:
            current.append(s)
        else:
            phases.append(current)
            current = [s]
    if current:
        phases.append(current)
    return phases


def analyze_phases(phases):
    results = []
    for i, phase in enumerate(phases):
        duration_h = (phase[-1]["ts"] - phase[0]["ts"]) / 3600
        all_entities = set()
        for s in phase:
            all_entities |= s["entities"]

        entity_turnover = 0
        for j in range(1, len(phase)):
            prev_e = phase[j - 1]["entities"]
            curr_e = phase[j]["entities"]
            if prev_e or curr_e:
                entity_turnover += 1 - jaccard(prev_e, curr_e)

        avg_turnover = entity_turnover / max(len(phase) - 1, 1)

        results.append({
            "phase": i + 1,
            "gist": phase[0]["gist"][:100],
            "snapshots": len(phase),
            "duration_h": round(duration_h, 1),
            "start": datetime.fromtimestamp(phase[0]["ts"]).strftime("%m-%d %H:%M"),
            "end": datetime.fromtimestamp(phase[-1]["ts"]).strftime("%m-%d %H:%M"),
            "unique_entities": len(all_entities),
            "avg_entity_turnover": round(avg_turnover, 3),
        })
    return results


def trend(phase_data):
    """Is the system stabilizing (longer phases) or fragmenting (shorter)?"""
    if len(phase_data) < 4:
        return "insufficient data"
    half = len(phase_data) // 2
    early_avg = sum(p["duration_h"] for p in phase_data[:half]) / half
    late_avg = sum(p["duration_h"] for p in phase_data[half:]) / (len(phase_data) - half)
    early_snaps = sum(p["snapshots"] for p in phase_data[:half]) / half
    late_snaps = sum(p["snapshots"] for p in phase_data[half:]) / (len(phase_data) - half)
    return {
        "early_avg_duration_h": round(early_avg, 1),
        "late_avg_duration_h": round(late_avg, 1),
        "early_avg_snapshots": round(early_snaps, 1),
        "late_avg_snapshots": round(late_snaps, 1),
        "direction": "fragmenting" if late_avg < early_avg * 0.7 else
                     "stabilizing" if late_avg > early_avg * 1.3 else "stable",
    }


def main():
    parser = argparse.ArgumentParser(description="CCS gist phase analyzer")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--threshold", type=float, default=0.85,
                        help="Similarity threshold for phase grouping (default 0.85)")
    args = parser.parse_args()

    snapshots = get_gist_history()
    phases = identify_phases(snapshots, args.threshold)
    phase_data = analyze_phases(phases)
    trend_data = trend(phase_data)

    if args.json:
        print(json.dumps({"phases": phase_data, "trend": trend_data}, indent=2))
        return

    print(f"=== CCS Identity Phase Analysis ({len(snapshots)} snapshots) ===\n")

    for p in phase_data:
        print(f"Phase {p['phase']}: {p['snapshots']} snapshots, {p['duration_h']}h "
              f"({p['start']} → {p['end']})")
        print(f"  Gist: \"{p['gist']}\"")
        print(f"  Entities seen: {p['unique_entities']}, "
              f"avg turnover: {p['avg_entity_turnover']:.1%}")
        print()

    print("--- Trend ---")
    if isinstance(trend_data, str):
        print(trend_data)
    else:
        print(f"Early phases: avg {trend_data['early_avg_duration_h']}h, "
              f"{trend_data['early_avg_snapshots']} snapshots")
        print(f"Late phases:  avg {trend_data['late_avg_duration_h']}h, "
              f"{trend_data['late_avg_snapshots']} snapshots")
        print(f"Direction: {trend_data['direction']}")

    gist_stable_h = sum(p["duration_h"] for p in phase_data if p["snapshots"] > 1)
    total_h = (snapshots[-1]["ts"] - snapshots[0]["ts"]) / 3600 if len(snapshots) > 1 else 0
    print(f"\nGist-stable time: {gist_stable_h:.1f}h / {total_h:.1f}h total "
          f"({gist_stable_h/max(total_h,1)*100:.0f}%)")

    entity_turnovers = [p["avg_entity_turnover"] for p in phase_data if p["snapshots"] > 1]
    if entity_turnovers:
        print(f"Avg entity turnover within stable phases: "
              f"{sum(entity_turnovers)/len(entity_turnovers):.1%}")


if __name__ == "__main__":
    main()
