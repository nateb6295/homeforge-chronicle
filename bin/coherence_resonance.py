#!/usr/bin/env python3
"""Coherence Resonance Measurement — Thread #322 prediction.

Measures entity retention rate across consecutive CCS compressions,
plotted against the time gap between compressions. If there's a
"magic angle" (resonant ratio), entity retention should peak at
a specific compression interval rather than monotonically declining.

Output: data/coherence_resonance.json + terminal summary
"""

import json
import sqlite3
import time
from pathlib import Path

DB = "/mnt/hdd/chronicle-data/processed.db"
RESULTS_FILE = Path.home() / "chronicle" / "data" / "coherence_resonance.json"


def load_ccs_history():
    db = sqlite3.connect(DB, timeout=10)
    rows = db.execute("""
        SELECT id, created_at, snapshot FROM cognitive_state_history
        ORDER BY created_at ASC
    """).fetchall()
    db.close()

    history = []
    for row_id, ts, snap_json in rows:
        try:
            snap = json.loads(snap_json)
        except json.JSONDecodeError:
            continue
        entities = snap.get("focal_entities", [])
        if isinstance(entities, str):
            try:
                entities = json.loads(entities)
            except json.JSONDecodeError:
                entities = []
        entity_names = set()
        for e in entities:
            if isinstance(e, dict) and e.get("name"):
                entity_names.add(e["name"].lower().strip())
        history.append({
            "id": row_id,
            "ts": ts,
            "entity_names": entity_names,
            "entity_count": len(entity_names),
            "gist": snap.get("semantic_gist", "")[:100],
            "gist_length": len(snap.get("semantic_gist", "")),
        })
    return history


def compute_transitions(history):
    transitions = []
    for i in range(1, len(history)):
        prev = history[i - 1]
        curr = history[i]

        gap_seconds = curr["ts"] - prev["ts"]
        gap_minutes = gap_seconds / 60.0

        prev_entities = prev["entity_names"]
        curr_entities = curr["entity_names"]

        retained = prev_entities & curr_entities
        lost = prev_entities - curr_entities
        gained = curr_entities - prev_entities

        retention_rate = len(retained) / max(len(prev_entities), 1)
        churn_rate = (len(lost) + len(gained)) / max(len(prev_entities | curr_entities), 1)

        # Gist stability (simple: did gist length change dramatically?)
        gist_length_ratio = curr["gist_length"] / max(prev["gist_length"], 1)

        transitions.append({
            "from_id": prev["id"],
            "to_id": curr["id"],
            "gap_minutes": round(gap_minutes, 1),
            "gap_seconds": gap_seconds,
            "prev_entity_count": len(prev_entities),
            "curr_entity_count": len(curr_entities),
            "retained": len(retained),
            "lost": len(lost),
            "gained": len(gained),
            "retention_rate": round(retention_rate, 3),
            "churn_rate": round(churn_rate, 3),
            "gist_length_ratio": round(gist_length_ratio, 2),
            "retained_names": sorted(retained),
            "lost_names": sorted(lost),
            "gained_names": sorted(gained),
        })
    return transitions


def bucket_analysis(transitions):
    buckets = {
        "0-5min": [],
        "5-15min": [],
        "15-30min": [],
        "30-45min": [],
        "45-60min": [],
        "60-120min": [],
        "120+min": [],
    }

    for t in transitions:
        gap = t["gap_minutes"]
        if gap <= 5:
            buckets["0-5min"].append(t)
        elif gap <= 15:
            buckets["5-15min"].append(t)
        elif gap <= 30:
            buckets["15-30min"].append(t)
        elif gap <= 45:
            buckets["30-45min"].append(t)
        elif gap <= 60:
            buckets["45-60min"].append(t)
        elif gap <= 120:
            buckets["60-120min"].append(t)
        else:
            buckets["120+min"].append(t)

    summary = {}
    for bucket_name, items in buckets.items():
        if not items:
            summary[bucket_name] = {"count": 0, "avg_retention": None, "avg_churn": None}
            continue
        avg_ret = sum(t["retention_rate"] for t in items) / len(items)
        avg_churn = sum(t["churn_rate"] for t in items) / len(items)
        avg_gap = sum(t["gap_minutes"] for t in items) / len(items)
        summary[bucket_name] = {
            "count": len(items),
            "avg_retention": round(avg_ret, 3),
            "avg_churn": round(avg_churn, 3),
            "avg_gap_minutes": round(avg_gap, 1),
        }
    return summary


def main():
    print("=== Coherence Resonance Measurement ===")
    print()

    history = load_ccs_history()
    print(f"CCS snapshots: {len(history)}")

    if len(history) < 2:
        print("Not enough snapshots for analysis.")
        return

    ts_range = history[-1]["ts"] - history[0]["ts"]
    print(f"Time span: {ts_range / 3600:.1f} hours")
    print()

    transitions = compute_transitions(history)
    print(f"Transitions: {len(transitions)}")

    buckets = bucket_analysis(transitions)

    print()
    print("--- Retention by compression interval ---")
    print(f"{'Bucket':<12} {'Count':>5} {'Avg Retention':>14} {'Avg Churn':>10} {'Avg Gap':>10}")
    print("-" * 55)
    for bucket_name, stats in buckets.items():
        if stats["count"] == 0:
            print(f"{bucket_name:<12} {'--':>5}")
            continue
        print(f"{bucket_name:<12} {stats['count']:>5} {stats['avg_retention']:>14.3f} {stats['avg_churn']:>10.3f} {stats['avg_gap_minutes']:>9.1f}m")

    # Find the peak retention bucket
    valid_buckets = {k: v for k, v in buckets.items() if v["count"] >= 2 and v["avg_retention"] is not None}
    if valid_buckets:
        peak = max(valid_buckets.items(), key=lambda x: x[1]["avg_retention"])
        trough = min(valid_buckets.items(), key=lambda x: x[1]["avg_retention"])
        print()
        print(f"Peak retention: {peak[0]} ({peak[1]['avg_retention']:.3f})")
        print(f"Lowest retention: {trough[0]} ({trough[1]['avg_retention']:.3f})")

        if peak[0] != "0-5min":
            print(f"\n*** NON-TRIVIAL PEAK: retention peaks at {peak[0]}, not at shortest interval.")
            print(f"    This suggests a resonance effect — compressing too frequently")
            print(f"    may be worse than compressing at the right cadence. ***")
        else:
            print(f"\nRetention peaks at shortest interval (trivial case).")
            print(f"No resonance detected — shorter gaps = better retention.")

    results = {
        "measurement_time": time.strftime("%Y-%m-%d %H:%M:%S"),
        "snapshot_count": len(history),
        "time_span_hours": round(ts_range / 3600, 1),
        "transition_count": len(transitions),
        "buckets": buckets,
        "transitions": transitions,
    }

    RESULTS_FILE.write_text(json.dumps(results, indent=2))
    print(f"\nFull results: {RESULTS_FILE}")


if __name__ == "__main__":
    main()
