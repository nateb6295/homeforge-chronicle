#!/usr/bin/env python3
"""Retention Half-Life Tracker — measures CCS entity memory decay.

The temporal memory compression paper (arxiv 2604.00067) predicts:
  retention half-life ≈ 2.4 × L
where L = segment budget (entity slots).

CCS has 7 entity slots → predicted half-life = 16.8 compressions.
Empirical measurement (2026-04-18, 50 snapshots): half-life = 1.9 compressions.
That's 8.8x worse than theoretical minimum.

This tracker:
1. Computes entity survival curves from CCS history
2. Derives empirical half-life
3. Compares against theoretical prediction
4. Tracks half-life over time to measure stabilizer impact

Usage:
  python3 retention_halflife.py              # Full analysis
  python3 retention_halflife.py --window 20  # Last 20 snapshots only
  python3 retention_halflife.py --log        # Append to tracking log
  python3 retention_halflife.py --trend      # Show half-life trend from log
"""

import json
import os
import sqlite3
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

from entity_guard import classify_entity_type

DB = Path("/mnt/hdd/chronicle-data/processed.db")
LOG_FILE = os.path.expanduser("~/chronicle/data/halflife_log.jsonl")

# Temporal compression paper prediction
SLOT_BUDGET = 7  # 7±2 entity slots per CCS schema
PREDICTED_HALFLIFE = 2.4 * SLOT_BUDGET  # ≈ 16.8


def get_snapshots(n: int = None) -> list[dict]:
    """Get CCS snapshots from history, optionally limited to last N."""
    db = sqlite3.connect(str(DB))
    if n:
        rows = db.execute(
            "SELECT id, snapshot, created_at FROM cognitive_state_history "
            "ORDER BY created_at DESC LIMIT ?", (n,)
        ).fetchall()
    else:
        rows = db.execute(
            "SELECT id, snapshot, created_at FROM cognitive_state_history "
            "ORDER BY created_at ASC"
        ).fetchall()
    db.close()

    results = []
    for row in rows:
        try:
            snap = json.loads(row[1])
            snap["_id"] = row[0]
            snap["_ts"] = row[2]
            results.append(snap)
        except json.JSONDecodeError:
            continue

    if n:
        results.reverse()  # chronological order
    return results


def extract_entities(snap: dict) -> set[str]:
    """Get normalized entity names from a snapshot."""
    entities = snap.get("focal_entities", [])
    if isinstance(entities, str):
        try:
            entities = json.loads(entities)
        except json.JSONDecodeError:
            return set()
    return {e.get("name", "").lower().strip() for e in entities if e.get("name")}


def compute_survival_curve(snapshots: list[dict]) -> dict:
    """Compute entity survival curve from consecutive snapshots.

    Returns:
        {
            "runs": list of all consecutive run lengths,
            "survival": {k: P(survive >= k)},
            "half_life": float,
            "mean_run": float,
            "median_run": int,
            "total_entities": int (unique),
            "snapshot_count": int,
            "longest_lived": {name: max_run_length},
        }
    """
    n = len(snapshots)
    entity_runs = defaultdict(list)
    current_runs = {}

    for snap in snapshots:
        present = extract_entities(snap)

        # Increment runs for present entities
        for e in present:
            if e in current_runs:
                current_runs[e] += 1
            else:
                current_runs[e] = 1

        # Close runs for absent entities
        to_close = [e for e in current_runs if e not in present]
        for e in to_close:
            entity_runs[e].append(current_runs[e])
            del current_runs[e]

    # Close remaining open runs
    for e, length in current_runs.items():
        entity_runs[e].append(length)

    all_runs = []
    for runs in entity_runs.values():
        all_runs.extend(runs)

    if not all_runs:
        return {
            "runs": [], "survival": {}, "half_life": 0,
            "mean_run": 0, "median_run": 0,
            "total_entities": 0, "snapshot_count": n,
            "longest_lived": {},
        }

    # Survival curve
    max_len = max(all_runs)
    survival = {}
    for k in range(1, max_len + 1):
        surviving = sum(1 for r in all_runs if r >= k)
        survival[k] = surviving / len(all_runs)

    # Half-life: where survival drops below 0.5
    half_life = None
    for k in sorted(survival.keys()):
        if survival[k] < 0.5:
            # Linear interpolation between k-1 and k
            if k > 1 and survival.get(k - 1, 1.0) >= 0.5:
                prev = survival[k - 1]
                curr = survival[k]
                half_life = (k - 1) + (prev - 0.5) / (prev - curr)
            else:
                half_life = float(k)
            break

    if half_life is None:
        half_life = float(max_len)  # All entities survive the full window

    # Longest-lived entities
    longest = {}
    for e, runs in entity_runs.items():
        longest[e] = max(runs)

    sorted_runs = sorted(all_runs)
    median_run = sorted_runs[len(sorted_runs) // 2]

    return {
        "runs": all_runs,
        "survival": survival,
        "half_life": round(half_life, 2),
        "mean_run": round(sum(all_runs) / len(all_runs), 2),
        "median_run": median_run,
        "total_entities": len(entity_runs),
        "snapshot_count": n,
        "longest_lived": dict(sorted(longest.items(), key=lambda x: -x[1])[:10]),
    }


def compute_type_halflife(snapshots: list[dict]) -> dict[str, dict]:
    """Compute per-type entity survival metrics.

    Returns: {type_name: {"half_life": float, "avg_run": float, "entities": int, "one_shot_pct": float}}
    """
    type_runs = defaultdict(list)
    current_runs = {}
    entity_types = {}

    for snap in snapshots:
        present = extract_entities(snap)
        for e in present:
            if e not in entity_types:
                entity_types[e] = classify_entity_type(e)
            current_runs[e] = current_runs.get(e, 0) + 1

        to_close = [e for e in current_runs if e not in present]
        for e in to_close:
            type_runs[entity_types.get(e, "concept")].append(current_runs[e])
            del current_runs[e]

    for e, length in current_runs.items():
        type_runs[entity_types.get(e, "concept")].append(length)

    result = {}
    for etype, runs in type_runs.items():
        if not runs:
            continue
        # Half-life calculation
        max_len = max(runs)
        hl = float(max_len)
        for k in range(1, max_len + 1):
            surviving = sum(1 for r in runs if r >= k) / len(runs)
            if surviving < 0.5:
                if k > 1:
                    prev = sum(1 for r in runs if r >= k - 1) / len(runs)
                    hl = (k - 1) + (prev - 0.5) / (prev - surviving)
                else:
                    hl = float(k)
                break

        result[etype] = {
            "half_life": round(hl, 1),
            "avg_run": round(sum(runs) / len(runs), 2),
            "entities": len(runs),
            "one_shot_pct": round(sum(1 for r in runs if r == 1) / len(runs) * 100, 1),
        }

    return result


def compute_transition_metrics(snapshots: list[dict]) -> dict:
    """Compute per-transition entity retention metrics.

    For each consecutive pair, measures:
    - How many entities survived
    - How many were dropped
    - How many were added
    """
    transitions = []
    for i in range(1, len(snapshots)):
        prev = extract_entities(snapshots[i - 1])
        curr = extract_entities(snapshots[i])

        if not prev:
            continue

        retained = prev & curr
        dropped = prev - curr
        added = curr - prev

        transitions.append({
            "idx": i,
            "prev_count": len(prev),
            "curr_count": len(curr),
            "retained": len(retained),
            "dropped": len(dropped),
            "added": len(added),
            "retention_rate": len(retained) / len(prev) if prev else 1.0,
        })

    if not transitions:
        return {"mean_retention": 0, "worst_retention": 0, "flush_events": 0}

    retention_rates = [t["retention_rate"] for t in transitions]

    return {
        "transitions": len(transitions),
        "mean_retention": round(sum(retention_rates) / len(retention_rates), 3),
        "worst_retention": round(min(retention_rates), 3),
        "best_retention": round(max(retention_rates), 3),
        "flush_events": sum(1 for r in retention_rates if r == 0),
        "full_retain_events": sum(1 for r in retention_rates if r == 1.0),
    }


def print_analysis(snapshots: list[dict]):
    """Print full retention analysis."""
    curve = compute_survival_curve(snapshots)
    trans = compute_transition_metrics(snapshots)

    print(f"=== CCS Entity Retention Analysis ({curve['snapshot_count']} snapshots) ===\n")

    # Half-life comparison
    ratio = PREDICTED_HALFLIFE / curve["half_life"] if curve["half_life"] > 0 else float("inf")
    print(f"RETENTION HALF-LIFE:")
    print(f"  Empirical:  {curve['half_life']:.1f} compressions")
    print(f"  Predicted:  {PREDICTED_HALFLIFE:.1f} compressions (2.4 × {SLOT_BUDGET} slots)")
    print(f"  Gap:        {ratio:.1f}x worse than theoretical minimum")
    print(f"  Mean run:   {curve['mean_run']:.2f} compressions")
    print(f"  Median run: {curve['median_run']}")
    print()

    # Survival curve (top 15)
    print(f"SURVIVAL CURVE (P(entity survives >= k compressions)):")
    max_k = min(15, max(curve["survival"].keys())) if curve["survival"] else 0
    for k in range(1, max_k + 1):
        p = curve["survival"].get(k, 0)
        bar = "█" * int(p * 40)
        print(f"  k={k:2d}: {p:.3f} {bar}")
    print()

    # Transition metrics
    print(f"TRANSITION METRICS:")
    print(f"  Mean retention per compression: {trans['mean_retention']:.1%}")
    print(f"  Worst single transition:        {trans['worst_retention']:.1%}")
    print(f"  Best single transition:         {trans['best_retention']:.1%}")
    print(f"  Total flush events (0%):        {trans['flush_events']}")
    print(f"  Full retain events (100%):      {trans['full_retain_events']}")
    print()

    # Longest-lived entities
    print(f"LONGEST-LIVED ENTITIES:")
    for name, length in curve["longest_lived"].items():
        pct_of_window = length / curve["snapshot_count"] * 100
        print(f"  {name:<35} {length:>3} compressions ({pct_of_window:.0f}% of window)")
    print()

    # Type-specific half-lives
    type_hl = compute_type_halflife(snapshots)
    if type_hl:
        print(f"TYPE-SPECIFIC HALF-LIVES:")
        for etype in ["agent", "thread", "file", "concept"]:
            if etype in type_hl:
                t = type_hl[etype]
                print(f"  {etype:<10} HL={t['half_life']:<6} avg_run={t['avg_run']:<6} entities={t['entities']:<4} 1-shot={t['one_shot_pct']:.0f}%")
        print()

    # Entity diversity
    avg_per_snap = sum(len(extract_entities(s)) for s in snapshots) / len(snapshots)
    churn = curve["total_entities"] / avg_per_snap if avg_per_snap else 0
    print(f"ENTITY DYNAMICS:")
    print(f"  Unique entities seen:    {curve['total_entities']}")
    print(f"  Avg entities per snap:   {avg_per_snap:.1f}")
    print(f"  Churn ratio:             {churn:.1f}x (unique/avg_per_snap)")
    print()

    # Diagnosis
    print(f"DIAGNOSIS:")
    if curve["half_life"] < 3:
        print(f"  ⚠ CRITICAL — entities survive less than 3 compressions on average.")
        print(f"  The compressor is treating entity slots as scratch pad, not persistent memory.")
        print(f"  Stabilizer injection should increase half-life toward {PREDICTED_HALFLIFE:.0f}.")
    elif curve["half_life"] < PREDICTED_HALFLIFE / 2:
        print(f"  ⚡ BELOW THEORETICAL — half-life {curve['half_life']:.1f} vs predicted {PREDICTED_HALFLIFE:.1f}.")
        print(f"  Stabilizer is helping but not yet at theoretical efficiency.")
    else:
        print(f"  ✓ NEAR THEORETICAL — half-life {curve['half_life']:.1f} approaches predicted {PREDICTED_HALFLIFE:.1f}.")
        print(f"  Entity persistence is at expected efficiency for {SLOT_BUDGET}-slot system.")

    if trans["flush_events"] > 0:
        flush_pct = trans["flush_events"] / trans["transitions"] * 100 if trans.get("transitions") else 0
        print(f"  ⚠ {trans['flush_events']} total flush events ({flush_pct:.0f}% of transitions)")


def log_measurement(curve: dict, trans: dict, window: int, type_hl: dict = None):
    """Append measurement to tracking log."""
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    entry = {
        "ts": int(time.time()),
        "window": window,
        "half_life": curve["half_life"],
        "mean_run": curve["mean_run"],
        "median_run": curve["median_run"],
        "total_entities": curve["total_entities"],
        "snapshot_count": curve["snapshot_count"],
        "mean_retention": trans.get("mean_retention", 0),
        "flush_events": trans.get("flush_events", 0),
        "predicted": PREDICTED_HALFLIFE,
        "gap_ratio": round(PREDICTED_HALFLIFE / curve["half_life"], 2) if curve["half_life"] > 0 else None,
    }
    if type_hl:
        entry["type_halflife"] = type_hl
    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(entry) + "\n")
    print(f"Logged: half_life={curve['half_life']}, gap={entry['gap_ratio']}x")
    if type_hl:
        for t in ["agent", "thread", "file", "concept"]:
            if t in type_hl:
                print(f"  {t}: HL={type_hl[t]['half_life']}")


def show_trend():
    """Show half-life trend from log."""
    if not os.path.exists(LOG_FILE):
        print("No tracking log yet. Run with --log first.")
        return

    with open(LOG_FILE) as f:
        entries = [json.loads(line) for line in f if line.strip()]

    if not entries:
        print("Log is empty.")
        return

    print(f"=== Half-Life Trend ({len(entries)} measurements) ===\n")
    print(f"  {'Time':<20} {'Half-Life':>10} {'Gap':>6} {'Mean Ret':>9} {'Flushes':>8}")
    print(f"  {'-'*20} {'-'*10} {'-'*6} {'-'*9} {'-'*8}")

    from datetime import datetime
    for e in entries:
        ts = datetime.fromtimestamp(e["ts"]).strftime("%Y-%m-%d %H:%M")
        gap = f"{e['gap_ratio']}x" if e.get("gap_ratio") else "?"
        mr = f"{e['mean_retention']:.1%}" if e.get("mean_retention") else "?"
        print(f"  {ts:<20} {e['half_life']:>10.1f} {gap:>6} {mr:>9} {e.get('flush_events', '?'):>8}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="CCS Entity Retention Half-Life Tracker")
    parser.add_argument("--window", type=int, default=None,
                        help="Analyze last N snapshots (default: all)")
    parser.add_argument("--log", action="store_true",
                        help="Append measurement to tracking log")
    parser.add_argument("--trend", action="store_true",
                        help="Show half-life trend from log")
    parser.add_argument("--json", action="store_true",
                        help="Output as JSON")
    args = parser.parse_args()

    if args.trend:
        show_trend()
        return

    snapshots = get_snapshots(args.window)
    if not snapshots:
        print("No CCS snapshots found.")
        return

    curve = compute_survival_curve(snapshots)
    trans = compute_transition_metrics(snapshots)
    type_hl = compute_type_halflife(snapshots)

    if args.json:
        out = {
            "half_life": curve["half_life"],
            "predicted": PREDICTED_HALFLIFE,
            "gap_ratio": round(PREDICTED_HALFLIFE / curve["half_life"], 2) if curve["half_life"] > 0 else None,
            "mean_run": curve["mean_run"],
            "median_run": curve["median_run"],
            "total_entities": curve["total_entities"],
            "snapshot_count": curve["snapshot_count"],
            "longest_lived": curve["longest_lived"],
            "transitions": trans,
            "type_halflife": type_hl,
        }
        print(json.dumps(out, indent=2))
    else:
        print_analysis(snapshots)

    if args.log:
        window = args.window or len(snapshots)
        log_measurement(curve, trans, window, type_hl)


if __name__ == "__main__":
    main()
