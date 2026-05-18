#!/usr/bin/env python3
"""tolerance_audit.py — measure immune tolerance in CCS entity management.

Evaluates whether entity deletions (thymic selection) and retentions
(immune privilege) across CCS compressions were correct or harmful.

Classifications:
  correct_deletion   — entity dropped, not needed in subsequent sessions
  autoimmune         — entity dropped, but referenced in later activity (wrong deletion)
  correct_retention  — entity kept, referenced in later activity
  immunodeficient    — entity kept, but never referenced again (bloat)

Usage:
  python3 tolerance_audit.py              # Full audit with summary
  python3 tolerance_audit.py --json       # Machine-readable
  python3 tolerance_audit.py --detail     # Show each entity decision
"""

import argparse
import json
import sqlite3
import sys
from pathlib import Path

DB = Path("/mnt/hdd/chronicle-data/processed.db")

LOOKFORWARD_SECONDS = 86400  # 24h window to check if entity was "needed"


def get_snapshots(db: sqlite3.Connection) -> list[dict]:
    rows = db.execute(
        "SELECT created_at, snapshot FROM cognitive_state_history "
        "ORDER BY created_at ASC"
    ).fetchall()
    results = []
    for ts, snap_raw in rows:
        try:
            snap = json.loads(snap_raw)
            entities = snap.get("focal_entities", [])
            names = {e["name"] for e in entities if isinstance(e, dict)}
            results.append({"ts": ts, "entities": names, "raw": entities})
        except (json.JSONDecodeError, TypeError):
            continue
    return results


def entity_referenced_after(db: sqlite3.Connection, name: str, after_ts: float, window: int = LOOKFORWARD_SECONDS) -> bool:
    row = db.execute(
        "SELECT 1 FROM activity_feed WHERE content LIKE ? AND created_at > ? AND created_at < ? LIMIT 1",
        (f"%{name}%", after_ts, after_ts + window)
    ).fetchone()
    return row is not None


def audit(db: sqlite3.Connection) -> dict:
    snapshots = get_snapshots(db)
    if len(snapshots) < 2:
        return {"error": "Need at least 2 CCS snapshots", "transitions": []}

    transitions = []

    for i in range(1, len(snapshots)):
        prev = snapshots[i - 1]
        curr = snapshots[i]
        ts = curr["ts"]

        dropped = prev["entities"] - curr["entities"]
        added = curr["entities"] - prev["entities"]
        kept = prev["entities"] & curr["entities"]

        decisions = []

        for name in dropped:
            needed = entity_referenced_after(db, name, ts)
            decisions.append({
                "entity": name,
                "action": "dropped",
                "classification": "autoimmune" if needed else "correct_deletion",
                "needed_later": needed,
            })

        for name in kept:
            needed = entity_referenced_after(db, name, ts)
            decisions.append({
                "entity": name,
                "action": "kept",
                "classification": "correct_retention" if needed else "immunodeficient",
                "needed_later": needed,
            })

        for name in added:
            decisions.append({
                "entity": name,
                "action": "added",
                "classification": "new",
                "needed_later": entity_referenced_after(db, name, ts),
            })

        transitions.append({
            "compression_ts": ts,
            "dropped": len(dropped),
            "added": len(added),
            "kept": len(kept),
            "decisions": decisions,
        })

    counts = {"correct_deletion": 0, "autoimmune": 0, "correct_retention": 0, "immunodeficient": 0, "new": 0}
    for t in transitions:
        for d in t["decisions"]:
            counts[d["classification"]] += 1

    total_drops = counts["correct_deletion"] + counts["autoimmune"]
    total_keeps = counts["correct_retention"] + counts["immunodeficient"]

    return {
        "snapshots": len(snapshots),
        "transitions": len(transitions),
        "counts": counts,
        "deletion_accuracy": round(counts["correct_deletion"] / total_drops, 3) if total_drops else None,
        "retention_accuracy": round(counts["correct_retention"] / total_keeps, 3) if total_keeps else None,
        "autoimmune_rate": round(counts["autoimmune"] / total_drops, 3) if total_drops else None,
        "bloat_rate": round(counts["immunodeficient"] / total_keeps, 3) if total_keeps else None,
        "detail": transitions,
    }


def main():
    parser = argparse.ArgumentParser(description="CCS entity tolerance audit")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--detail", action="store_true")
    args = parser.parse_args()

    db = sqlite3.connect(str(DB))
    result = audit(db)
    db.close()

    if args.json:
        print(json.dumps(result, indent=2))
        return

    if "error" in result:
        print(result["error"])
        return

    print(f"=== CCS Entity Tolerance Audit ===\n")
    print(f"Snapshots analyzed: {result['snapshots']}")
    print(f"Compression transitions: {result['transitions']}")
    print()

    c = result["counts"]
    print("Entity Decisions:")
    print(f"  Correct deletions:  {c['correct_deletion']}")
    print(f"  Autoimmune (wrong): {c['autoimmune']}")
    print(f"  Correct retentions: {c['correct_retention']}")
    print(f"  Immunodeficient:    {c['immunodeficient']}")
    print(f"  New additions:      {c['new']}")
    print()

    if result["deletion_accuracy"] is not None:
        print(f"Deletion accuracy:  {result['deletion_accuracy']:.1%} (1 - autoimmune rate)")
    if result["retention_accuracy"] is not None:
        print(f"Retention accuracy: {result['retention_accuracy']:.1%} (1 - bloat rate)")
    if result["autoimmune_rate"] is not None:
        print(f"Autoimmune rate:    {result['autoimmune_rate']:.1%}")
    if result["bloat_rate"] is not None:
        print(f"Bloat rate:         {result['bloat_rate']:.1%}")

    if args.detail:
        print("\n--- Transition Detail ---")
        for t in result["detail"]:
            import time
            ts_str = time.strftime("%Y-%m-%d %H:%M", time.localtime(t["compression_ts"]))
            print(f"\n[{ts_str}] dropped={t['dropped']} added={t['added']} kept={t['kept']}")
            for d in t["decisions"]:
                marker = {"autoimmune": "!!", "immunodeficient": "~~", "correct_deletion": "OK", "correct_retention": "OK", "new": "++"}
                print(f"  {marker[d['classification']]:>2} {d['action']:>7} {d['entity']}")


if __name__ == "__main__":
    main()
