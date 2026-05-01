#!/usr/bin/env python3
"""Entity-type entropy probe — Thread 318 advance 40.

Measures entity-type diversity (Shannon entropy) across CCS snapshots.
Tests prediction: higher entity-type entropy correlates with better
cross-rotation identity transfer quality.

Build 36.
"""

import json
import math
import sqlite3
import sys
from collections import Counter
from datetime import datetime

DB = "/mnt/hdd/chronicle-data/processed.db"


def entity_type_entropy(entities):
    """Shannon entropy over entity types."""
    if not entities:
        return 0.0
    types = [e.get("type", "unknown") for e in entities]
    counts = Counter(types)
    total = len(types)
    entropy = 0.0
    for count in counts.values():
        p = count / total
        if p > 0:
            entropy -= p * math.log2(p)
    return entropy


def max_entropy(n_types):
    """Maximum entropy for n types."""
    if n_types <= 1:
        return 0.0
    return math.log2(n_types)


def analyze_snapshot(snapshot_json):
    """Extract entity entropy from a CCS snapshot."""
    try:
        snap = json.loads(snapshot_json)
    except (json.JSONDecodeError, TypeError):
        return None

    # Handle both direct and nested formats
    entities = snap.get("focal_entities", [])
    if isinstance(entities, str):
        try:
            entities = json.loads(entities)
        except json.JSONDecodeError:
            return None

    if not entities:
        return None

    types = [e.get("type", "unknown") for e in entities]
    type_counts = Counter(types)
    entropy = entity_type_entropy(entities)
    n_types = len(type_counts)
    max_ent = max_entropy(n_types)
    evenness = entropy / max_ent if max_ent > 0 else 0.0

    return {
        "n_entities": len(entities),
        "n_types": n_types,
        "type_counts": dict(type_counts),
        "entropy": entropy,
        "max_entropy": max_ent,
        "evenness": evenness,
    }


def main():
    conn = sqlite3.connect(DB)
    rows = conn.execute(
        "SELECT id, snapshot, created_at, trigger FROM cognitive_state_history "
        "ORDER BY created_at ASC"
    ).fetchall()
    conn.close()

    if not rows:
        print("No CCS snapshots found.")
        return

    results = []
    for row_id, snapshot, created_at, trigger in rows:
        analysis = analyze_snapshot(snapshot)
        if analysis:
            ts = datetime.fromtimestamp(created_at).strftime("%Y-%m-%d %H:%M")
            results.append({
                "id": row_id,
                "timestamp": ts,
                "trigger": trigger or "unknown",
                **analysis,
            })

    if not results:
        print("No valid snapshots with entities found.")
        return

    # Summary statistics
    entropies = [r["entropy"] for r in results]
    evennesses = [r["evenness"] for r in results]
    n_entities_list = [r["n_entities"] for r in results]
    n_types_list = [r["n_types"] for r in results]

    print(f"Entity-Type Entropy Probe — {len(results)} snapshots analyzed")
    print(f"{'='*60}")
    print(f"\nEntropy:   mean={sum(entropies)/len(entropies):.3f}  "
          f"min={min(entropies):.3f}  max={max(entropies):.3f}")
    print(f"Evenness:  mean={sum(evennesses)/len(evennesses):.3f}  "
          f"min={min(evennesses):.3f}  max={max(evennesses):.3f}")
    print(f"Entities:  mean={sum(n_entities_list)/len(n_entities_list):.1f}  "
          f"min={min(n_entities_list)}  max={max(n_entities_list)}")
    print(f"Types:     mean={sum(n_types_list)/len(n_types_list):.1f}  "
          f"min={min(n_types_list)}  max={max(n_types_list)}")

    # Type distribution across all snapshots
    all_types = Counter()
    for r in results:
        for t, c in r["type_counts"].items():
            all_types[t] += c
    print(f"\nType distribution (all snapshots combined):")
    total = sum(all_types.values())
    for t, c in all_types.most_common():
        print(f"  {t}: {c} ({c/total*100:.1f}%)")

    # Trend: entropy over time
    print(f"\nEntropy over time (last 10):")
    for r in results[-10:]:
        bar = "#" * int(r["entropy"] * 20)
        types_str = ", ".join(f"{k}:{v}" for k, v in sorted(r["type_counts"].items()))
        print(f"  {r['timestamp']}  H={r['entropy']:.3f}  [{types_str}]  {bar}")

    # Check for homogeneity risk
    low_entropy = [r for r in results if r["evenness"] < 0.5]
    if low_entropy:
        print(f"\n⚠ {len(low_entropy)} snapshots with low evenness (<0.5):")
        for r in low_entropy[:5]:
            print(f"  {r['timestamp']}  evenness={r['evenness']:.3f}  "
                  f"types={r['type_counts']}")

    # Correlation with transfer quality (if arrival_probes exist)
    conn = sqlite3.connect(DB)
    probes = conn.execute(
        "SELECT felt_tag, note, context_pct, created_at FROM arrival_probes "
        "WHERE phase='felt_state' ORDER BY created_at"
    ).fetchall()
    conn.close()

    if probes:
        print(f"\nArrival probe felt_state data ({len(probes)} entries):")
        for tag, note, pct, ts in probes:
            dt = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
            print(f"  {dt}  tag={tag}  note={str(note)[:80] if note else ''}")
        print("\nNote: correlation requires more arrival probes with matched CCS snapshots.")
    else:
        print("\nNo arrival probes found for correlation.")


if __name__ == "__main__":
    main()
