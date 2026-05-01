#!/usr/bin/env python3
"""
morphospace_probe.py — Computes Perrier & Bennett (2603.09043) identity
morphospace coordinates from CCS history.

Five operational metrics:
  1. Identifiability — distance from a reference identity (first stable snapshot)
  2. Continuity — smoothness of state changes (1 - mean |jaccard_diff|)
  3. Consistency — variance of entity sets under same conditions (proxy: entity
     jaccard across recent window)
  4. Persistence (P_weak) — proportion of identity ingredients that appear
     somewhere in a window
  5. Recovery — mean entity Jaccard after compression events vs before

Three morphospace axes:
  Coherence (Coh) = α·Consistency + (1-α)·Identifiability
  Availability (Avail) = P_weak
  Binding (Bind) = P_strong (how often full conjunction co-instantiates)

Usage:
  python3 morphospace_probe.py
  python3 morphospace_probe.py --window 5 --alpha 0.5
"""

import argparse
import json
import sqlite3
import sys
from pathlib import Path

DB = Path("/mnt/hdd/chronicle-data/processed.db")


def get_snapshots(limit=100):
    """Get CCS history snapshots."""
    db = sqlite3.connect(str(DB))
    rows = db.execute(
        "SELECT id, snapshot, created_at FROM cognitive_state_history "
        "ORDER BY created_at ASC LIMIT ?",
        (limit,)
    ).fetchall()
    db.close()
    result = []
    for row in rows:
        try:
            snap = json.loads(row[1])
            result.append({"id": row[0], "data": snap, "ts": row[2]})
        except (json.JSONDecodeError, TypeError):
            continue
    return result


def extract_entity_names(snap):
    """Extract entity name set from a snapshot."""
    entities = snap.get("focal_entities", [])
    if isinstance(entities, str):
        try:
            entities = json.loads(entities)
        except (json.JSONDecodeError, TypeError):
            return set()
    return {e.get("name", "").lower().strip() for e in entities if isinstance(e, dict) and e.get("name")}


def extract_constraints(snap):
    """Extract constraint set from a snapshot."""
    constraints = snap.get("constraints", [])
    if isinstance(constraints, str):
        try:
            constraints = json.loads(constraints)
        except (json.JSONDecodeError, TypeError):
            return set()
    if isinstance(constraints, list):
        return {str(c).lower().strip() for c in constraints}
    return set()


def jaccard(a, b):
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def compute_metrics(snapshots, window=5, alpha=0.5):
    """Compute all five P/B metrics + morphospace coordinates."""
    if len(snapshots) < 3:
        print("Need at least 3 snapshots.")
        return None

    # Reference identity: use first snapshot that has entities
    ref_snap = None
    for s in snapshots:
        if extract_entity_names(s["data"]):
            ref_snap = s
            break
    if not ref_snap:
        print("No snapshot with entities found.")
        return None

    ref_entities = extract_entity_names(ref_snap["data"])
    ref_constraints = extract_constraints(ref_snap["data"])

    # 1. Identifiability — how close each snapshot is to reference
    identifiability_scores = []
    for s in snapshots:
        ent = extract_entity_names(s["data"])
        con = extract_constraints(s["data"])
        # Combined identity distance
        ent_j = jaccard(ref_entities, ent)
        con_j = jaccard(ref_constraints, con)
        identifiability_scores.append((ent_j + con_j) / 2)

    mean_identifiability = sum(identifiability_scores) / len(identifiability_scores)

    # 2. Continuity — smoothness of successive changes
    continuity_scores = []
    for i in range(1, len(snapshots)):
        prev_ent = extract_entity_names(snapshots[i-1]["data"])
        curr_ent = extract_entity_names(snapshots[i]["data"])
        prev_con = extract_constraints(snapshots[i-1]["data"])
        curr_con = extract_constraints(snapshots[i]["data"])
        ent_j = jaccard(prev_ent, curr_ent)
        con_j = jaccard(prev_con, curr_con)
        continuity_scores.append((ent_j + con_j) / 2)

    mean_continuity = sum(continuity_scores) / len(continuity_scores) if continuity_scores else 0

    # 3. Consistency — entity stability within a sliding window
    consistency_scores = []
    for i in range(window, len(snapshots)):
        window_sets = [extract_entity_names(snapshots[j]["data"]) for j in range(i-window, i)]
        # Pairwise jaccard within window
        pairs = 0
        total_j = 0
        for a in range(len(window_sets)):
            for b in range(a+1, len(window_sets)):
                total_j += jaccard(window_sets[a], window_sets[b])
                pairs += 1
        if pairs > 0:
            consistency_scores.append(total_j / pairs)

    mean_consistency = sum(consistency_scores) / len(consistency_scores) if consistency_scores else 0

    # 4. Persistence (P_weak) — proportion of reference entities appearing
    #    somewhere in each window
    p_weak_scores = []
    for i in range(window, len(snapshots)):
        window_union = set()
        for j in range(i-window, i):
            window_union |= extract_entity_names(snapshots[j]["data"])
        if ref_entities:
            p_weak_scores.append(len(ref_entities & window_union) / len(ref_entities))
        else:
            p_weak_scores.append(0)

    mean_p_weak = sum(p_weak_scores) / len(p_weak_scores) if p_weak_scores else 0

    # 5. P_strong — proportion of windows where ALL reference entities co-instantiate
    p_strong_scores = []
    for i in range(window, len(snapshots)):
        # Check if any single snapshot in window has ALL reference entities
        found_all = False
        for j in range(i-window, i):
            snap_ent = extract_entity_names(snapshots[j]["data"])
            if ref_entities <= snap_ent:  # subset check
                found_all = True
                break
        p_strong_scores.append(1.0 if found_all else 0.0)

    mean_p_strong = sum(p_strong_scores) / len(p_strong_scores) if p_strong_scores else 0

    # 6. Recovery — not computed without explicit perturbation events
    #    Proxy: jaccard immediately after drops of >2 entities
    recovery_scores = []
    for i in range(2, len(snapshots)):
        prev = extract_entity_names(snapshots[i-1]["data"])
        curr = extract_entity_names(snapshots[i]["data"])
        dropped = len(prev - curr)
        if dropped >= 2:
            # Check recovery in next snapshot
            if i + 1 < len(snapshots):
                next_ent = extract_entity_names(snapshots[i+1]["data"])
                restored = len((prev - curr) & next_ent)
                recovery_scores.append(restored / dropped if dropped > 0 else 0)

    mean_recovery = sum(recovery_scores) / len(recovery_scores) if recovery_scores else 0

    # Morphospace coordinates
    coh = alpha * mean_consistency + (1 - alpha) * mean_identifiability
    avail = mean_p_weak
    bind = mean_p_strong
    temporal_gap = (avail + 1) / (bind + 1) if bind > 0 else float('inf')

    return {
        "identifiability": mean_identifiability,
        "continuity": mean_continuity,
        "consistency": mean_consistency,
        "p_weak": mean_p_weak,
        "p_strong": mean_p_strong,
        "recovery": mean_recovery,
        "coherence": coh,
        "availability": avail,
        "binding": bind,
        "temporal_gap_ratio": temporal_gap,
        "n_snapshots": len(snapshots),
        "ref_entities": sorted(ref_entities),
    }


def main():
    parser = argparse.ArgumentParser(description="Perrier/Bennett Identity Morphospace")
    parser.add_argument("--window", type=int, default=5, help="Sliding window size")
    parser.add_argument("--alpha", type=float, default=0.5, help="Coherence weighting (consistency vs identifiability)")
    parser.add_argument("--limit", type=int, default=100, help="Max snapshots to analyze")
    args = parser.parse_args()

    snapshots = get_snapshots(args.limit)
    print(f"Loaded {len(snapshots)} CCS snapshots")

    metrics = compute_metrics(snapshots, window=args.window, alpha=args.alpha)
    if not metrics:
        return

    print(f"\n{'='*55}")
    print(f"  Perrier/Bennett Identity Morphospace (n={metrics['n_snapshots']})")
    print(f"{'='*55}")
    print(f"  Reference entities: {metrics['ref_entities']}")
    print(f"\n  Five Operational Metrics:")
    print(f"    Identifiability:  {metrics['identifiability']:.3f}")
    print(f"    Continuity:       {metrics['continuity']:.3f}")
    print(f"    Consistency:      {metrics['consistency']:.3f}")
    print(f"    P_weak (occur):   {metrics['p_weak']:.3f}")
    print(f"    P_strong (coinst):{metrics['p_strong']:.3f}")
    print(f"    Recovery:         {metrics['recovery']:.3f}")
    print(f"\n  Morphospace Coordinates:")
    print(f"    Coherence (Coh):  {metrics['coherence']:.3f}")
    print(f"    Availability:     {metrics['availability']:.3f}")
    print(f"    Binding:          {metrics['binding']:.3f}")
    print(f"    Temporal gap:     {metrics['temporal_gap_ratio']:.2f}")

    # Interpret position in P/B Table 1
    print(f"\n  P/B Architecture Classification:")
    if metrics['coherence'] > 0.7 and metrics['availability'] > 0.7 and metrics['binding'] > 0.5:
        print(f"    → Stateful controller LMA (High/High/High)")
    elif metrics['availability'] > 0.6 and metrics['binding'] < 0.4:
        print(f"    → Memory LMA (Medium/High/Medium)")
    elif metrics['coherence'] > 0.4 and metrics['availability'] > 0.3:
        print(f"    → RAG LMA (Medium/Medium/Medium)")
    else:
        print(f"    → Prompted LMA or lower")

    # The temporal gap analysis
    gap = metrics['temporal_gap_ratio']
    if gap < 1.5:
        print(f"    Temporal gap: NARROW ({gap:.2f}) — ingredients co-instantiate frequently")
    elif gap < 3.0:
        print(f"    Temporal gap: MODERATE ({gap:.2f}) — some ingredient-wise occurrence without co-instantiation")
    else:
        print(f"    Temporal gap: WIDE ({gap:.2f}) — identity is smeared, rarely co-instantiated")


if __name__ == "__main__":
    main()
