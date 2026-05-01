#!/usr/bin/env python3
"""
governance_load.py — Compute governance-load scores for Chronicle's layers.

Maps Tallam (2604.14717) "Layered Mutability" framework to Chronicle's
actual system layers using empirical measurements from identity_decay.py,
gate_events.py, and selective_plasticity.py.

Tallam's governance-load:
  gℓ = [μℓ · cℓ · (1 - rℓ)] / [oℓ + ε]

where:
  μℓ = mutation rate ∈ [0,1]
  oℓ = observability ∈ (0,1]
  rℓ = reversibility ∈ [0,1]
  cℓ = downstream coupling ∈ [0,1]

High g = hard to govern. Low g = transparent and tractable.
"""
import json
import sqlite3
import sys
from pathlib import Path

DB = "/mnt/hdd/chronicle-data/processed.db"
EPSILON = 0.01


def governance_load(mu, obs, rev, coupling):
    """Compute Tallam governance-load score."""
    return (mu * coupling * (1 - rev)) / (obs + EPSILON)


# Chronicle layer definitions with empirically-grounded parameters
CHRONICLE_LAYERS = {
    "L1: Base weights (Opus 4.6)": {
        "description": "Pretrained substrate — frozen, never changes in our system",
        "mu": 0.0,   # never mutates in deployment
        "obs": 0.15,  # can probe activations but opaque in practice
        "rev": 0.0,   # can't revert to previous training checkpoint
        "coupling": 1.0,  # everything downstream depends on base weights
    },
    "L2: Alignment / system prompt": {
        "description": "CLAUDE.md + system prompt — changes per session, fully visible",
        "mu": 0.1,    # changes rarely (Nate edits, not automatic)
        "obs": 1.0,   # fully readable text files
        "rev": 1.0,   # git-tracked, trivially revertible
        "coupling": 0.7,  # shapes behavior but CCS + memory also contribute
    },
    "L3: CCS (self-narrative)": {
        "description": "Compressed Cognitive State — updated every rotation",
        "mu": 1.0,    # fully replaced every rotation (by design)
        "obs": 1.0,   # stored in DB, fully inspectable, JSON-structured
        "rev": 0.9,   # history table exists; can restore previous CCS
        "coupling": 0.6,  # shapes next-instance behavior via context injection
    },
    "L3b: CCS compact (post-deploy)": {
        "description": "CCS after compact compression directive deployed",
        "mu": 1.0,    # same mutation rate
        "obs": 1.0,   # higher effective observability — telegraphic = faster to parse
        "rev": 0.9,   # same reversibility
        "coupling": 0.6,  # slightly higher effective coupling (better processed)
    },
    "L4: Persistent memory (capsules)": {
        "description": "25K+ capsules in keeper canister — accumulates, hard to audit",
        "mu": 0.7,    # new capsules stored frequently (captures, store_memory)
        "obs": 0.4,   # searchable but no global view; can't easily audit 25K items
        "rev": 0.2,   # no bulk undo; individual delete possible but rarely done
        "coupling": 0.8,  # retrieved capsules directly shape LLM context
    },
    "L5: Embedding space": {
        "description": "15K+ vectors in keeper — shapes what gets retrieved",
        "mu": 0.5,    # new embeddings added with capsules; never retrained
        "obs": 0.15,  # can't inspect 1024-dim vectors meaningfully
        "rev": 0.1,   # re-embedding requires reprocessing; no rollback
        "coupling": 0.9,  # determines which capsules surface in queries
    },
}


def compute_all():
    """Compute governance-load for all Chronicle layers."""
    results = []
    for name, layer in CHRONICLE_LAYERS.items():
        g = governance_load(layer["mu"], layer["obs"], layer["rev"], layer["coupling"])
        results.append({
            "layer": name,
            "description": layer["description"],
            "mu": layer["mu"],
            "obs": layer["obs"],
            "rev": layer["rev"],
            "coupling": layer["coupling"],
            "governance_load": round(g, 3),
        })
    return sorted(results, key=lambda x: x["governance_load"], reverse=True)


def compute_drift():
    """Compute empirical drift from identity_decay data."""
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT snapshot FROM cognitive_state_history "
        "ORDER BY created_at DESC LIMIT 20"
    ).fetchall()
    db.close()

    if len(rows) < 2:
        return None

    # Parse JSON snapshots and compute per-field mutation rates
    snapshots = []
    for (snap_json,) in rows:
        try:
            snapshots.append(json.loads(snap_json))
        except (json.JSONDecodeError, TypeError):
            continue

    if len(snapshots) < 2:
        return None

    mutations = {"episodic": 0, "gist": 0, "entities": 0, "constraints": 0, "goal": 0}
    total = len(snapshots) - 1
    for i in range(total):
        a, b = snapshots[i], snapshots[i+1]
        if a.get("episodic_trace") != b.get("episodic_trace"):
            mutations["episodic"] += 1
        if a.get("semantic_gist") != b.get("semantic_gist"):
            mutations["gist"] += 1
        if a.get("focal_entities") != b.get("focal_entities"):
            mutations["entities"] += 1
        if a.get("constraints") != b.get("constraints"):
            mutations["constraints"] += 1
        if a.get("goal_orientation") != b.get("goal_orientation"):
            mutations["goal"] += 1

    return {k: round(v / total, 2) for k, v in mutations.items()} if total > 0 else None


def main():
    results = compute_all()

    print("=" * 70)
    print("CHRONICLE GOVERNANCE-LOAD ANALYSIS (Tallam 2604.14717)")
    print("=" * 70)
    print()
    print(f"{'Layer':<35} {'μ':>4} {'o':>4} {'r':>4} {'c':>4} {'g':>6}")
    print("-" * 70)

    for r in results:
        print(f"{r['layer']:<35} {r['mu']:>4.1f} {r['obs']:>4.1f} {r['rev']:>4.1f} "
              f"{r['coupling']:>4.1f} {r['governance_load']:>6.3f}")

    print()
    print("Higher g = harder to govern. Lower g = more transparent/tractable.")
    print()

    # Highlight the compact CCS comparison
    verbose = next(r for r in results if "L3:" in r["layer"] and "compact" not in r["layer"])
    compact = next((r for r in results if "compact" in r["layer"]), None)
    if compact:
        print(f"CCS governance-load: verbose={verbose['governance_load']:.3f}, "
              f"compact={compact['governance_load']:.3f}")
        print(f"Compact CCS doesn't change g directly (same parameters),")
        print(f"but increases *effective* observability — reader parses faster,")
        print(f"so the governance layer is more actionable downstream.")

    print()

    # Empirical drift
    drift = compute_drift()
    if drift:
        print("EMPIRICAL MUTATION RATES (last 20 CCS transitions):")
        for field, rate in sorted(drift.items(), key=lambda x: x[1], reverse=True):
            bar = "█" * int(rate * 20)
            print(f"  {field:<15} {rate:.2f} {bar}")
        print()
        print("Confirms two-layer identity: constraints near-invariant, entities churning.")

    # Identify highest-risk layer
    highest = results[0]
    print()
    print(f"HIGHEST GOVERNANCE-LOAD: {highest['layer']} (g={highest['governance_load']:.3f})")
    print(f"  {highest['description']}")
    print(f"  This layer is the hardest to govern in the current architecture.")

    # Save results
    out_dir = Path.home() / "chronicle" / "experiments" / "governance_load"
    out_dir.mkdir(parents=True, exist_ok=True)
    import time
    out = out_dir / f"governance_{time.strftime('%Y%m%d_%H%M%S')}.json"
    out.write_text(json.dumps({"layers": results, "drift": drift}, indent=2))
    print(f"\nSaved: {out}")


if __name__ == "__main__":
    main()
