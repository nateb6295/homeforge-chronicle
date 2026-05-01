#!/usr/bin/env python3
"""Entity Guard — post-compression entity replacement quota enforcement.

The CCS compression loop has an empirical entity half-life of 1.9 compressions
(8.8x worse than the temporal compression paper's prediction of 16.8).

Root cause: the compressor either keeps all entities (0% replacement) or does
a major swap (30-50% replacement). No middle ground. When context shifts, the
entire entity set gets replaced — losing accumulated persistence.

The fix: enforce a MAX REPLACEMENT QUOTA per compression. Never swap more than
MAX_REPLACE entities in a single compression. This creates a sliding window
effect where entities are phased out gradually rather than flushed.

Design inspired by:
  - Parcae (2604.12946): bounded spectral radius prevents residual explosion
  - Temporal memory compression (2604.00067): retention ≈ 2.4 × L at optimal
  - Entity persistence tiers from compression_stabilizer.py

Usage:
  # As a library (called by stabilized_compress.py after compression):
  from entity_guard import enforce_quota
  guarded = enforce_quota(old_entities, new_entities, history_snapshots)

  # As a standalone diagnostic:
  python3 entity_guard.py                # Show what guard would do on current CCS
  python3 entity_guard.py --simulate     # Retroactive simulation over history
"""

import json
import re
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

DB = Path("/mnt/hdd/chronicle-data/processed.db")

# Maximum entities that can be replaced in a single compression
MAX_REPLACE = 2

# Minimum persistence (fraction of history) to be considered "load-bearing"
LOAD_BEARING_THRESHOLD = 0.3

# Maximum entity slots (from CCS schema: 7±2)
MAX_ENTITIES = 7


def extract_entity_list(snap_or_field) -> list[dict]:
    """Extract entity list from a snapshot or raw field value."""
    if isinstance(snap_or_field, dict):
        entities = snap_or_field.get("focal_entities", [])
    else:
        entities = snap_or_field

    if isinstance(entities, str):
        try:
            entities = json.loads(entities)
        except json.JSONDecodeError:
            return []

    if not isinstance(entities, list):
        return []

    return [e for e in entities if isinstance(e, dict) and e.get("name")]


def entity_names(entities: list[dict]) -> set[str]:
    """Get normalized names from entity list."""
    return {e["name"].lower().strip() for e in entities}


def entity_by_name(entities: list[dict]) -> dict[str, dict]:
    """Index entities by normalized name."""
    result = {}
    for e in entities:
        name = e["name"].lower().strip()
        result[name] = e
    return result


def compute_persistence(snapshots: list[dict]) -> dict[str, float]:
    """Compute persistence score for each entity across history."""
    n = len(snapshots)
    if n == 0:
        return {}

    counts = defaultdict(int)
    for snap in snapshots:
        for name in entity_names(extract_entity_list(snap)):
            counts[name] += 1

    return {name: count / n for name, count in counts.items()}


# Entity type bonus: agents/persons and threads persist 10x more than concepts
# empirically. The guard should protect them proportionally.
ENTITY_TYPE_BONUS = {
    "agent": 0.3,    # nate, hermes, gemma — avg persistence 0.337
    "thread": 0.15,  # thread #318 — structural reference
    "file": 0.0,     # operational, expected to churn
    "concept": 0.0,  # the churn engine
}


def classify_entity_type(name: str) -> str:
    """Classify entity by naming pattern."""
    name_lower = name.lower().strip()
    if re.search(r'\.(py|rs|js|md|sh|toml|json)$', name_lower):
        return "file"
    if name_lower in {"nate", "hermes", "gemma"}:
        return "agent"
    if "thread" in name_lower:
        return "thread"
    return "concept"


def check_staleness(entity_name: str, context: str = "", window: int = 100) -> bool:
    """Check if an entity is stale — not mentioned in recent context.

    Goertzel (2026-04-20): monotone reinforcement without anti-resonance
    collapses the system to a habit trap. Entities that persist without
    fresh context support should lose categorical protection.

    Args:
        entity_name: Entity name to check
        context: Current session context (passed to compressor)
        window: Number of recent activity_feed rows to check

    Returns:
        True if stale (not found in context or recent activity)
    """
    name_lower = entity_name.lower().strip()

    # Check if mentioned in current session context
    if context and name_lower in context.lower():
        return False

    # Check recent activity feed
    try:
        db = sqlite3.connect(str(DB))
        rows = db.execute(
            "SELECT content FROM activity_feed ORDER BY created_at DESC LIMIT ?",
            (window,)
        ).fetchall()
        db.close()

        for row in rows:
            if name_lower in row[0].lower():
                return False
    except Exception:
        # If we can't check, assume not stale (conservative)
        return False

    return True


# Anti-resonance parameters (Goertzel 2026: η=0.4 optimal for autocatalytic sets)
# Names that are NEVER subject to staleness (true identity core)
IMMORTAL_ENTITIES = {"nate"}  # Nate is always present by definition


def enforce_quota(
    old_entities: list[dict],
    new_entities: list[dict],
    history_snapshots: list[dict] = None,
    max_replace: int = MAX_REPLACE,
    session_context: str = "",
) -> list[dict]:
    """Enforce entity replacement quota on compression output.

    Args:
        old_entities: Entity list from CCS before compression
        new_entities: Entity list from LLM compression output
        history_snapshots: Optional CCS history for persistence scoring
        max_replace: Maximum entities to replace per compression
        session_context: Current session context for staleness checking

    Returns:
        Guarded entity list — respects replacement quota while allowing evolution
    """
    old_names = entity_names(old_entities)
    new_names = entity_names(new_entities)
    old_by_name = entity_by_name(old_entities)
    new_by_name = entity_by_name(new_entities)

    retained = old_names & new_names
    dropped = old_names - new_names
    added = new_names - old_names

    # If replacement is within quota, accept as-is
    if len(dropped) <= max_replace:
        return new_entities

    # Over quota — need to merge
    # Compute persistence if history available
    persistence = {}
    if history_snapshots:
        persistence = compute_persistence(history_snapshots)

    # === TIERED GUARD (level-appropriate evaluation) ===
    # Insight from superpsychism critique: evaluating across levels with a single
    # metric is the unification trap. Different entity types have different
    # persistence regimes:
    #   agents: identity backbone (avg run 6.7, 20% one-shot) → categorically protected
    #   threads: structural reference (bimodal) → protected if active
    #   files: operational churn (56% one-shot) → free to cycle
    #   concepts: relevance engine (84% one-shot) → quota-managed
    #
    # The old type bonus (additive score inflation) double-counted persistence.
    # Tiered guard instead PARTITIONS the replacement pool: protected entities
    # never compete for quota slots.

    protected_dropped = set()
    eligible_dropped = set()
    stale_downgrades = set()  # Track staleness-caused downgrades
    for name in dropped:
        etype = classify_entity_type(name)

        # Anti-resonance: stale entities lose categorical protection
        # (Goertzel: overused pathways must become harder to maintain tail productivity)
        is_stale = name not in IMMORTAL_ENTITIES and check_staleness(name, session_context)

        if etype == "agent":
            # Agents are identity — protect unless persistence is near zero OR stale
            p = persistence.get(name, 0)
            if p >= 0.1 and not is_stale:
                protected_dropped.add(name)
            else:
                eligible_dropped.add(name)
                if is_stale:
                    stale_downgrades.add(name)
        elif etype == "thread":
            # Active threads are structural — protect if salience > 0.7 AND not stale
            salience = old_by_name.get(name, {}).get("salience", 0.5)
            if salience >= 0.7 and not is_stale:
                protected_dropped.add(name)
            else:
                eligible_dropped.add(name)
                if is_stale:
                    stale_downgrades.add(name)
        else:
            # Files and concepts compete for quota slots
            eligible_dropped.add(name)

    # Report staleness downgrades
    if stale_downgrades:
        print(f"  Anti-resonance: {sorted(stale_downgrades)} downgraded from protected → eligible (stale)")

    # If eligible drops are within quota even after protecting, accept
    if len(eligible_dropped) <= max_replace:
        # All eligible drops are fine, just protect the protected ones
        # Rebuild: new entities + protected old entities
        guarded = []
        for name in new_names:
            guarded.append(new_by_name[name])
        for name in protected_dropped:
            guarded.append(old_by_name[name])
        if len(guarded) > MAX_ENTITIES:
            guarded.sort(key=lambda e: e.get("salience", 0.5), reverse=True)
            guarded = guarded[:MAX_ENTITIES]
        return guarded

    # Score ELIGIBLE dropped entities (not protected ones)
    dropped_scores = {}
    for name in eligible_dropped:
        p = persistence.get(name, 0)
        salience = old_by_name.get(name, {}).get("salience", 0.5)
        dropped_scores[name] = p * 0.7 + salience * 0.3

    # Score added entities by how relevant they are
    added_scores = {}
    for name in added:
        salience = new_by_name.get(name, {}).get("salience", 0.5)
        p = persistence.get(name, 0)
        added_scores[name] = salience * 0.7 + p * 0.3

    # Sort: drop the LEAST important eligible entities, add the MOST important new
    dropped_sorted = sorted(dropped_scores.items(), key=lambda x: x[1])
    added_sorted = sorted(added_scores.items(), key=lambda x: -x[1])

    # Allow up to max_replace actual replacements (from eligible pool only)
    to_actually_drop = {name for name, _ in dropped_sorted[:max_replace]}
    to_actually_add = {name for name, _ in added_sorted[:max_replace]}

    # Build guarded entity list
    guarded = []

    # Keep all retained entities (use new version for updated salience)
    for name in retained:
        guarded.append(new_by_name[name])

    # Keep ALL protected entities (categorical, not quota-managed)
    for name in protected_dropped:
        guarded.append(old_by_name[name])

    # Keep eligible old entities that weren't dropped
    for name in eligible_dropped:
        if name not in to_actually_drop:
            guarded.append(old_by_name[name])

    # Add allowed new entities
    for name in to_actually_add:
        guarded.append(new_by_name[name])

    # Trim to MAX_ENTITIES if over (protected entities still have priority via salience)
    if len(guarded) > MAX_ENTITIES:
        guarded.sort(key=lambda e: e.get("salience", 0.5), reverse=True)
        guarded = guarded[:MAX_ENTITIES]

    return guarded


def get_snapshots(n: int = 20) -> list[dict]:
    """Get last N CCS snapshots."""
    db = sqlite3.connect(str(DB))
    rows = db.execute(
        "SELECT snapshot FROM cognitive_state_history ORDER BY created_at DESC LIMIT ?", (n,)
    ).fetchall()
    db.close()
    results = []
    for row in rows:
        try:
            results.append(json.loads(row[0]))
        except json.JSONDecodeError:
            continue
    results.reverse()
    return results


def simulate_retroactive(snapshots: list[dict], max_replace: int = MAX_REPLACE):
    """Simulate guard effect retroactively over history."""
    print(f"=== Entity Guard Retroactive Simulation (max_replace={max_replace}) ===")
    print(f"Snapshots: {len(snapshots)}\n")

    # Track entity runs WITH guard applied
    guarded_entity_sets = []
    interventions = 0
    entities_saved = 0

    for i, snap in enumerate(snapshots):
        new_entities = extract_entity_list(snap)

        if i == 0:
            guarded_entity_sets.append(new_entities)
            continue

        old_entities = guarded_entity_sets[-1]
        old_names = entity_names(old_entities)
        new_names = entity_names(new_entities)

        dropped = old_names - new_names
        added = new_names - old_names

        if len(dropped) > max_replace:
            # Guard would intervene
            history = snapshots[:i]
            guarded = enforce_quota(old_entities, new_entities, history, max_replace)
            guarded_entity_sets.append(guarded)
            interventions += 1
            entities_saved += len(dropped) - max_replace
        else:
            guarded_entity_sets.append(new_entities)

    print(f"Interventions: {interventions} ({interventions/(len(snapshots)-1)*100:.0f}% of transitions)")
    print(f"Entities saved from premature drop: {entities_saved}")

    # Compute guarded survival curve
    entity_runs = defaultdict(list)
    current_runs = {}

    for entities in guarded_entity_sets:
        present = entity_names(entities)
        for e in present:
            current_runs[e] = current_runs.get(e, 0) + 1
        to_close = [e for e in current_runs if e not in present]
        for e in to_close:
            entity_runs[e].append(current_runs[e])
            del current_runs[e]

    for e, length in current_runs.items():
        entity_runs[e].append(length)

    all_runs = []
    for runs in entity_runs.values():
        all_runs.extend(runs)

    if not all_runs:
        print("No entity runs to analyze.")
        return

    max_len = max(all_runs)
    survival = {}
    for k in range(1, max_len + 1):
        surviving = sum(1 for r in all_runs if r >= k)
        survival[k] = surviving / len(all_runs)

    # Half-life
    half_life = None
    for k in sorted(survival.keys()):
        if survival[k] < 0.5:
            if k > 1:
                prev = survival.get(k - 1, 1.0)
                half_life = (k - 1) + (prev - 0.5) / (prev - survival[k])
            else:
                half_life = float(k)
            break
    if not half_life:
        half_life = float(max_len)

    print(f"\nGUARDED HALF-LIFE: {half_life:.1f} compressions (actual: 1.9, predicted: 16.8)")
    print(f"Improvement: {half_life/1.9:.1f}x over unguarded")
    print(f"Gap to theory: {16.8/half_life:.1f}x")

    print(f"\nGuarded survival curve:")
    for k in range(1, min(12, max_len + 1)):
        p = survival.get(k, 0)
        bar = "█" * int(p * 40)
        print(f"  k={k:2d}: {p:.3f} {bar}")

    # Compare: entity diversity
    total_unique = len(entity_runs)
    avg_per_snap = sum(len(entity_names(e)) for e in guarded_entity_sets) / len(guarded_entity_sets)
    print(f"\nEntity diversity: {total_unique} unique, {avg_per_snap:.1f} avg/snap, churn {total_unique/avg_per_snap:.1f}x")


def show_current():
    """Show what guard would do on the current CCS transition."""
    db = sqlite3.connect(str(DB))

    # Get current CCS entities
    row = db.execute("SELECT focal_entities FROM cognitive_state WHERE id = 1").fetchone()
    if not row:
        print("No current CCS found.")
        return
    current = extract_entity_list(row[0] if isinstance(row[0], str) else row[0])
    current_names = entity_names(current)

    # Get most recent history snapshot
    hist_row = db.execute(
        "SELECT snapshot FROM cognitive_state_history ORDER BY created_at DESC LIMIT 1"
    ).fetchone()
    db.close()

    if not hist_row:
        print("No history snapshots.")
        return

    prev = extract_entity_list(json.loads(hist_row[0]))
    prev_names = entity_names(prev)

    dropped = prev_names - current_names
    added = current_names - prev_names
    retained = prev_names & current_names

    # Check staleness for current entities
    stale = []
    fresh = []
    for name in sorted(current_names):
        if name in IMMORTAL_ENTITIES:
            fresh.append(f"{name} (immortal)")
        elif check_staleness(name):
            stale.append(name)
        else:
            fresh.append(name)

    print(f"CURRENT TRANSITION:")
    print(f"  Previous: {sorted(prev_names)}")
    print(f"  Current:  {sorted(current_names)}")
    print(f"  Retained: {sorted(retained)}")
    print(f"  Dropped:  {sorted(dropped)}")
    print(f"  Added:    {sorted(added)}")
    print(f"  Replacements: {len(dropped)} (quota: {MAX_REPLACE})")
    print(f"\n  STALENESS CHECK:")
    print(f"  Fresh: {fresh}")
    if stale:
        print(f"  Stale (would lose protection): {stale}")
    else:
        print(f"  No stale entities")

    if len(dropped) > MAX_REPLACE:
        print(f"\n  ⚠ OVER QUOTA by {len(dropped) - MAX_REPLACE}")
        snapshots = get_snapshots(20)
        guarded = enforce_quota(prev, current, snapshots)
        guarded_names = entity_names(guarded)
        print(f"  Guard would produce: {sorted(guarded_names)}")
        print(f"  Entities saved: {sorted(prev_names & guarded_names - current_names)}")
    else:
        print(f"\n  ✓ Within quota")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="CCS Entity Guard")
    parser.add_argument("--simulate", action="store_true",
                        help="Retroactive simulation over history")
    parser.add_argument("--max-replace", type=int, default=MAX_REPLACE,
                        help=f"Max entity replacements per compression (default {MAX_REPLACE})")
    parser.add_argument("--history", type=int, default=50,
                        help="Number of snapshots for simulation")
    args = parser.parse_args()

    if args.simulate:
        snapshots = get_snapshots(args.history)
        simulate_retroactive(snapshots, args.max_replace)
    else:
        show_current()


if __name__ == "__main__":
    main()
