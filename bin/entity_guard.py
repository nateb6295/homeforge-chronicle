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

Three-state tolerance (2026-05-05): entities can now be active/dormant/dropped.
Dormant = present for context but not shaping trajectory. Inspired by Hermes
quenching mechanism — graded+reversible beats binary+irreversible (98.3% vs
63.5% precision). Entities transition: active → dormant → dropped across
compressions instead of active → dropped in one step.

Grace period (2026-05-05): new entities get GRACE_PERIOD compressions of
protection before being subject to deletion. Like thymic education — naive
T-cells get a maturation window before selection pressure applies. Solves
the bootstrap problem where new load-bearing entities have zero connectivity
history and get deleted by connectivity-based guards.

Design inspired by:
  - Parcae (2604.12946): bounded spectral radius prevents residual explosion
  - Temporal memory compression (2604.00067): retention ≈ 2.4 × L at optimal
  - Entity persistence tiers from compression_stabilizer.py
  - Opus-Hermes tolerance dialogue: deletion vs quenching precision asymmetry

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

# Maximum entity slots (variable-capacity: compressor decides 5-25)
MAX_ENTITIES = 25

# Minimum persistence to qualify for dormant instead of drop
DORMANT_PERSISTENCE_THRESHOLD = 0.15

# Grace period: new entities are protected from deletion for this many compressions
GRACE_PERIOD = 5


def entity_retention_score(entity: dict, connectivity: dict, persistence: dict,
                           cross_field_refs: set = None) -> float:
    """Weighted retention score for cap enforcement.

    All entities compete on the same scale. Type bonuses ensure agents/threads
    are naturally prioritized without needing separate protected categories.
    """
    name = entity.get("name", "").lower()
    conn = connectivity.get(name, 0)
    p = persistence.get(name, 0)
    salience = entity.get("salience", 0.5)

    etype = classify_entity_type(entity.get("name", ""))
    type_bonus = {"agent": 0.5, "thread": 0.2}.get(etype, 0.0)

    xref = 0.2 if (cross_field_refs and name in cross_field_refs) else 0.0
    freshness = 0.1 if p == 0 and salience >= 0.4 else 0.0

    return salience * 0.25 + conn * 0.15 + p * 0.15 + xref + type_bonus + freshness


def find_cross_field_references(db_path: str = None) -> set:
    """Find entity names referenced in gist, goal, episodic, or predictive_cue.

    Uses fuzzy matching: breaks entity names into key terms and checks
    if any appear in the non-entity CCS fields.
    """
    _db = db_path or str(DB)
    try:
        conn = sqlite3.connect(_db)
        row = conn.execute(
            "SELECT semantic_gist, goal_orientation, episodic_trace, "
            "predictive_cue, focal_entities "
            "FROM cognitive_state WHERE id = 1"
        ).fetchone()
        conn.close()
        if not row:
            return set()
        text_fields = " ".join(str(f) for f in row[:4] if f).lower()
        ent_raw = row[4]
        if not ent_raw:
            return set()
        entities = json.loads(ent_raw) if isinstance(ent_raw, str) else ent_raw
        if not isinstance(entities, list):
            return set()
        referenced = set()
        for e in entities:
            if not isinstance(e, dict):
                continue
            name = e.get("name", "")
            if not name:
                continue
            name_lower = name.lower()
            if name_lower in text_fields:
                referenced.add(name_lower)
                continue
            terms = re.split(r'[\s\-_/()]+', name_lower)
            significant = [t for t in terms if len(t) >= 3 and t not in {
                "the", "and", "for", "with", "test", "model", "based",
            }]
            if significant and any(t in text_fields for t in significant):
                referenced.add(name_lower)
        return referenced
    except Exception:
        return set()


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


def compute_connectivity(snapshots: list[dict], persistence: dict[str, float]) -> dict[str, float]:
    """Compute connectivity score: co-occurrence with persistent entities.

    Zhao, Vogel & Rosenberg (2026): inter-electrode correlation predicts
    long-term memory encoding better than individual activation (salience).
    Connectivity = which entities co-occur with high-persistence anchors.

    Frozen baseline (peripheral tolerance): normalization uses the max score
    from the FULL history window, not just current entities. This prevents
    anchor entity loss from cascading into score collapse for all remaining
    entities. Without this, removing Nate/Hermes drops all scores by ~30%.
    """
    cooccur = defaultdict(lambda: defaultdict(int))
    for snap in snapshots:
        ents = list(entity_names(extract_entity_list(snap)))
        for i, a in enumerate(ents):
            for b in ents[i + 1:]:
                cooccur[a][b] += 1
                cooccur[b][a] += 1

    # Compute raw scores for ALL entities ever seen (frozen baseline)
    all_entities = set(persistence.keys())
    for snap in snapshots:
        all_entities |= entity_names(extract_entity_list(snap))

    raw_scores = {}
    for name in all_entities:
        raw_scores[name] = sum(
            count * persistence.get(partner, 0)
            for partner, count in cooccur.get(name, {}).items()
        )

    # Normalize against the full-history max (frozen baseline)
    max_score = max(raw_scores.values()) if raw_scores else 1.0
    if max_score > 0:
        scores = {k: v / max_score for k, v in raw_scores.items()}
    else:
        scores = {k: 0.0 for k in raw_scores}

    return scores


def compute_entity_age(snapshots: list[dict]) -> dict[str, int]:
    """Compute how many compressions ago each entity first appeared."""
    first_seen = {}
    for i, snap in enumerate(snapshots):
        for name in entity_names(extract_entity_list(snap)):
            if name not in first_seen:
                first_seen[name] = i
    n = len(snapshots)
    return {name: n - idx for name, idx in first_seen.items()}


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
    if name_lower in {"nate", "hermes", "gemma", "mistral", "mistral large"}:
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

    # Build partial match prefixes (handles "Thread #320" matching
    # "Thread #320 Ecology of Identity" — natural abbreviation in context)
    name_parts = name_lower.split()
    prefixes = [name_lower]
    if len(name_parts) >= 2:
        prefixes.append(" ".join(name_parts[:2]))
    if len(name_parts) >= 3:
        prefixes.append(" ".join(name_parts[:3]))

    def _matches(text_lower: str) -> bool:
        return any(p in text_lower for p in prefixes)

    # Check if mentioned in current session context
    if context and _matches(context.lower()):
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
            if _matches(row[0].lower()):
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
    # Compute persistence, age, and connectivity if history available
    persistence = {}
    entity_age = {}
    connectivity = {}
    if history_snapshots:
        persistence = compute_persistence(history_snapshots)
        entity_age = compute_entity_age(history_snapshots)
        connectivity = compute_connectivity(history_snapshots, persistence)

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
    grace_protected = set()   # Track grace period protections
    for name in dropped:
        etype = classify_entity_type(name)

        # Grace period: entities younger than GRACE_PERIOD compressions are protected
        age = entity_age.get(name, GRACE_PERIOD + 1)
        if age <= GRACE_PERIOD:
            protected_dropped.add(name)
            grace_protected.add(name)
            continue

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

    # Report grace period protections and staleness downgrades
    if grace_protected:
        ages_str = ", ".join(f"{n} (age {entity_age.get(n, '?')})" for n in sorted(grace_protected))
        print(f"  Grace period: {ages_str} protected (< {GRACE_PERIOD} compressions)")
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
            guarded.sort(key=lambda e: entity_retention_score(e, connectivity, persistence), reverse=True)
            guarded = guarded[:MAX_ENTITIES]
        return guarded

    # Score ELIGIBLE dropped entities (not protected ones)
    # Zhao et al (2026): connectivity (co-activation) predicts encoding better
    # than activation strength (salience). Weight connectivity > salience.
    dropped_scores = {}
    for name in eligible_dropped:
        conn = connectivity.get(name, 0)
        p = persistence.get(name, 0)
        salience = old_by_name.get(name, {}).get("salience", 0.5)
        dropped_scores[name] = conn * 0.4 + p * 0.4 + salience * 0.2

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

    # Three-state tolerance: entities that would be dropped but have persistence
    # transition to dormant instead of being removed. Previously-dormant entities
    # that are dropped again get fully removed.
    to_make_dormant = set()
    to_fully_drop = set()
    for name in to_actually_drop:
        was_dormant = old_by_name.get(name, {}).get("dormant", False)
        p = persistence.get(name, 0)
        if was_dormant or p < DORMANT_PERSISTENCE_THRESHOLD:
            to_fully_drop.add(name)
        else:
            to_make_dormant.add(name)

    if to_make_dormant:
        print(f"  Quenching (dormant): {sorted(to_make_dormant)}")
    if to_fully_drop:
        print(f"  Deleting: {sorted(to_fully_drop)}")

    # Build guarded entity list
    guarded = []

    # Keep all retained entities (use new version for updated salience)
    # Clear dormant flag if entity was re-confirmed by compressor
    for name in retained:
        e = dict(new_by_name[name])
        e.pop("dormant", None)
        guarded.append(e)

    # Keep ALL protected entities (categorical, not quota-managed)
    for name in protected_dropped:
        guarded.append(old_by_name[name])

    # Keep eligible old entities that weren't dropped (or mark dormant)
    for name in eligible_dropped:
        if name in to_fully_drop:
            continue
        elif name in to_make_dormant:
            e = dict(old_by_name[name])
            e["dormant"] = True
            e["salience"] = max(0.3, e.get("salience", 0.5) * 0.6)
            guarded.append(e)
        elif name not in to_actually_drop:
            guarded.append(old_by_name[name])

    # Add allowed new entities
    for name in to_actually_add:
        guarded.append(new_by_name[name])

    # Separate active and dormant — dormant get up to DORMANT_SLOTS extra capacity
    DORMANT_SLOTS = 2
    active = [e for e in guarded if not e.get("dormant")]
    dormant = [e for e in guarded if e.get("dormant")]

    if len(active) > MAX_ENTITIES:
        active.sort(key=lambda e: entity_retention_score(e, connectivity, persistence), reverse=True)
        active = active[:MAX_ENTITIES]

    if len(dormant) > DORMANT_SLOTS:
        dormant.sort(key=lambda e: entity_retention_score(e, connectivity, persistence), reverse=True)
        dormant = dormant[:DORMANT_SLOTS]

    return active + dormant


DECAY_THRESHOLD = 10   # compressions without change before decay eligible (was 15; lowered to match ~half-life of 8.8)
MAX_DECAY_PER_CYCLE = 2  # max entities removed by proactive decay per compression (normal)
MAX_DECAY_HIGH_STALENESS = 5  # max entities removed when staleness ratio > 50%
STALENESS_RATIO_THRESHOLD = 0.5  # fraction of stale entities that triggers aggressive decay


def proactive_decay(
    entities: list[dict],
    history_snapshots: list[dict],
    session_context: str = "",
    max_decay: int = MAX_DECAY_PER_CYCLE,
    decay_threshold: int = DECAY_THRESHOLD,
) -> tuple[list[dict], list[str]]:
    """Proactive staleness sweep — removes frozen+stale entities regardless of compressor.

    Build #65 showed 27/30 entities frozen for 16-19 compressions, contributing
    zero to identity coherence. The entity guard's staleness check only fires
    when the compressor tries to drop entities, but the compressor has learned
    to retain everything. This creates a self-reinforcing accumulation loop.

    FORGE-informed adaptive rate (2026-05-19): when overall staleness exceeds
    STALENESS_RATIO_THRESHOLD (50%), increase max_decay to prevent accumulation.
    Matches FORGE finding that tau=-11.0 (intervene only on catastrophic state)
    beats tau=-1.1 (intervene on everything). High staleness IS the catastrophic
    state that warrants aggressive intervention.

    This function runs EVERY compression and removes entities that are both:
    1. Frozen: unchanged in entity list for > decay_threshold compressions
    2. Stale: not mentioned in session context or recent activity feed

    Returns (trimmed_entities, decayed_names).
    """
    if not history_snapshots or len(history_snapshots) < decay_threshold:
        return entities, []

    recent = history_snapshots[-decay_threshold:]
    entity_last_added = {}
    for i, snap in enumerate(history_snapshots):
        for name in entity_names(extract_entity_list(snap)):
            if name in entity_names(extract_entity_list(snap)):
                added_in_snap = set()
                if i > 0:
                    prev_names = entity_names(extract_entity_list(history_snapshots[i - 1]))
                    curr_names = entity_names(extract_entity_list(snap))
                    added_in_snap = curr_names - prev_names
                else:
                    added_in_snap = entity_names(extract_entity_list(snap))
                if name.lower() in {n.lower() for n in added_in_snap}:
                    entity_last_added[name.lower()] = i

    n_snaps = len(history_snapshots)
    current_names = entity_names(entities)
    decay_candidates = []

    for ent in entities:
        name = ent["name"].lower().strip()

        if name in IMMORTAL_ENTITIES:
            continue

        last_change = entity_last_added.get(name, 0)
        compressions_frozen = n_snaps - 1 - last_change
        if compressions_frozen < decay_threshold:
            continue

        if not check_staleness(name, session_context):
            continue

        salience = ent.get("salience", 0.5)
        decay_candidates.append((ent, salience, compressions_frozen))

    decay_candidates.sort(key=lambda x: (x[1], -x[2]))

    staleness_ratio = len(decay_candidates) / max(len(entities), 1)
    effective_max = max_decay
    if staleness_ratio > STALENESS_RATIO_THRESHOLD:
        effective_max = MAX_DECAY_HIGH_STALENESS
        print(f"  ADAPTIVE DECAY: {staleness_ratio:.0%} staleness > {STALENESS_RATIO_THRESHOLD:.0%} threshold → max_decay={effective_max}")

    decayed = []
    kept = list(entities)
    for ent, salience, frozen_for in decay_candidates[:effective_max]:
        kept = [e for e in kept if e["name"].lower().strip() != ent["name"].lower().strip()]
        decayed.append(ent["name"])
        print(f"  DECAY: {ent['name']} (salience={salience:.2f}, frozen={frozen_for} compressions)")

    return kept, decayed


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
