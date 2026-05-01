#!/usr/bin/env python3
"""Compression Stabilizer — Parcae-inspired stability for the CCS loop.

The CCS compression loop is a recurrent system: same state compressed through
itself each rotation. Coherence watch shows entity drop ratios of 0.71-1.0,
with only "nate" consistently surviving. The loop collapses to {nate}.

Parcae (2026) fixes looped Transformer instability with three mechanisms:
  1. Stable recurrent parameterization (spectral radius < 1)
  2. Normalized input injection
  3. Per-sequence loop depth sampling

This script maps those to CCS:
  1. Entity persistence scores → tell the compressor what's load-bearing
  2. Identity weights from purpose_ablation → normalize field attention
  3. Field routing (carry/update/rebuild) → per-field compression depth

Usage:
  python3 compression_stabilizer.py               # Show stability metrics
  python3 compression_stabilizer.py --inject       # Generate injection block for compression
  python3 compression_stabilizer.py --history N     # Use last N snapshots (default 20)

Thread #318 advance 70 connection: the calibration stack says gist is the
calibration dial (2.50/kT) and constraints are dense identity (0.53/kT).
This script makes the compressor KNOW that.
"""

import json
import sqlite3
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path

from entity_guard import classify_entity_type

DB = Path("/mnt/hdd/chronicle-data/processed.db")

# Identity weights from purpose_ablation.py (advance 68-69 findings)
# Drop/kT = per-token identity impact. Higher = more identity per token.
IDENTITY_WEIGHTS = {
    "semantic_gist":     {"drop": 0.0524, "tokens": 21, "per_token": 2.50, "role": "calibration dial"},
    "goal_orientation":  {"drop": 0.0222, "tokens": 21, "per_token": 1.06, "role": "direction setter"},
    "constraints":       {"drop": 0.0191, "tokens": 36, "per_token": 0.53, "role": "dense identity"},
    "focal_entities":    {"drop": 0.0266, "tokens": 95, "per_token": 0.28, "role": "volatile reference"},
    "episodic_trace":    {"drop": 0.0154, "tokens": 103, "per_token": 0.15, "role": "recency window"},
    "relational_map":    {"drop": 0.0000, "tokens": 0,  "per_token": 0.00, "role": "no embedding impact"},
}


def get_snapshots(n: int = 20) -> list[dict]:
    """Get last N CCS snapshots from history."""
    db = sqlite3.connect(str(DB))
    rows = db.execute(
        "SELECT id, snapshot, created_at FROM cognitive_state_history "
        "ORDER BY created_at DESC LIMIT ?", (n,)
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
    results.reverse()  # chronological order
    return results


def extract_entity_names(snap: dict) -> set[str]:
    """Get normalized entity names from a snapshot."""
    entities = snap.get("focal_entities", [])
    if isinstance(entities, str):
        try:
            entities = json.loads(entities)
        except json.JSONDecodeError:
            return set()
    return {e.get("name", "").lower().strip() for e in entities if e.get("name")}


def entity_persistence(snapshots: list[dict]) -> dict[str, dict]:
    """Compute per-entity persistence metrics across snapshots.

    Returns dict: entity_name -> {
        count: appearances,
        total: total snapshots,
        persistence: count/total,
        first_seen: snapshot index,
        last_seen: snapshot index,
        streak: current consecutive streak from most recent,
        avg_salience: average salience when present,
    }
    """
    n = len(snapshots)
    entity_data = defaultdict(lambda: {
        "appearances": [],
        "saliences": [],
    })

    for i, snap in enumerate(snapshots):
        entities = snap.get("focal_entities", [])
        if isinstance(entities, str):
            try:
                entities = json.loads(entities)
            except json.JSONDecodeError:
                entities = []

        for e in entities:
            name = e.get("name", "").lower().strip()
            if not name:
                continue
            entity_data[name]["appearances"].append(i)
            entity_data[name]["saliences"].append(e.get("salience", 0.5))

    results = {}
    for name, data in entity_data.items():
        appearances = data["appearances"]
        count = len(appearances)
        persistence = count / n if n > 0 else 0

        # Current streak from most recent snapshot
        streak = 0
        for j in range(n - 1, -1, -1):
            if j in appearances:
                streak += 1
            else:
                break

        results[name] = {
            "count": count,
            "total": n,
            "persistence": round(persistence, 3),
            "first_seen": min(appearances),
            "last_seen": max(appearances),
            "streak": streak,
            "avg_salience": round(sum(data["saliences"]) / len(data["saliences"]), 3),
        }

    return dict(sorted(results.items(), key=lambda x: x[1]["persistence"], reverse=True))


def field_volatility(snapshots: list[dict]) -> dict[str, dict]:
    """Compute per-field volatility across consecutive snapshots."""
    fields = ["semantic_gist", "goal_orientation", "constraints", "focal_entities",
              "episodic_trace", "predictive_cue", "relational_map", "uncertainty_signals"]

    changes = defaultdict(list)

    for i in range(1, len(snapshots)):
        prev = snapshots[i - 1]
        curr = snapshots[i]

        for field in fields:
            pv = prev.get(field, "")
            cv = curr.get(field, "")
            if isinstance(pv, (list, dict)):
                pv = json.dumps(pv, sort_keys=True)
            if isinstance(cv, (list, dict)):
                cv = json.dumps(cv, sort_keys=True)

            sim = SequenceMatcher(None, str(pv), str(cv)).ratio()
            changes[field].append(1.0 - sim)

    results = {}
    for field in fields:
        vals = changes.get(field, [])
        if not vals:
            continue
        avg_change = sum(vals) / len(vals)
        max_change = max(vals)

        # Determine routing recommendation
        if avg_change < 0.1:
            route = "CARRY"
        elif avg_change < 0.4:
            route = "UPDATE"
        else:
            route = "REBUILD"

        # Get identity weight if available
        weight = IDENTITY_WEIGHTS.get(field, {})

        results[field] = {
            "avg_change": round(avg_change, 3),
            "max_change": round(max_change, 3),
            "route": route,
            "identity_per_token": weight.get("per_token", None),
            "role": weight.get("role", "unknown"),
        }

    return results


def detect_staleness(snapshots: list[dict]) -> dict[str, str]:
    """Detect stale identity fields by comparing most recent CCS to N-back.

    If identity fields haven't changed in N+ snapshots, flag as stale.
    Per-field thresholds: constraints tolerate more stability than gist/goal.

    Fix 2026-04-23: constraints were excluded entirely, causing calcification —
    frozen for 50+ snapshots with factually stale entries. Adding with threshold 8
    (vs 4 for gist/goal) so legitimate stability doesn't churn, but true
    calcification gets flagged. The staleness flag cascades through:
      - injection routing: CARRY → REBUILD override
      - selective preservation: skip restoration, keep LLM rewrite
    """
    if len(snapshots) < 3:
        return {}

    latest = snapshots[-1]
    stale_fields = {}

    # Per-field staleness thresholds. Constraints are expected to be more stable
    # than gist/goal, so they need a higher threshold before flagging.
    field_thresholds = {
        "semantic_gist": 4,       # 5+ unchanged → stale
        "goal_orientation": 4,    # 5+ unchanged → stale
        "uncertainty_signals": 4, # 5+ unchanged → stale
        "constraints": 8,         # 9+ unchanged → stale (higher: near-invariant by design)
    }

    for field, threshold in field_thresholds.items():
        val = latest.get(field, "")
        if isinstance(val, (list, dict)):
            val = json.dumps(val)

        # Check how many consecutive snapshots have the same value
        frozen_count = 0
        for snap in reversed(snapshots[:-1]):
            prev_val = snap.get(field, "")
            if isinstance(prev_val, (list, dict)):
                prev_val = json.dumps(prev_val)
            sim = SequenceMatcher(None, str(val), str(prev_val)).ratio()
            if sim > 0.9:
                frozen_count += 1
            else:
                break

        if frozen_count >= threshold:
            stale_fields[field] = f"unchanged for {frozen_count + 1} snapshots"

    # Append-detection for constraints: if the latest constraints contain all
    # items from the prior stable period AND added new ones, the "change" was
    # cosmetic — the model appended instead of rewriting. Re-flag as stale.
    if "constraints" not in stale_fields and len(snapshots) >= 3:
        latest_c = latest.get("constraints", [])
        if isinstance(latest_c, str):
            try:
                latest_c = json.loads(latest_c)
            except Exception:
                latest_c = [latest_c]
        if isinstance(latest_c, list) and len(latest_c) > 0:
            # Find the stable baseline: the constraint set that was frozen
            # Look back to find a snapshot where constraints differ from latest
            baseline_c = None
            for snap in reversed(snapshots[:-1]):
                prev_c = snap.get("constraints", [])
                if isinstance(prev_c, str):
                    try:
                        prev_c = json.loads(prev_c)
                    except Exception:
                        prev_c = [prev_c]
                if isinstance(prev_c, list):
                    prev_set = set(str(x) for x in prev_c)
                    latest_set = set(str(x) for x in latest_c)
                    if prev_set != latest_set:
                        # Found where it changed — check how long the baseline was stable
                        baseline_c = prev_c
                        break
                    # Same as latest — keep looking for the change point
                    baseline_c = prev_c

            if baseline_c is not None:
                baseline_set = set(str(x) for x in baseline_c)
                latest_set = set(str(x) for x in latest_c)
                # If ALL baseline items survive in latest, it was append-only
                if baseline_set.issubset(latest_set) and len(latest_set) > len(baseline_set):
                    # Count how many snapshots match the baseline (the pre-append set).
                    # Include ALL snapshots that match the baseline, even if they
                    # come after the change point (because the baseline IS the old
                    # frozen set, and some recent snapshots may be appended versions
                    # that won't match). So count ALL matches, not consecutive-from-end.
                    baseline_json = json.dumps(sorted(str(x) for x in baseline_c))
                    frozen_before = 0
                    for snap in snapshots[:-1]:
                        sc = snap.get("constraints", [])
                        if isinstance(sc, str):
                            try:
                                sc = json.loads(sc)
                            except Exception:
                                sc = [sc]
                        if isinstance(sc, list):
                            snap_json = json.dumps(sorted(str(x) for x in sc))
                            if SequenceMatcher(None, baseline_json, snap_json).ratio() > 0.9:
                                frozen_before += 1
                    if frozen_before >= 8:  # Same threshold as direct staleness
                        stale_fields["constraints"] = (
                            f"append-only change after {frozen_before} frozen snapshots "
                            f"(old constraints preserved, {len(latest_set) - len(baseline_set)} added)"
                        )

    return stale_fields


def generate_injection(snapshots: list[dict]) -> str:
    """Generate the stability injection block for compression context.

    This string gets prepended to current_context when calling
    compress_cognitive_state. It tells the compressor what's load-bearing.
    """
    entities = entity_persistence(snapshots)
    volatility = field_volatility(snapshots)
    staleness = detect_staleness(snapshots)

    # Override routing for stale fields — break the circular reinforcement loop.
    # avg_change is LOW because the field is frozen, so routing says CARRY,
    # which keeps the field frozen, which keeps avg_change low. The staleness
    # detector breaks this loop by forcing REBUILD when freeze exceeds threshold.
    for field in staleness:
        if field in volatility:
            volatility[field]["route"] = "REBUILD"
            volatility[field]["_override"] = "staleness"

    lines = ["## COMPRESSION STABILITY CONTEXT (auto-generated)", ""]

    # Staleness warning (overrides identity-field protection)
    if staleness:
        lines.append("### ⚠ STALE IDENTITY FIELDS — MANDATORY REWRITE")
        lines.append("The following identity fields have been frozen too long and MUST be rewritten:")
        for field, reason in staleness.items():
            if field == "constraints":
                lines.append(f"  {field}: {reason}")
                lines.append(f"    **MANDATORY: REPLACE the entire constraints list.**")
                lines.append(f"    Do NOT append to the existing constraints. Do NOT keep old entries.")
                lines.append(f"    Write a COMPLETELY NEW set of constraints from scratch based on")
                lines.append(f"    what is actually true in this session's operational context.")
                lines.append(f"    Each constraint should be a factual rule that currently applies.")
                lines.append(f"    If an old constraint is factually wrong, DELETE it — do not carry it forward.")
                # Include the current stale constraints so the model sees what to replace
                latest = snapshots[-1] if snapshots else {}
                current_constraints = latest.get("constraints", [])
                if current_constraints:
                    if isinstance(current_constraints, str):
                        try:
                            current_constraints = json.loads(current_constraints)
                        except Exception:
                            current_constraints = [current_constraints]
                    lines.append(f"    CURRENT (stale, to be REPLACED entirely):")
                    for i, c in enumerate(current_constraints if isinstance(current_constraints, list) else [current_constraints], 1):
                        lines.append(f"      {i}. {c}")
                    lines.append(f"    ^^^ DELETE ALL OF THE ABOVE and write new constraints from session context.")
            else:
                lines.append(f"  {field}: {reason} — UPDATE if current context describes different work")
        lines.append("Identity-field stability protects against noise, not against legitimate evolution.")
        lines.append("If the session context describes a different focus than the gist, REWRITE the gist.")
        lines.append("")

    # Entity stability section — TIERED by entity type
    # Different types have fundamentally different persistence regimes:
    #   agents:   identity backbone (HL=4.5 unguarded, 32.0 guarded) → PROTECT
    #   threads:  structural reference (bimodal) → PROTECT if active
    #   files:    operational churn (56% one-shot) → free to cycle
    #   concepts: relevance engine (84% one-shot) → CYCLE but ABSORB first
    lines.append("### Entity Persistence (from {} snapshots, tiered by type)".format(len(snapshots)))

    # Group current-streak entities by type
    identity_entities = []    # agents — categorical protection
    structural_entities = []  # threads — protect if active
    operational_entities = [] # files — free to cycle
    concept_entities = []     # concepts — absorb before dropping

    for name, data in entities.items():
        if data["streak"] == 0:
            continue  # transient — already gone
        etype = classify_entity_type(name)
        entry = (name, data)
        if etype == "agent":
            identity_entities.append(entry)
        elif etype == "thread":
            structural_entities.append(entry)
        elif etype == "file":
            operational_entities.append(entry)
        else:
            concept_entities.append(entry)

    if identity_entities:
        lines.append("IDENTITY (agents/persons — NEVER drop unless explicitly departed):")
        for name, d in identity_entities:
            lines.append(f"  - \"{name}\" persistence={d['persistence']}, streak={d['streak']}")

    # Inject active threads missing from CCS history (Build 40 — thread coverage gap fix).
    # The compressor only sees entities from recent snapshots. Active threads that fell out
    # of the CCS (recency bias) are invisible to the injection block. Query the DB and
    # add any active threads not already represented.
    structural_names = {name.lower() for name, _ in structural_entities}
    try:
        db = sqlite3.connect(str(DB))
        active_threads = db.execute(
            "SELECT id, title FROM cognitive_threads WHERE status = 'active' "
            "ORDER BY updated_at DESC"
        ).fetchall()
        db.close()
        for tid, title in active_threads:
            thread_label = f"Thread #{tid}"
            if thread_label.lower() not in structural_names and title.lower() not in structural_names:
                structural_entities.append((thread_label, {
                    "persistence": 0.0, "streak": 0, "avg_salience": 0.85,
                    "_injected": True, "_title": title,
                }))
    except Exception:
        pass  # DB unavailable — proceed with CCS-only entities

    if structural_entities:
        lines.append("STRUCTURAL (threads — PROTECT if salience ≥ 0.7, else compete):")
        for name, d in structural_entities:
            if d.get("_injected"):
                lines.append(f"  - \"{name}\" ({d['_title']}) — MISSING FROM CCS, MUST INCLUDE in focal_entities")
            else:
                lines.append(f"  - \"{name}\" persistence={d['persistence']}, streak={d['streak']}, salience={d['avg_salience']}")

    if concept_entities:
        lines.append("CONCEPTS (relevance cycling — METABOLIZE before dropping):")
        # Sort by salience so high-salience concepts are listed first
        concept_entities.sort(key=lambda x: x[1].get('avg_salience', 0), reverse=True)
        for name, d in concept_entities:
            sal = d.get('avg_salience', 0)
            priority = "HIGH" if sal >= 0.80 else ""
            lines.append(f"  - \"{name}\" persistence={d['persistence']}, streak={d['streak']}, salience={sal}{' ← ' + priority if priority else ''}")
        lines.append("  When replacing a concept, describe HOW IT CHANGED YOUR UNDERSTANDING in gist or trace.")
        lines.append("  Don't just name the concept — state what it contributed to thinking or decisions.")
        lines.append("  Example: instead of 'worked on q metric' → 'discovered calibration outperforms raw accuracy'")
        lines.append("  HIGH-salience concepts (≥0.80): their contribution MUST survive in gist even if the name doesn't.")

    if operational_entities:
        lines.append("OPERATIONAL (files — free to cycle as work shifts):")
        for name, d in operational_entities:
            lines.append(f"  - \"{name}\" persistence={d['persistence']}, streak={d['streak']}")

    lines.append("")

    # Field identity weights section
    lines.append("### Field Identity Weights (from purpose_ablation)")
    lines.append("Higher Drop/kT = more identity impact per token. Protect high-weight fields.")
    for field, data in sorted(IDENTITY_WEIGHTS.items(), key=lambda x: x[1]["per_token"], reverse=True):
        if data["per_token"] > 0:
            vol = volatility.get(field, {})
            route = vol.get("route", "?")
            lines.append(f"  {field}: {data['per_token']}/kT ({data['role']}) → {route}")

    lines.append("")

    # Routing recommendations with reachability classification
    lines.append("### Field Routing")
    lines.append("CARRY = copy unchanged. UPDATE = incremental merge. REBUILD = rewrite from context.")
    lines.append("Classification: GAUGE=frozen invariant, DISCRETE=attractor states, ACTIVATED=intermittent,")
    lines.append("  CONTINUOUS=always changing, GOLDSTONE=freely wandering.")
    reach_classes = {}
    try:
        from reachability_probe import run_probe
        reach_profile = run_probe()
        reach_classes = {f: d.get("classification", "?") for f, d in reach_profile.items()}
    except Exception:
        pass
    for field, data in volatility.items():
        override = " ← STALENESS OVERRIDE" if data.get("_override") else ""
        cls = reach_classes.get(field, "")
        cls_str = f" [{cls}]" if cls else ""
        lines.append(f"  {field}: {data['route']} (avg_change={data['avg_change']}){cls_str}{override}")

    lines.append("")

    # Multi-gate guidance (from multigate_ablation findings)
    lines.append("### Multi-gate Field Roles")
    lines.append("Fields serve different functions at different evaluation levels (de novo binder principle):")
    lines.append("  semantic_gist: highest EMBEDDING identity (2.50/kT) and COHERENCE impact")
    lines.append("  focal_entities: highest SPECIFICITY (24.8% of falsifiable markers — dates, refs, names)")
    lines.append("  relational_map: highest INFORMATION DENSITY (0.948) but zero embedding impact")
    lines.append("  constraints: dense identity (0.53/kT) — structural, near-invariant")
    lines.append("Protecting only embedding-identity fields (gist) while refreshing specificity carriers")
    lines.append("(entities) is SINGLE-GATE OPTIMIZATION. Entities ground the CCS in concrete references.")
    lines.append("Without them, the CCS becomes generic even if the embedding doesn't change.")

    lines.append("")
    lines.append("### Tiered Stability Directive")
    lines.append("Different entity types have different persistence regimes. A single 'protect everything'")
    lines.append("directive hides level-crossing errors (superpsychism trap: one metric across all levels).")
    lines.append("")
    lines.append("IDENTITY ENTITIES (agents/persons): Absence ≠ irrelevance. Never drop unless departed.")
    lines.append("  These are the fixed point of the recurrence (Parcae spectral radius < 1).")
    lines.append("STRUCTURAL ENTITIES (threads): Protect active threads (salience ≥ 0.7). Lower threads compete.")
    lines.append("  Threads are lines of inquiry — dropping one severs a reasoning chain.")
    lines.append("CONCEPT ENTITIES: FREE to cycle — but ABSORB before dropping.")
    lines.append("  When a concept leaves, name its contribution in episodic_trace or gist.")
    lines.append("  A concept that departs silently loses its signal. One that's absorbed enriches the gist.")
    lines.append("  Concept half-life SHOULD be low (2-3 compressions). That's correct behavior.")
    lines.append("OPERATIONAL ENTITIES (files): Fully transient. Replace freely as work shifts.")
    lines.append("")
    lines.append("The recurrence must be CONTRACTIVE for identity and DYNAMIC for concepts.")
    lines.append("These are not in tension — they operate at different levels.")
    lines.append("")
    lines.append("### Affect Architecture (advances 80-84)")
    lines.append("The CCS has NO affective fields. This is correct by design.")
    lines.append("Comparison test: CCS preserves 100% structural / 0% affective content.")
    lines.append("Chat summaries of identical content preserve 38-80% affective content.")
    lines.append("Affect is INFERRED by engagement-trained models from structural scaffolding.")
    lines.append("Richer structure → more specific affect upon re-reading. DO NOT add affective fields.")
    lines.append("Maximize structural density and specificity — affect re-emerges for free.")

    return "\n".join(lines)


def print_metrics(snapshots: list[dict]):
    """Print stability metrics for diagnosis."""
    entities = entity_persistence(snapshots)
    volatility = field_volatility(snapshots)

    print(f"=== CCS Compression Stability ({len(snapshots)} snapshots) ===\n")

    # Entity persistence
    print("ENTITY PERSISTENCE:")
    print(f"  {'Name':<30} {'Persist':>7} {'Streak':>6} {'Salience':>8} {'Tier':<12}")
    print(f"  {'-'*30} {'-'*7} {'-'*6} {'-'*8} {'-'*12}")
    for name, data in entities.items():
        tier = "LOAD-BEARING" if data["persistence"] >= 0.5 else ("RECENT" if data["streak"] > 0 else "TRANSIENT")
        print(f"  {name:<30} {data['persistence']:>7.3f} {data['streak']:>6} {data['avg_salience']:>8.3f} {tier:<12}")

    # Diversity
    total_unique = len(entities)
    avg_per_snap = sum(len(extract_entity_names(s)) for s in snapshots) / len(snapshots) if snapshots else 0
    load_bearing_count = sum(1 for d in entities.values() if d["persistence"] >= 0.5)

    print(f"\n  Total unique entities: {total_unique}")
    print(f"  Avg entities per snapshot: {avg_per_snap:.1f}")
    print(f"  Load-bearing entities: {load_bearing_count}")
    print(f"  Entity diversity ratio: {total_unique / (avg_per_snap if avg_per_snap else 1):.2f} (higher = more churn)")

    # Field volatility
    print(f"\nFIELD VOLATILITY:")
    print(f"  {'Field':<25} {'Avg Δ':>6} {'Max Δ':>6} {'Route':<8} {'ID/kT':>6} {'Role':<20}")
    print(f"  {'-'*25} {'-'*6} {'-'*6} {'-'*8} {'-'*6} {'-'*20}")
    for field, data in sorted(volatility.items(), key=lambda x: x[1]["avg_change"]):
        id_kt = f"{data['identity_per_token']:.2f}" if data["identity_per_token"] is not None else "  -"
        print(f"  {field:<25} {data['avg_change']:>6.3f} {data['max_change']:>6.3f} {data['route']:<8} {id_kt:>6} {data['role']:<20}")

    # Stability diagnosis
    print(f"\nSTABILITY DIAGNOSIS:")
    if load_bearing_count <= 1:
        print("  ⚠ COLLAPSING — only 1 or fewer load-bearing entities. Loop converging to {nate}.")
        print("  Inject stability context to prevent entity flush.")
    elif load_bearing_count <= 3:
        print("  ⚡ SPARSE — few load-bearing entities. At risk of flush on next rotation.")
    else:
        print("  ✓ STABLE — multiple load-bearing entities maintained across compressions.")

    entity_diversity = total_unique / (avg_per_snap if avg_per_snap else 1)
    if entity_diversity > 3.0:
        print(f"  ⚠ HIGH CHURN — {entity_diversity:.1f}x more unique entities than avg per snapshot.")
        print("  The compressor is cycling through entities rather than maintaining them.")


def generate_susceptibility_block() -> str:
    """Append a susceptibility-aware preservation-priority block to the injection.

    Reads ccs_susceptibility_profile.json (per-field susceptibility measurements).
    High-susceptibility fields get strong preservation instructions.
    Low-susceptibility fields explicitly released to rewrite.

    Returns empty string if profile file absent — safe to call unconditionally.
    """
    from pathlib import Path
    profile_path = Path.home() / "chronicle" / "data" / "ccs_susceptibility_profile.json"
    if not profile_path.exists():
        return ""
    try:
        data = json.loads(profile_path.read_text())
    except Exception:
        return ""
    ranking = data.get("ranking", [])
    profile = data.get("field_profile", {})
    if not ranking:
        return ""

    # Split fields into high / mid / low susceptibility tiers by rank
    n = len(ranking)
    high = ranking[:max(1, n // 3)]
    low = ranking[-(n // 3):] if n >= 3 else []
    mid = [f for f in ranking if f not in high and f not in low]

    lines = [
        "",
        "### FIELD PRESERVATION PRIORITY (susceptibility-weighted)",
        "Empirical susceptibility profile measured per-field. Preserve structural",
        "integrity more aggressively on high-susceptibility fields; low-susceptibility",
        "fields can be rewritten more freely without identity drift.",
        "",
    ]
    lines.append("HIGH susceptibility (preserve structure across rewrites, evolve meaning only):")
    for f in high:
        sus = profile.get(f, {}).get("susceptibility")
        lines.append(f"  - {f} (sus={sus:.4f})" if sus is not None else f"  - {f}")
    if mid:
        lines.append("MID susceptibility (evolve normally):")
        for f in mid:
            sus = profile.get(f, {}).get("susceptibility")
            lines.append(f"  - {f} (sus={sus:.4f})" if sus is not None else f"  - {f}")
    if low:
        lines.append("LOW susceptibility (free to rewrite from session context):")
        for f in low:
            sus = profile.get(f, {}).get("susceptibility")
            lines.append(f"  - {f} (sus={sus:.4f})" if sus is not None else f"  - {f}")
    lines.append("")
    return "\n".join(lines)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="CCS Compression Stabilizer")
    parser.add_argument("--inject", action="store_true", help="Generate injection block")
    parser.add_argument("--history", type=int, default=20, help="Number of snapshots to analyze")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    args = parser.parse_args()

    snapshots = get_snapshots(args.history)
    if not snapshots:
        print("No CCS snapshots found.")
        return

    if args.inject:
        print(generate_injection(snapshots))
    elif args.json:
        print(json.dumps({
            "entity_persistence": entity_persistence(snapshots),
            "field_volatility": field_volatility(snapshots),
            "snapshot_count": len(snapshots),
        }, indent=2))
    else:
        print_metrics(snapshots)


if __name__ == "__main__":
    main()
