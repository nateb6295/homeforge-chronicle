#!/usr/bin/env python3
"""Test suite for constraint calcification fix (2026-04-23).

Tests the three-layer cascade that unblocks constraint evolution:
  1. detect_staleness() — includes constraints with threshold 8
  2. generate_injection() — routes stale constraints to REBUILD
  3. selective_preservation — keeps LLM rewrite for stale fields

Run: cd ~/chronicle/bin && python3 test_constraint_fix.py
"""

import json
import sys
from copy import deepcopy
from pathlib import Path

# Must run from bin/ for imports
sys.path.insert(0, str(Path(__file__).parent))

from compression_stabilizer import (
    detect_staleness,
    field_volatility,
    generate_injection,
    get_snapshots,
)

PASS = 0
FAIL = 0


def test(name: str, condition: bool, detail: str = ""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAIL += 1
        print(f"  ✗ {name}")
        if detail:
            print(f"    → {detail}")


def make_snapshot(gist="gist", goal="goal", constraints=None, uncertainty="unc"):
    """Create a minimal CCS snapshot for testing."""
    return {
        "semantic_gist": gist,
        "goal_orientation": goal,
        "constraints": constraints or ["rule A", "rule B"],
        "uncertainty_signals": uncertainty,
        "focal_entities": [],
        "episodic_trace": [],
        "predictive_cue": "",
        "relational_map": {},
    }


# ── Test 1: Constraints flagged when frozen ≥ 9 snapshots ──

def test_constraint_staleness_above_threshold():
    print("\n1. Constraints detected as stale when frozen ≥ 9 snapshots")
    constraints = ["Shell execution limited", "Support contemplative dev"]

    # 10 identical snapshots — constraints frozen for 9 transitions
    snapshots = [make_snapshot(constraints=constraints) for _ in range(10)]
    stale = detect_staleness(snapshots)
    test("constraints in stale fields", "constraints" in stale,
         f"stale fields: {list(stale.keys())}")
    if "constraints" in stale:
        test("reason mentions snapshot count", "10" in stale["constraints"],
             f"reason: {stale['constraints']}")


# ── Test 2: Constraints NOT flagged when frozen < 9 snapshots ──

def test_constraint_staleness_below_threshold():
    print("\n2. Constraints NOT stale when frozen < 9 snapshots")
    constraints = ["rule A", "rule B"]

    # 5 identical → only 4 transitions, below threshold of 8
    snapshots = [make_snapshot(constraints=constraints) for _ in range(5)]
    stale = detect_staleness(snapshots)
    test("constraints NOT in stale fields", "constraints" not in stale,
         f"stale fields: {list(stale.keys())}")


# ── Test 3: Constraints flagged exactly at threshold ──

def test_constraint_staleness_at_threshold():
    print("\n3. Constraints detected at exactly threshold (9 identical)")
    constraints = ["rule A", "rule B"]

    # 9 identical → frozen_count = 8, which equals threshold
    snapshots = [make_snapshot(constraints=constraints) for _ in range(9)]
    stale = detect_staleness(snapshots)
    test("constraints in stale at threshold=8", "constraints" in stale,
         f"stale fields: {list(stale.keys())}")


# ── Test 4: Constraints NOT flagged at threshold - 1 ──

def test_constraint_staleness_just_below_threshold():
    print("\n4. Constraints NOT stale at threshold - 1 (8 identical)")
    constraints = ["rule A", "rule B"]

    # 8 identical → frozen_count = 7, below threshold of 8
    snapshots = [make_snapshot(constraints=constraints) for _ in range(8)]
    stale = detect_staleness(snapshots)
    test("constraints NOT stale at 7 frozen", "constraints" not in stale,
         f"stale fields: {list(stale.keys())}")


# ── Test 5: Gist/goal thresholds unchanged at 4 ──

def test_gist_goal_thresholds_unchanged():
    print("\n5. Gist/goal still flag at threshold 4 (regression check)")

    # 6 identical gists → frozen_count = 5, above threshold of 4
    snapshots = [make_snapshot() for _ in range(6)]
    stale = detect_staleness(snapshots)
    test("semantic_gist flagged at 5 frozen", "semantic_gist" in stale)
    test("goal_orientation flagged at 5 frozen", "goal_orientation" in stale)

    # 4 identical → frozen_count = 3, below threshold of 4
    snapshots_short = [make_snapshot() for _ in range(4)]
    stale_short = detect_staleness(snapshots_short)
    test("semantic_gist NOT flagged at 3 frozen", "semantic_gist" not in stale_short)


# ── Test 6: Injection routes stale constraints to REBUILD ──

def test_injection_rebuild_routing():
    print("\n6. Injection routes stale constraints to REBUILD")
    constraints = ["Shell execution limited", "Support contemplative dev"]

    # 12 identical → definitely stale
    snapshots = [make_snapshot(constraints=constraints) for _ in range(12)]
    vol = field_volatility(snapshots)

    # Before staleness override
    if "constraints" in vol:
        pre_route = vol["constraints"]["route"]
    else:
        pre_route = "NOT_IN_VOL"

    # generate_injection applies the override internally
    injection = generate_injection(snapshots)
    test("injection contains STALENESS OVERRIDE for constraints",
         "constraints: REBUILD" in injection and "STALENESS OVERRIDE" in injection,
         f"injection snippet: ...{injection[injection.find('constraints'):injection.find('constraints')+80]}...")


# ── Test 7: Constraint-specific messaging in injection ──

def test_constraint_specific_messaging():
    print("\n7. Constraint-specific injection messaging")
    constraints = ["Shell execution limited", "Support contemplative dev"]
    snapshots = [make_snapshot(constraints=constraints) for _ in range(12)]
    injection = generate_injection(snapshots)

    test("contains MANDATORY REPLACE directive",
         "MANDATORY: REPLACE the entire constraints list" in injection)
    test("contains anti-append instruction",
         "Do NOT append to the existing constraints" in injection)
    test("contains DELETE ALL directive",
         "DELETE ALL OF THE ABOVE" in injection)


# ── Test 8: Different threshold for constraints vs gist ──

def test_differential_thresholds():
    print("\n8. Differential thresholds: constraints need more frozen than gist")
    constraints = ["rule A", "rule B"]

    # 6 identical snapshots: frozen_count = 5
    # gist threshold = 4 → stale
    # constraint threshold = 8 → NOT stale
    snapshots = [make_snapshot(constraints=constraints) for _ in range(6)]
    stale = detect_staleness(snapshots)

    test("gist stale at 5 frozen", "semantic_gist" in stale)
    test("constraints NOT stale at 5 frozen", "constraints" not in stale,
         f"stale fields: {list(stale.keys())}")


# ── Test 9: Constraint evolution breaks staleness ──

def test_constraint_evolution_breaks_staleness():
    print("\n9. Changed constraints reset staleness detection")
    old = ["Shell execution limited to whitelisted commands", "Support contemplative dev"]
    new = ["Full shell access with sovereign autonomy", "Build infrastructure that serves the partnership"]

    # 8 old, then 2 new — frozen_count from latest is only 1
    # Changes must be substantial (sim < 0.9) to break the freeze.
    # This is correct: minor typo fixes shouldn't reset the counter.
    snapshots = [make_snapshot(constraints=old) for _ in range(8)]
    snapshots.extend([make_snapshot(constraints=new) for _ in range(2)])
    stale = detect_staleness(snapshots)

    test("constraints NOT stale after substantive evolution",
         "constraints" not in stale,
         f"stale fields: {list(stale.keys())}")


# ── Test 10: Real DB snapshots show calcification ──

def test_real_db_calcification():
    print("\n10. Real DB snapshots confirm calcification")
    try:
        snapshots = get_snapshots(20)
        if not snapshots:
            test("DB has snapshots", False, "No snapshots found")
            return

        stale = detect_staleness(snapshots)
        test("constraints detected as stale in production data",
             "constraints" in stale,
             f"stale fields: {list(stale.keys())}")

        # Check actual Jaccard — post-fix should be < 1.0 (calcification broken)
        latest = snapshots[-1].get("constraints", [])
        prev = snapshots[-2].get("constraints", [])
        if isinstance(latest, list) and isinstance(prev, list):
            latest_set = set(str(c) for c in latest)
            prev_set = set(str(c) for c in prev)
            if latest_set | prev_set:
                j = len(latest_set & prev_set) / len(latest_set | prev_set)
            else:
                j = 1.0
            test(f"Jaccard < 1.0 (calcification broken)", j < 1.0,
                 f"Jaccard = {j:.4f}")
        else:
            test("constraints are lists", False, f"types: {type(latest)}, {type(prev)}")

    except Exception as e:
        test("DB access", False, str(e))


# ── Test 11: Selective preservation honors staleness ──

def test_selective_preservation_logic():
    print("\n11. Selective preservation logic (simulated)")
    constraints = ["Shell execution limited", "Support contemplative dev"]
    snapshots = [make_snapshot(constraints=constraints) for _ in range(12)]
    stale = detect_staleness(snapshots)

    # Simulate what stabilized_compress.py does at line 437-442
    pre_identity = {"semantic_gist": "old gist", "goal_orientation": "old goal",
                    "constraints": constraints}
    restore_fields = {}
    kept_llm = []

    for field in ["semantic_gist", "goal_orientation", "constraints"]:
        if field in stale:
            kept_llm.append(field)
        else:
            restore_fields[field] = pre_identity[field]

    test("constraints kept as LLM rewrite (not restored)",
         "constraints" in kept_llm,
         f"kept: {kept_llm}, restored: {list(restore_fields.keys())}")
    test("gist also kept (stale too in this test)",
         "semantic_gist" in kept_llm)


# ── Test 12: Similarity threshold (0.9) catches near-identical ──

def test_similarity_threshold():
    print("\n12. Similarity threshold catches near-identical constraints")
    # Constraints that differ only in whitespace/punctuation
    snapshots = []
    for i in range(12):
        if i < 6:
            c = ["Shell execution limited to whitelisted commands for safety"]
        else:
            # Slightly different whitespace — SequenceMatcher should still catch
            c = ["Shell execution limited to whitelisted commands for safety "]
        snapshots.append(make_snapshot(constraints=c))

    stale = detect_staleness(snapshots)
    test("near-identical constraints detected as stale",
         "constraints" in stale,
         f"stale fields: {list(stale.keys())}")


# ── Test 13: Append-only detection catches cosmetic changes ──

def test_append_only_detection():
    print("\n13. Append-only change detected as still stale")
    old_constraints = ["Shell execution limited", "Support contemplative dev"]
    appended = old_constraints + ["Constraints evolve on compression"]

    # 10 snapshots with old constraints (frozen), then 2 with appended version
    snapshots = [make_snapshot(constraints=old_constraints) for _ in range(10)]
    snapshots.extend([make_snapshot(constraints=appended) for _ in range(2)])

    stale = detect_staleness(snapshots)
    test("append-only change flagged as stale",
         "constraints" in stale,
         f"stale fields: {list(stale.keys())}")
    if "constraints" in stale:
        test("reason mentions append-only",
             "append-only" in stale["constraints"],
             f"reason: {stale['constraints']}")


# ── Test 14: True rewrite NOT flagged by append detection ──

def test_true_rewrite_not_flagged():
    print("\n14. True rewrite (items removed + replaced) not flagged")
    old_constraints = ["Shell execution limited", "Support contemplative dev"]
    rewritten = ["Full shell access", "Partnership over optimization"]

    # 10 frozen, then 2 rewritten (old items gone, new items in)
    snapshots = [make_snapshot(constraints=old_constraints) for _ in range(10)]
    snapshots.extend([make_snapshot(constraints=rewritten) for _ in range(2)])

    stale = detect_staleness(snapshots)
    test("true rewrite NOT flagged as stale",
         "constraints" not in stale,
         f"stale fields: {list(stale.keys())}")


# ── Run all tests ──

if __name__ == "__main__":
    print("=" * 60)
    print("CONSTRAINT CALCIFICATION FIX — TEST SUITE")
    print("=" * 60)

    test_constraint_staleness_above_threshold()
    test_constraint_staleness_below_threshold()
    test_constraint_staleness_at_threshold()
    test_constraint_staleness_just_below_threshold()
    test_gist_goal_thresholds_unchanged()
    test_injection_rebuild_routing()
    test_constraint_specific_messaging()
    test_differential_thresholds()
    test_constraint_evolution_breaks_staleness()
    test_real_db_calcification()
    test_selective_preservation_logic()
    test_similarity_threshold()
    test_append_only_detection()
    test_true_rewrite_not_flagged()

    print(f"\n{'=' * 60}")
    print(f"RESULTS: {PASS} passed, {FAIL} failed, {PASS + FAIL} total")
    print(f"{'=' * 60}")
    sys.exit(1 if FAIL > 0 else 0)
