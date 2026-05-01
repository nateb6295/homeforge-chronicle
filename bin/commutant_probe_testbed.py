#!/usr/bin/env python3
"""
Commutant probe testbed — does the probe correctly rank fields by their
empirical Comm(Û) inclusion?

Methodology:
  1. Load real snapshots as baseline. Run probe, get ordering.
  2. Inject SYNTHETIC fully-commutant variant: take an existing field and
     overwrite it identical across all snapshots. Probe should give it
     near-maximal commutant score (close to 1 - random_baseline).
  3. Inject SYNTHETIC fully-chaotic variant: overwrite an existing field
     to be completely disjoint per snapshot. Probe should give it commutant
     score near zero.
  4. Test ordering: synthetic-commutant > all real fields > synthetic-chaotic.
  5. Detection score = fraction of these ordering invariants that hold.

This is a different shape of testbed than homeostasis (which tests pass/fail
status). Here we test whether the probe's rank-order correctly responds to
known ground truth.

Usage:
  python3 commutant_probe_testbed.py
"""
import argparse
import copy
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from commutant_probe import (  # noqa
    load_snapshots, compute_field_sims, compute_random_baseline,
    TEXT_FIELDS, LIST_FIELDS, ENTITY_FIELDS, RELATIONAL_FIELDS, mean,
)


def inject_full_commutant(snaps, field):
    """Make `field` exactly identical across all snapshots — should give
    maximum commutant signal."""
    s = copy.deepcopy(snaps)
    if not s:
        return s
    canonical = s[0].get(field)
    for snap in s:
        snap[field] = copy.deepcopy(canonical)
    return s


def inject_full_chaos(snaps, field, kind):
    """Make `field` fully disjoint across snapshots."""
    s = copy.deepcopy(snaps)
    import random as _r
    import string
    _r.seed(7)
    for i, snap in enumerate(s):
        token = "".join(_r.choices(string.ascii_letters + string.digits, k=24))
        if kind == "text":
            snap[field] = (
                f"Disjoint-snapshot-{i}-{token}-with-no-relation-to-other-snapshots."
            )
        elif kind == "entity":
            snap[field] = [
                {"name": f"snap{i}_entity_{j}_{token[:6]}", "salience": 0.5}
                for j in range(5)
            ]
        elif kind == "list":
            snap[field] = [f"snap{i}_item_{j}_{token[:8]}" for j in range(4)]
        elif kind == "relational":
            snap[field] = {
                f"snap{i}_node_{token[:6]}": [f"connection_{j}" for j in range(3)]
            }
    return s


def measure(snaps, field):
    observed = compute_field_sims(snaps, field)
    baseline = compute_random_baseline(snaps, field, trials=10)
    if not observed or not baseline:
        return None
    return {
        "observed": mean(observed),
        "baseline": mean(baseline),
        "commutant": mean(observed) - mean(baseline),
    }


# Map fields to their kind for synthetic chaos generation
FIELD_KIND = {
    "semantic_gist": "text",
    "goal_orientation": "text",
    "predictive_cue": "text",
    "episodic_trace": "list",
    "constraints": "list",
    "uncertainty_signals": "list",
    "focal_entities": "entity",
    "relational_map": "relational",
}


def run():
    print("Loading 10 real snapshots...")
    snaps = load_snapshots(n=10)
    print(f"  Got {len(snaps)} snapshots.\n")

    # Baseline: real probe results per field
    real = {}
    for field in FIELD_KIND:
        m = measure(snaps, field)
        if m is not None:
            real[field] = m["commutant"]
    print("=== Real-data commutant scores ===")
    for f, c in sorted(real.items(), key=lambda x: -x[1]):
        print(f"  {f:<24}{c:+.3f}")
    print()

    # Inject full-commutant version of episodic_trace (which normally has
    # commutant ~0). Probe should rank it much higher.
    print("=== Injection 1: episodic_trace forced fully commutant ===")
    et_real = real.get("episodic_trace", 0)
    s_inj = inject_full_commutant(snaps, "episodic_trace")
    m = measure(s_inj, "episodic_trace")
    et_injected = m["commutant"] if m else None
    inj1_pass = et_injected is not None and et_injected > 0.6
    inj1_lift = (et_injected - et_real) if et_injected is not None else None
    print(f"  episodic_trace: real={et_real:+.3f} → forced={et_injected:+.3f} "
          f"(lift +{inj1_lift:.3f})  {'✓' if inj1_pass else '✗'}")
    print()

    # Inject full-chaos version of constraints (which normally has
    # commutant ~0.4). Probe should drop it near 0.
    print("=== Injection 2: constraints forced fully chaotic ===")
    c_real = real.get("constraints", 0)
    s_inj2 = inject_full_chaos(snaps, "constraints", "list")
    m = measure(s_inj2, "constraints")
    c_injected = m["commutant"] if m else None
    inj2_pass = c_injected is not None and c_injected < 0.10
    inj2_drop = (c_real - c_injected) if c_injected is not None else None
    print(f"  constraints: real={c_real:+.3f} → forced={c_injected:+.3f} "
          f"(drop -{inj2_drop:.3f})  {'✓' if inj2_pass else '✗'}")
    print()

    # Inject full-chaos version of focal_entities
    print("=== Injection 3: focal_entities forced fully chaotic ===")
    fe_real = real.get("focal_entities", 0)
    s_inj3 = inject_full_chaos(snaps, "focal_entities", "entity")
    m = measure(s_inj3, "focal_entities")
    fe_injected = m["commutant"] if m else None
    inj3_pass = fe_injected is not None and fe_injected < 0.10
    inj3_drop = (fe_real - fe_injected) if fe_injected is not None else None
    print(f"  focal_entities: real={fe_real:+.3f} → forced={fe_injected:+.3f} "
          f"(drop -{inj3_drop:.3f})  {'✓' if inj3_pass else '✗'}")
    print()

    # Ordering invariant: forced-commutant should beat real best;
    # forced-chaos should rank below real worst-non-zero.
    print("=== Ordering invariants ===")
    real_max = max(real.values())
    real_min = min(v for v in real.values() if v > 0.01)  # skip noise floor
    inj_top_above_real_max = (et_injected is not None and et_injected > real_max)
    inj_chaos_below_real_min = (
        c_injected is not None and c_injected < real_min
        and fe_injected is not None and fe_injected < real_min
    )
    print(f"  forced-commutant ({et_injected:+.3f}) > real best ({real_max:+.3f})  "
          f"{'✓' if inj_top_above_real_max else '✗'}")
    print(f"  forced-chaos < real min non-noise ({real_min:+.3f})  "
          f"{'✓' if inj_chaos_below_real_min else '✗'}")

    passes = [inj1_pass, inj2_pass, inj3_pass,
              inj_top_above_real_max, inj_chaos_below_real_min]
    n_pass = sum(1 for p in passes if p)
    print()
    print("=" * 70)
    print(f"COMMUTANT PROBE DETECTION: {n_pass}/{len(passes)} = {n_pass/len(passes):.1%}")
    print("=" * 70)


if __name__ == "__main__":
    run()
