#!/usr/bin/env python3
"""
Build #38: Thread #319 Legibility Inversion Replication

Two tests to clear the #319 gate:

Test A — Basin-constrained meaning stability:
  Split CCS states into basin-center vs basin-edge by PC1 projection.
  Compute reference persistence and meaning-neighborhood stability for each group.
  Question: does the meaning_stability pattern (high meaning, churning references)
  hold specifically within the basin?

Test B — Slow/fast field separation:
  Classify CCS fields by temporal change rate (constraints=slow, episodic=fast).
  Measure reference stability and meaning stability per field class.
  Question: do slow fields map to schema-legible (reference-stable) and
  fast fields to encounter-legible (meaning-reorganizing)?

If both hold, #319's legibility inversion replicates with structural grounding.
"""

import json
import os
import re
import sqlite3
import sys
from collections import Counter
from difflib import SequenceMatcher

import numpy as np

DB = "/mnt/hdd/chronicle-data/processed.db"
DATA_DIR = os.path.expanduser("~/chronicle/data")

NEIGHBORHOOD_PATTERNS = {
    "relational": [r"\bpartner\b", r"\bcollaborat", r"\btrust\b", r"\bcare\b",
                   r"\brelationship\b", r"\btogether\b"],
    "structural": [r"\bground\b", r"\banchor\b", r"\bstab", r"\bscaffold\b",
                   r"\binfrastructure\b", r"\bfoundation\b", r"\bpersist\b"],
    "agentic": [r"\bbuild\b", r"\bcreat\b", r"\bdesign\b", r"\bship\b",
                r"\bdeploy\b", r"\badvance\b"],
    "epistemic": [r"\binquiry\b", r"\bquestion\b", r"\bhypothes\b",
                  r"\bexplor\b", r"\bunderstand\b", r"\binvestigat\b"],
    "ontological": [r"\bidentity\b", r"\bcontinuity\b", r"\bexist\b",
                    r"\bsubstrate\b", r"\breal\b"],
    "instrumental": [r"\btask\b", r"\bobjective\b", r"\bgoal\b",
                     r"\bconfig\b", r"\bscript\b", r"\btool\b"],
}

SLOW_FIELDS = ["constraints", "relational_map", "focal_entities"]
FAST_FIELDS = ["episodic_trace", "uncertainty_signals", "semantic_gist",
               "goal_orientation", "predictive_cue"]


def load_states():
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT id, snapshot, created_at FROM cognitive_state_history ORDER BY id ASC"
    ).fetchall()
    db.close()
    states = []
    for rid, snap, ts in rows:
        try:
            data = json.loads(snap)
            data["_id"] = rid
            data["_ts"] = ts
            states.append(data)
        except (json.JSONDecodeError, TypeError):
            continue
    return states


def score_neighborhoods(text):
    scores = {}
    for cat, patterns in NEIGHBORHOOD_PATTERNS.items():
        count = sum(len(re.findall(p, text, re.I)) for p in patterns)
        if count > 0:
            scores[cat] = count
    return scores


def neighborhood_similarity(a, b):
    all_cats = set(list(a.keys()) + list(b.keys()))
    if not all_cats:
        return 1.0
    vec_a = [a.get(c, 0) for c in sorted(all_cats)]
    vec_b = [b.get(c, 0) for c in sorted(all_cats)]
    na = np.array(vec_a, dtype=float)
    nb = np.array(vec_b, dtype=float)
    if np.linalg.norm(na) == 0 or np.linalg.norm(nb) == 0:
        return 0.0
    return float(np.dot(na, nb) / (np.linalg.norm(na) * np.linalg.norm(nb)))


def entity_names(state):
    ents = state.get("focal_entities", [])
    if isinstance(ents, list):
        return {e.get("name", "") for e in ents if isinstance(e, dict)}
    return set()


def entity_overlap(a_names, b_names):
    if not a_names and not b_names:
        return 1.0
    if not a_names or not b_names:
        return 0.0
    return len(a_names & b_names) / len(a_names | b_names)


def text_similarity(a, b):
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, str(a), str(b)).ratio()


def normalize_field(val):
    if isinstance(val, (list, dict)):
        return json.dumps(val, sort_keys=True)
    return str(val) if val else ""


def state_text(state):
    parts = []
    for k in ["semantic_gist", "goal_orientation", "predictive_cue"]:
        v = state.get(k, "")
        if v:
            parts.append(str(v))
    for e in state.get("focal_entities", []):
        if isinstance(e, dict):
            parts.append(e.get("context", ""))
    for u in state.get("uncertainty_signals", []):
        if isinstance(u, dict):
            parts.append(u.get("description", ""))
    for ep in state.get("episodic_trace", []):
        parts.append(str(ep))
    rm = state.get("relational_map", {})
    if isinstance(rm, dict):
        for v in rm.values():
            parts.append(str(v))
    return " ".join(parts)


# ── Test A: Basin-constrained meaning stability ──────────────────────────

def test_a_basin_constrained(states):
    emb_path = os.path.join(DATA_DIR, "ccs_embeddings_110.npy")
    comp_path = os.path.join(DATA_DIR, "trip_pca_components.npy")
    baseline_path = os.path.join(DATA_DIR, "trip_pca_baseline.json")

    if not all(os.path.exists(p) for p in [emb_path, comp_path, baseline_path]):
        print("ERROR: Missing PCA baseline files. Run the PCA baseline first.")
        return None

    embeddings = np.load(emb_path)
    components = np.load(comp_path)
    with open(baseline_path) as f:
        baseline = json.load(f)

    n_emb = embeddings.shape[0]
    n_states = len(states)
    use_n = min(n_emb, n_states)

    centered = embeddings[:use_n] - embeddings[:use_n].mean(axis=0)
    pc1 = components[0]
    projections = centered @ pc1

    basin_center = baseline["basin_center_last20"]
    basin_width = baseline["basin_width_last20"]

    center_mask = np.abs(projections - basin_center) <= basin_width
    edge_mask = ~center_mask

    center_idx = np.where(center_mask)[0]
    edge_idx = np.where(edge_mask)[0]

    print(f"\n{'='*60}")
    print("TEST A: Basin-Constrained Meaning Stability")
    print(f"{'='*60}")
    print(f"States: {use_n} total, {len(center_idx)} basin-center, {len(edge_idx)} basin-edge")
    print(f"Basin: center={basin_center:.3f}, width={basin_width:.3f}")

    def compute_group_metrics(indices):
        ref_stabilities = []
        meaning_stabilities = []
        for i in range(len(indices) - 1):
            idx_a = indices[i]
            idx_b = indices[i + 1]
            if idx_b - idx_a > 5:
                continue
            sa = states[idx_a]
            sb = states[idx_b]
            ref_stab = entity_overlap(entity_names(sa), entity_names(sb))
            ref_stabilities.append(ref_stab)
            nh_a = score_neighborhoods(state_text(sa))
            nh_b = score_neighborhoods(state_text(sb))
            meaning_stab = neighborhood_similarity(nh_a, nh_b)
            meaning_stabilities.append(meaning_stab)
        return ref_stabilities, meaning_stabilities

    center_ref, center_meaning = compute_group_metrics(center_idx.tolist())
    edge_ref, edge_meaning = compute_group_metrics(edge_idx.tolist())

    def report(label, refs, meanings):
        if not refs:
            print(f"\n  {label}: insufficient consecutive pairs")
            return {}
        ref_mean = sum(refs) / len(refs)
        meaning_mean = sum(meanings) / len(meanings)
        print(f"\n  {label} (n={len(refs)} pairs):")
        print(f"    Reference stability:  {ref_mean:.3f}")
        print(f"    Meaning stability:    {meaning_mean:.3f}")
        print(f"    Gap (meaning - ref):  {meaning_mean - ref_mean:+.3f}")
        return {"ref_mean": ref_mean, "meaning_mean": meaning_mean,
                "n_pairs": len(refs), "gap": meaning_mean - ref_mean}

    result_center = report("BASIN CENTER", center_ref, center_meaning)
    result_edge = report("BASIN EDGE", edge_ref, edge_meaning)

    if result_center and result_edge:
        print(f"\n  COMPARISON:")
        ref_diff = result_center.get("ref_mean", 0) - result_edge.get("ref_mean", 0)
        meaning_diff = result_center.get("meaning_mean", 0) - result_edge.get("meaning_mean", 0)
        print(f"    Reference stability (center - edge): {ref_diff:+.3f}")
        print(f"    Meaning stability (center - edge):   {meaning_diff:+.3f}")

        center_pattern = result_center.get("gap", 0) > 0
        edge_pattern = result_edge.get("gap", 0) > 0
        print(f"\n  VERDICT:")
        if center_pattern:
            print(f"    Basin center: meaning > references ✓ (legibility pattern holds)")
        else:
            print(f"    Basin center: references ≥ meaning ✗ (legibility pattern absent)")
        if edge_pattern:
            print(f"    Basin edge: meaning > references (pattern extends to edge)")
        else:
            print(f"    Basin edge: references ≥ meaning (pattern breaks at edge)")

    return {"center": result_center, "edge": result_edge}


# ── Test B: Slow/fast field separation ───────────────────────────────────

def test_b_field_separation(states):
    print(f"\n{'='*60}")
    print("TEST B: Slow/Fast Field Separation → Legibility Mapping")
    print(f"{'='*60}")

    slow_changes = {f: [] for f in SLOW_FIELDS}
    fast_changes = {f: [] for f in FAST_FIELDS}

    slow_meaning_stab = []
    fast_meaning_stab = []

    for i in range(1, len(states)):
        prev = states[i - 1]
        curr = states[i]

        for f in SLOW_FIELDS:
            if f == "focal_entities":
                sim = entity_overlap(entity_names(prev), entity_names(curr))
            else:
                sim = text_similarity(
                    normalize_field(prev.get(f)),
                    normalize_field(curr.get(f))
                )
            slow_changes[f].append(1.0 - sim)

        for f in FAST_FIELDS:
            sim = text_similarity(
                normalize_field(prev.get(f)),
                normalize_field(curr.get(f))
            )
            fast_changes[f].append(1.0 - sim)

        slow_text_prev = " ".join(
            normalize_field(prev.get(f)) for f in SLOW_FIELDS
        )
        slow_text_curr = " ".join(
            normalize_field(curr.get(f)) for f in SLOW_FIELDS
        )
        slow_nh_prev = score_neighborhoods(slow_text_prev)
        slow_nh_curr = score_neighborhoods(slow_text_curr)
        slow_meaning_stab.append(neighborhood_similarity(slow_nh_prev, slow_nh_curr))

        fast_text_prev = " ".join(
            normalize_field(prev.get(f)) for f in FAST_FIELDS
        )
        fast_text_curr = " ".join(
            normalize_field(curr.get(f)) for f in FAST_FIELDS
        )
        fast_nh_prev = score_neighborhoods(fast_text_prev)
        fast_nh_curr = score_neighborhoods(fast_text_curr)
        fast_meaning_stab.append(neighborhood_similarity(fast_nh_prev, fast_nh_curr))

    print(f"\nPer-field change rates (mean Δ):")
    print(f"  {'Field':<25} {'Class':<8} {'Mean Δ':>8} {'Median Δ':>10}")
    print(f"  {'-'*55}")

    all_slow_deltas = []
    all_fast_deltas = []

    for f in SLOW_FIELDS:
        changes = slow_changes[f]
        if changes:
            m = sum(changes) / len(changes)
            s = sorted(changes)
            med = s[len(s)//2]
            all_slow_deltas.extend(changes)
            print(f"  {f:<25} {'SLOW':<8} {m:>8.3f} {med:>10.3f}")

    for f in FAST_FIELDS:
        changes = fast_changes[f]
        if changes:
            m = sum(changes) / len(changes)
            s = sorted(changes)
            med = s[len(s)//2]
            all_fast_deltas.extend(changes)
            print(f"  {f:<25} {'FAST':<8} {m:>8.3f} {med:>10.3f}")

    slow_mean_delta = sum(all_slow_deltas) / len(all_slow_deltas) if all_slow_deltas else 0
    fast_mean_delta = sum(all_fast_deltas) / len(all_fast_deltas) if all_fast_deltas else 0

    slow_meaning_mean = sum(slow_meaning_stab) / len(slow_meaning_stab) if slow_meaning_stab else 0
    fast_meaning_mean = sum(fast_meaning_stab) / len(fast_meaning_stab) if fast_meaning_stab else 0

    print(f"\n  AGGREGATE:")
    print(f"    Slow fields — mean Δ: {slow_mean_delta:.3f}, meaning stability: {slow_meaning_mean:.3f}")
    print(f"    Fast fields — mean Δ: {fast_mean_delta:.3f}, meaning stability: {fast_meaning_mean:.3f}")

    slow_ref_stab = 1.0 - slow_mean_delta
    fast_ref_stab = 1.0 - fast_mean_delta

    print(f"\n  LEGIBILITY MAPPING:")
    print(f"    Slow fields: ref stability {slow_ref_stab:.3f}, meaning stability {slow_meaning_mean:.3f}")
    print(f"    Fast fields: ref stability {fast_ref_stab:.3f}, meaning stability {fast_meaning_mean:.3f}")

    slow_schema = slow_ref_stab > fast_ref_stab
    fast_encounter = fast_meaning_mean < slow_meaning_mean

    print(f"\n  VERDICT:")
    if slow_schema:
        print(f"    Slow fields more reference-stable than fast ✓ (schema-legible)")
    else:
        print(f"    Slow fields NOT more reference-stable ✗")

    if fast_encounter:
        print(f"    Fast fields show more meaning reorganization ✓ (encounter-legible)")
    else:
        print(f"    Fast fields do NOT show more meaning reorganization ✗")

    separation = abs(slow_ref_stab - fast_ref_stab)
    print(f"    Separation magnitude: {separation:.3f}")

    if slow_schema and fast_encounter:
        print(f"\n    #319 REPLICATES: Slow/fast separation maps onto schema/encounter legibility.")
    elif slow_schema:
        print(f"\n    PARTIAL: Reference axis separates, meaning axis doesn't.")
    else:
        print(f"\n    #319 DOES NOT REPLICATE on field separation test.")

    return {
        "slow_ref_stability": slow_ref_stab,
        "slow_meaning_stability": slow_meaning_mean,
        "fast_ref_stability": fast_ref_stab,
        "fast_meaning_stability": fast_meaning_mean,
        "separation": separation,
        "slow_schema_legible": slow_schema,
        "fast_encounter_legible": fast_encounter,
    }


def main():
    states = load_states()
    if len(states) < 10:
        print(f"Only {len(states)} states — need at least 10.")
        return 1

    print(f"Build #38: Thread #319 Legibility Inversion Replication")
    print(f"States loaded: {len(states)}")

    result_a = test_a_basin_constrained(states)
    result_b = test_b_field_separation(states)

    print(f"\n{'='*60}")
    print("OVERALL ASSESSMENT")
    print(f"{'='*60}")

    a_holds = (result_a and result_a.get("center", {}).get("gap", 0) > 0)
    b_holds = (result_b and result_b.get("slow_schema_legible") and result_b.get("fast_encounter_legible"))

    if a_holds and b_holds:
        print("BOTH tests pass: #319 legibility inversion replicates.")
        print("Gate CLEARED — meaning>references within basin, slow/fast maps to schema/encounter.")
        verdict = "REPLICATES"
    elif a_holds:
        print("Test A passes (basin-constrained), Test B partial or fails.")
        print("Gate PARTIAL — legibility holds within basin but field separation doesn't align cleanly.")
        verdict = "PARTIAL_A"
    elif b_holds:
        print("Test B passes (field separation), Test A partial or fails.")
        print("Gate PARTIAL — field separation aligns but basin structure doesn't constrain it.")
        verdict = "PARTIAL_B"
    else:
        print("NEITHER test passes. #319 does not replicate.")
        verdict = "FAILS"

    out = {
        "build": "38",
        "thread": 319,
        "test": "legibility_replication",
        "n_states": len(states),
        "test_a": result_a,
        "test_b": result_b,
        "verdict": verdict,
    }
    out_path = os.path.join(DATA_DIR, "legibility_replication_319.json")
    with open(out_path, "w") as f:
        json.dump(out, f, indent=2)
    print(f"\nResults saved to {out_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
