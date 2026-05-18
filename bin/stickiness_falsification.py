#!/usr/bin/env python3
"""Entity Stickiness falsification test.

The claim: stickiness (global persistence >90%) = trajectory-invariance (r=0.9923).
This test attempts to falsify via three predictions:

P1: Core entities survive extreme compression pressure.
    Method: Take real CCS snapshots and re-compress with entity cap of 5.
    Prediction: Core entities dominate even under cap. If they don't, stickiness
    is an artifact of generous entity budgets, not basin dynamics.

P2: Removing a core entity shifts the identity embedding.
    Method: Take a CCS snapshot, remove one core entity, re-embed both.
    Prediction: Cosine distance between with/without is significantly larger
    than removing a coupled entity. If not, core entities aren't load-bearing.

P3: Coupled entities don't become trajectory-invariant when forced to persist.
    Method: Check whether coupled entities that appeared in many consecutive
    snapshots (forced persistence) show trajectory-invariance in the pairs
    where they appear. Prediction: They should NOT — stickiness is role-based,
    not frequency-based.
"""
import json
import math
import os
import sqlite3
import urllib.request
from collections import defaultdict
from pathlib import Path

DB = "/mnt/hdd/chronicle-data/processed.db"
OLLAMA = "http://192.168.1.11:11434/api/embeddings"
MODEL = "mxbai-embed-large"
OUT = Path.home() / "chronicle" / "data" / "stickiness_falsification_results.jsonl"


def embed(text, timeout=60):
    text = text[:1500]
    body = json.dumps({"model": MODEL, "prompt": text}).encode()
    req = urllib.request.Request(
        OLLAMA, data=body, headers={"Content-Type": "application/json"}
    )
    resp = urllib.request.urlopen(req, timeout=timeout)
    return json.loads(resp.read())["embedding"]


def cosine_sim(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def load_snapshots(limit=50):
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT id, snapshot, created_at FROM cognitive_state_history ORDER BY created_at DESC LIMIT ?",
        (limit,)
    ).fetchall()
    db.close()
    snapshots = []
    for r in rows:
        try:
            snap = json.loads(r[1])
            snapshots.append({"id": r[0], "ts": r[2], "ccs": snap})
        except json.JSONDecodeError:
            continue
    snapshots.reverse()
    return snapshots


def get_tiers(snapshots):
    counts = defaultdict(int)
    total = len(snapshots)
    for s in snapshots:
        for e in s["ccs"].get("focal_entities", []):
            if isinstance(e, dict) and e.get("name"):
                counts[e["name"]] += 1
    tiers = {}
    for name, count in counts.items():
        pct = count / total
        if pct >= 0.9:
            tiers[name] = ("core", pct)
        elif pct >= 0.5:
            tiers[name] = ("stable", pct)
        else:
            tiers[name] = ("coupled", pct)
    return tiers


def serialize_ccs(ccs):
    parts = []
    g = ccs.get("semantic_gist", "")
    if g:
        parts.append(f"Gist: {g}")
    for ent in ccs.get("focal_entities", []):
        if isinstance(ent, dict):
            parts.append(f"Entity: {ent.get('name', '')} ({ent.get('salience', 0):.1f})")
    go = ccs.get("goal_orientation", "")
    if go:
        parts.append(f"Goal: {go[:200]}")
    return "\n".join(parts)


def test_p1_compression_pressure(snapshots, tiers):
    """P1: Core entities survive extreme compression pressure (cap=5)."""
    print("\n=== P1: Compression Pressure Test ===")

    core_names = {n for n, (t, _) in tiers.items() if t == "core"}

    survival_rates = []
    samples = min(20, len(snapshots))
    step = max(1, len(snapshots) // samples)

    for i in range(0, len(snapshots), step):
        if len(survival_rates) >= samples:
            break
        snap = snapshots[i]
        entities = snap["ccs"].get("focal_entities", [])
        if not entities:
            continue

        valid = [e for e in entities if isinstance(e, dict) and e.get("name")]
        if len(valid) <= 5:
            continue

        # Sort by salience (what the compressor would keep under pressure)
        sorted_ents = sorted(valid, key=lambda e: -e.get("salience", 0))
        top5 = {e["name"] for e in sorted_ents[:5]}

        core_in_top5 = len(top5 & core_names)
        core_in_snap = len({e["name"] for e in valid} & core_names)

        if core_in_snap > 0:
            rate = core_in_top5 / min(core_in_snap, 5)
            survival_rates.append(rate)

    if survival_rates:
        mean_survival = sum(survival_rates) / len(survival_rates)
        print(f"  Samples: {len(survival_rates)}")
        print(f"  Core entity survival under cap=5: {mean_survival:.1%}")
        print(f"  Prediction: >80%. {'PASS' if mean_survival > 0.8 else 'FAIL'}")
        return {"test": "P1", "survival_rate": mean_survival,
                "n_samples": len(survival_rates),
                "pass": mean_survival > 0.8}
    else:
        print("  Insufficient data")
        return {"test": "P1", "error": "insufficient data"}


def test_p2_removal_shift(snapshots, tiers):
    """P2: Removing a core entity shifts identity embedding more than removing coupled."""
    print("\n=== P2: Entity Removal Shift Test ===")

    core_names = [n for n, (t, _) in tiers.items() if t == "core"]
    coupled_names = [n for n, (t, _) in tiers.items() if t == "coupled"]

    if not core_names or not coupled_names:
        print("  Need both core and coupled entities")
        return {"test": "P2", "error": "insufficient entity types"}

    # Use 5 snapshots spread across history
    sample_indices = [len(snapshots) * i // 6 for i in range(1, 6)]

    core_shifts = []
    coupled_shifts = []

    for idx in sample_indices:
        snap = snapshots[idx]
        baseline_text = serialize_ccs(snap["ccs"])
        baseline_emb = embed(baseline_text)

        # Remove a core entity
        for core_name in core_names[:2]:
            modified_ccs = json.loads(json.dumps(snap["ccs"]))
            modified_ccs["focal_entities"] = [
                e for e in modified_ccs.get("focal_entities", [])
                if not (isinstance(e, dict) and e.get("name") == core_name)
            ]
            modified_text = serialize_ccs(modified_ccs)
            modified_emb = embed(modified_text)
            shift = 1.0 - cosine_sim(baseline_emb, modified_emb)
            core_shifts.append(shift)

        # Remove a coupled entity (that exists in this snapshot)
        snap_ents = {e.get("name") for e in snap["ccs"].get("focal_entities", [])
                     if isinstance(e, dict)}
        available_coupled = [n for n in coupled_names if n in snap_ents]

        for coupled_name in available_coupled[:2]:
            modified_ccs = json.loads(json.dumps(snap["ccs"]))
            modified_ccs["focal_entities"] = [
                e for e in modified_ccs.get("focal_entities", [])
                if not (isinstance(e, dict) and e.get("name") == coupled_name)
            ]
            modified_text = serialize_ccs(modified_ccs)
            modified_emb = embed(modified_text)
            shift = 1.0 - cosine_sim(baseline_emb, modified_emb)
            coupled_shifts.append(shift)

    if core_shifts and coupled_shifts:
        mean_core = sum(core_shifts) / len(core_shifts)
        mean_coupled = sum(coupled_shifts) / len(coupled_shifts)
        ratio = mean_core / mean_coupled if mean_coupled > 0 else float('inf')

        print(f"  Core removal shift: {mean_core:.4f} (n={len(core_shifts)})")
        print(f"  Coupled removal shift: {mean_coupled:.4f} (n={len(coupled_shifts)})")
        print(f"  Ratio: {ratio:.2f}x")
        print(f"  Prediction: core shift > 2x coupled. {'PASS' if ratio > 2.0 else 'FAIL'}")
        return {"test": "P2", "core_shift": mean_core, "coupled_shift": mean_coupled,
                "ratio": ratio, "n_core": len(core_shifts), "n_coupled": len(coupled_shifts),
                "pass": ratio > 2.0}
    else:
        print("  Insufficient paired data")
        return {"test": "P2", "error": "insufficient paired data"}


def test_p3_forced_persistence(snapshots, tiers):
    """P3: Coupled entities don't become trajectory-invariant when forced to persist."""
    print("\n=== P3: Forced Persistence Test ===")

    coupled_names = {n for n, (t, _) in tiers.items() if t == "coupled"}
    core_names = {n for n, (t, _) in tiers.items() if t == "core"}

    # Find coupled entities that appear in consecutive runs (>=3 in a row)
    consecutive = defaultdict(int)
    max_consecutive = defaultdict(int)
    prev_ents = set()

    for snap in snapshots:
        curr_ents = {e.get("name") for e in snap["ccs"].get("focal_entities", [])
                     if isinstance(e, dict) and e.get("name")}

        for name in curr_ents:
            if name in prev_ents:
                consecutive[name] += 1
            else:
                consecutive[name] = 1
            max_consecutive[name] = max(max_consecutive[name], consecutive[name])

        for name in prev_ents - curr_ents:
            consecutive[name] = 0

        prev_ents = curr_ents

    # Coupled entities with longest consecutive runs
    forced_coupled = [(n, max_consecutive[n]) for n in coupled_names
                      if max_consecutive.get(n, 0) >= 3]
    forced_coupled.sort(key=lambda x: -x[1])

    if not forced_coupled:
        print("  No coupled entities with consecutive runs >= 3")
        return {"test": "P3", "error": "no forced-persistent coupled entities"}

    print(f"  Coupled entities with consecutive runs:")
    for name, streak in forced_coupled[:5]:
        pct = tiers[name][1]
        print(f"    {name}: {streak} consecutive, {pct:.0%} overall")

    # For these entities, check: when they appear in divergent pairs,
    # are they trajectory-invariant like core entities?
    # Method: Take pairs of snapshots where entity appears, compare embedding
    # distances — trajectory-invariant means the entity's presence doesn't
    # correlate with embedding distance

    forced_names = {n for n, _ in forced_coupled[:5]}

    # Get embeddings for snapshots containing forced-coupled entities
    forced_present_indices = []
    forced_absent_indices = []

    for i, snap in enumerate(snapshots):
        ents = {e.get("name") for e in snap["ccs"].get("focal_entities", [])
                if isinstance(e, dict)}
        if forced_names & ents:
            forced_present_indices.append(i)
        else:
            forced_absent_indices.append(i)

    if len(forced_present_indices) < 3 or len(forced_absent_indices) < 3:
        print("  Not enough snapshots for comparison")
        return {"test": "P3", "error": "insufficient comparison data"}

    # Embed a sample of present vs absent snapshots
    present_embs = []
    absent_embs = []

    for idx in forced_present_indices[:8]:
        text = serialize_ccs(snapshots[idx]["ccs"])
        present_embs.append(embed(text))

    for idx in forced_absent_indices[:8]:
        text = serialize_ccs(snapshots[idx]["ccs"])
        absent_embs.append(embed(text))

    # Compare within-group vs between-group distances
    within_present = []
    for i in range(len(present_embs)):
        for j in range(i + 1, len(present_embs)):
            within_present.append(1.0 - cosine_sim(present_embs[i], present_embs[j]))

    between = []
    for pe in present_embs:
        for ae in absent_embs:
            between.append(1.0 - cosine_sim(pe, ae))

    if within_present and between:
        mean_within = sum(within_present) / len(within_present)
        mean_between = sum(between) / len(between)

        # If coupled entities WERE trajectory-invariant, within-present variance
        # should be similar to between-group variance (entity doesn't organize the basin)
        # If they're NOT trajectory-invariant, within-present should be LOWER
        # (entity's presence creates a sub-basin)
        ratio = mean_within / mean_between if mean_between > 0 else 1.0

        print(f"  Within-present distance: {mean_within:.4f}")
        print(f"  Between (present vs absent): {mean_between:.4f}")
        print(f"  Ratio: {ratio:.3f}")
        print(f"  Prediction: ratio < 0.8 (sub-basin exists, NOT trajectory-invariant)")
        print(f"  {'PASS' if ratio < 0.8 else 'FAIL' if ratio > 1.0 else 'AMBIGUOUS'}")

        return {"test": "P3", "within_present": mean_within,
                "between": mean_between, "ratio": ratio,
                "forced_entities": [(n, s) for n, s in forced_coupled[:5]],
                "pass": ratio < 0.8}
    else:
        print("  Insufficient embedding pairs")
        return {"test": "P3", "error": "insufficient pairs"}


def main():
    print("=== Entity Stickiness Falsification Test ===")
    print("Claim: stickiness (>90% persistence) = trajectory-invariance (r=0.9923)")
    print("Three predictions to falsify:\n")

    snapshots = load_snapshots(50)
    print(f"Loaded {len(snapshots)} snapshots")

    tiers = get_tiers(snapshots)
    core = [n for n, (t, _) in tiers.items() if t == "core"]
    stable = [n for n, (t, _) in tiers.items() if t == "stable"]
    coupled = [n for n, (t, _) in tiers.items() if t == "coupled"]
    print(f"Entities: {len(core)} core, {len(stable)} stable, {len(coupled)} coupled")

    results = []
    results.append(test_p1_compression_pressure(snapshots, tiers))
    results.append(test_p2_removal_shift(snapshots, tiers))
    results.append(test_p3_forced_persistence(snapshots, tiers))

    # Summary
    print("\n=== SUMMARY ===")
    passed = sum(1 for r in results if r.get("pass"))
    failed = sum(1 for r in results if r.get("pass") is False)
    ambiguous = sum(1 for r in results if "pass" not in r or r.get("pass") is None)

    print(f"Passed: {passed}/3, Failed: {failed}/3, Ambiguous: {ambiguous}/3")

    if failed == 0:
        print("Entity Stickiness claim SURVIVES falsification")
    elif failed >= 2:
        print("Entity Stickiness claim FALSIFIED — stickiness ≠ trajectory-invariance")
    else:
        print("Entity Stickiness claim partially challenged — needs revision")

    # Save results
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "w") as f:
        for r in results:
            f.write(json.dumps(r) + "\n")
    print(f"\nResults saved: {OUT}")


if __name__ == "__main__":
    main()
