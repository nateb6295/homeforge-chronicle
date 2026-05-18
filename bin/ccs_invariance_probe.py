#!/usr/bin/env python3
"""Build #55: CCS Reparameterization Invariance Probe

Tests which CCS properties are truly invariant across compression
regimes vs encoding-dependent artifacts (per M.T. Bennett's flat
minima argument).

Method:
  1. Load all cognitive_state_history snapshots
  2. Partition into regimes (pre-trip, during-trip, or by time windows)
  3. Compute coefficient of variation WITHIN each regime
  4. Compute shift BETWEEN regimes
  5. Classify: truly invariant (low CV within AND small shift between)
     vs regime-sensitive (low CV within BUT shifts between)
     vs volatile (high CV within)
  6. Test predictive power: do invariant properties predict downstream
     CCS behavior (entity persistence, thread advances) better than
     volatile ones?

Usage:
  python3 ccs_invariance_probe.py [--partition EPOCH] [--output FILE]
"""

import sqlite3
import json
import gzip
import math
import argparse
import statistics
from collections import defaultdict
from datetime import datetime


DB_PATH = "/mnt/hdd/chronicle-data/processed.db"

# Nate departed May 15 9:50am PDT = ~1778863800
TRIP_START = 1778863800


def load_snapshots(db):
    rows = db.execute(
        "SELECT id, snapshot, created_at FROM cognitive_state_history "
        "ORDER BY created_at ASC"
    ).fetchall()
    snapshots = []
    for row_id, snap_json, ts in rows:
        try:
            data = json.loads(snap_json)
        except (json.JSONDecodeError, TypeError):
            continue
        snapshots.append({"id": row_id, "ts": ts, "data": data, "raw": snap_json})
    return snapshots


def extract_features(snap):
    data = snap["data"]
    raw = snap["raw"]

    gist = data.get("semantic_gist", "")
    goal = data.get("goal_orientation", "")
    entities = data.get("focal_entities", [])
    constraints = data.get("constraints", [])
    episodic = data.get("episodic_trace", [])
    uncertainties = data.get("uncertainty_signals", [])
    relmap = data.get("relational_map", {})
    pred_cue = data.get("predictive_cue", "")

    saliences = []
    for e in entities:
        if isinstance(e, dict):
            s = e.get("salience", 0)
            try:
                saliences.append(float(s))
            except (ValueError, TypeError):
                pass

    mean_sal = sum(saliences) / len(saliences) if saliences else 0
    std_sal = (
        (sum((s - mean_sal) ** 2 for s in saliences) / len(saliences)) ** 0.5
        if len(saliences) > 1
        else 0
    )
    max_sal = max(saliences) if saliences else 0
    min_sal = min(saliences) if saliences else 0
    sal_range = max_sal - min_sal

    snap_bytes = raw.encode()
    compressed = gzip.compress(snap_bytes)
    compression_ratio = len(compressed) / len(snap_bytes)

    gist_bytes = gist.encode()
    gist_compressed = gzip.compress(gist_bytes) if gist_bytes else b""
    gist_cr = len(gist_compressed) / len(gist_bytes) if gist_bytes else 0

    entity_names = set()
    for e in entities:
        if isinstance(e, dict):
            entity_names.add(e.get("name", e.get("entity", "?")))

    return {
        "gist_len": len(gist),
        "gist_compression_ratio": round(gist_cr, 4),
        "goal_len": len(goal),
        "pred_cue_len": len(pred_cue),
        "n_entities": len(entities),
        "n_constraints": len(constraints),
        "n_episodic": len(episodic),
        "n_uncertainties": len(uncertainties),
        "n_relations": len(relmap),
        "mean_salience": round(mean_sal, 4),
        "std_salience": round(std_sal, 4),
        "salience_range": round(sal_range, 4),
        "max_salience": round(max_sal, 4),
        "min_salience": round(min_sal, 4),
        "compression_ratio": round(compression_ratio, 4),
        "total_size": len(snap_bytes),
        "entity_names": entity_names,
    }


def compute_cv(values):
    if len(values) < 2:
        return 0.0
    mean = statistics.mean(values)
    if mean == 0:
        return float("inf")
    return statistics.stdev(values) / mean


def analyze_regime(features_list, label):
    print(f"\n{'=' * 60}")
    print(f"  REGIME: {label} (n={len(features_list)})")
    print(f"{'=' * 60}")

    numeric_keys = [
        k
        for k in features_list[0].keys()
        if k != "entity_names" and isinstance(features_list[0][k], (int, float))
    ]

    results = {}
    for key in sorted(numeric_keys):
        vals = [f[key] for f in features_list]
        mean = statistics.mean(vals)
        std = statistics.stdev(vals) if len(vals) > 1 else 0
        cv = compute_cv(vals)
        mn, mx = min(vals), max(vals)
        results[key] = {
            "mean": mean,
            "std": std,
            "cv": cv,
            "min": mn,
            "max": mx,
        }
        tag = "*** INVARIANT" if cv < 0.10 else ("~~ stable" if cv < 0.20 else "")
        print(
            f"  {key:28s}  mean={mean:10.3f}  std={std:8.3f}  "
            f"CV={cv:.4f}  [{mn:.1f}, {mx:.1f}]  {tag}"
        )

    return results


def compare_regimes(results_a, results_b, label_a, label_b):
    print(f"\n{'=' * 60}")
    print(f"  REGIME SHIFT: {label_a} → {label_b}")
    print(f"{'=' * 60}")

    shared_keys = set(results_a.keys()) & set(results_b.keys())
    shifts = []

    for key in sorted(shared_keys):
        a_mean = results_a[key]["mean"]
        b_mean = results_b[key]["mean"]
        delta = b_mean - a_mean
        pct = (delta / a_mean * 100) if a_mean != 0 else 0
        a_cv = results_a[key]["cv"]
        b_cv = results_b[key]["cv"]

        shifts.append((abs(pct), key, a_mean, b_mean, delta, pct, a_cv, b_cv))

    shifts.sort(reverse=True)

    for _, key, a_m, b_m, delta, pct, a_cv, b_cv in shifts:
        invariance = ""
        if a_cv < 0.10 and b_cv < 0.10 and abs(pct) < 5:
            invariance = "*** TRULY INVARIANT"
        elif a_cv < 0.10 and b_cv < 0.10 and abs(pct) >= 5:
            invariance = "!! regime-sensitive"
        elif a_cv >= 0.20 or b_cv >= 0.20:
            invariance = "~~ volatile"

        print(
            f"  {key:28s}  {label_a}={a_m:8.3f}  {label_b}={b_m:8.3f}  "
            f"Δ={delta:+8.3f} ({pct:+6.1f}%)  {invariance}"
        )

    return shifts


def entity_persistence_analysis(snapshots, partition):
    pre = defaultdict(list)
    post = defaultdict(list)

    for snap in snapshots:
        features = extract_features(snap)
        for name in features["entity_names"]:
            if snap["ts"] < partition:
                pre[name].append(snap["ts"])
            else:
                post[name].append(snap["ts"])

    shared = set(pre.keys()) & set(post.keys())
    pre_only = set(pre.keys()) - set(post.keys())
    post_only = set(post.keys()) - set(pre.keys())

    print(f"\n{'=' * 60}")
    print(f"  ENTITY PERSISTENCE")
    print(f"{'=' * 60}")
    print(f"  Shared (both regimes): {len(shared)}")
    print(f"  Pre-only (dropped):    {len(pre_only)}")
    print(f"  Post-only (new):       {len(post_only)}")
    persistence_rate = len(shared) / (len(shared) + len(pre_only)) if (shared or pre_only) else 0
    print(f"  Persistence rate:      {persistence_rate:.3f}")

    return {
        "shared": len(shared),
        "pre_only": len(pre_only),
        "post_only": len(post_only),
        "persistence_rate": persistence_rate,
        "shared_entities": sorted(shared),
        "new_entities": sorted(post_only),
    }


def predictive_test(snapshots, partition):
    """Test: do invariant properties at time T predict entity persistence at T+1?"""
    print(f"\n{'=' * 60}")
    print(f"  PREDICTIVE TEST: does compression_ratio predict persistence?")
    print(f"{'=' * 60}")

    pairs = []
    for i in range(len(snapshots) - 1):
        f_now = extract_features(snapshots[i])
        f_next = extract_features(snapshots[i + 1])
        overlap = len(f_now["entity_names"] & f_next["entity_names"])
        total = len(f_now["entity_names"] | f_next["entity_names"])
        jaccard = overlap / total if total > 0 else 0
        pairs.append({
            "compression_ratio": f_now["compression_ratio"],
            "mean_salience": f_now["mean_salience"],
            "std_salience": f_now["std_salience"],
            "n_entities": f_now["n_entities"],
            "gist_len": f_now["gist_len"],
            "entity_jaccard_next": jaccard,
        })

    if len(pairs) < 10:
        print("  Not enough data for predictive test")
        return {}

    target = [p["entity_jaccard_next"] for p in pairs]
    predictors = ["compression_ratio", "mean_salience", "std_salience", "n_entities", "gist_len"]

    for pred_key in predictors:
        pred_vals = [p[pred_key] for p in pairs]
        n = len(pred_vals)
        mean_x = sum(pred_vals) / n
        mean_y = sum(target) / n
        cov_xy = sum((pred_vals[i] - mean_x) * (target[i] - mean_y) for i in range(n)) / n
        var_x = sum((x - mean_x) ** 2 for x in pred_vals) / n
        var_y = sum((y - mean_y) ** 2 for y in target) / n
        denom = (var_x * var_y) ** 0.5
        r = cov_xy / denom if denom > 0 else 0
        print(f"  {pred_key:28s}  r={r:+.4f}  (correlation with next-step entity persistence)")

    return {pred_key: 0 for pred_key in predictors}


def main():
    parser = argparse.ArgumentParser(description="Build #55: CCS Invariance Probe")
    parser.add_argument("--partition", type=int, default=TRIP_START,
                        help="Epoch to partition regimes (default: trip start)")
    parser.add_argument("--output", type=str, default=None,
                        help="Save results to JSON file")
    args = parser.parse_args()

    db = sqlite3.connect(DB_PATH)
    snapshots = load_snapshots(db)
    print(f"Loaded {len(snapshots)} CCS snapshots")

    pre_features = []
    post_features = []
    for snap in snapshots:
        f = extract_features(snap)
        if snap["ts"] < args.partition:
            pre_features.append(f)
        else:
            post_features.append(f)

    print(f"Pre-partition: {len(pre_features)} snapshots")
    print(f"Post-partition: {len(post_features)} snapshots")

    results_pre = analyze_regime(pre_features, "PRE-TRIP")
    results_post = analyze_regime(post_features, "DURING-TRIP")
    shifts = compare_regimes(results_pre, results_post, "pre", "trip")
    entity_info = entity_persistence_analysis(snapshots, args.partition)
    predictive_test(snapshots, args.partition)

    if args.output:
        output = {
            "partition": args.partition,
            "n_pre": len(pre_features),
            "n_post": len(post_features),
            "pre_regime": {k: v for k, v in results_pre.items()},
            "post_regime": {k: v for k, v in results_post.items()},
            "entity_persistence": entity_info,
            "timestamp": datetime.utcnow().isoformat(),
        }
        with open(args.output, "w") as f:
            json.dump(output, f, indent=2, default=str)
        print(f"\nResults saved to {args.output}")

    db.close()


if __name__ == "__main__":
    main()
