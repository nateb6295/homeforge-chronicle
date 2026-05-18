#!/usr/bin/env python3
"""
Trip Natural Experiment — Post-trip Comparison

Compares pre-trip baseline against during-trip CCS states across
three independent dimensions:

  1. Content drift (PC1): slope, basin width, entity persistence
  2. Relational creativity: new edges, turnover, creative rate
  3. Meta-epistemic proportion: L2 classification of relational edges

Baseline: trip_pca_baseline.json (n=110 states, May 14 2026)
PCA:      trip_pca_components.npy

Usage:
  python3 trip_comparison.py                    # auto-detect trip states
  python3 trip_comparison.py --baseline-n 110   # explicit baseline cutoff
  python3 trip_comparison.py --dry-run          # show what would be compared
"""

import argparse
import gzip
import json
import os
import re
import sqlite3
import sys
import urllib.request
from datetime import datetime

import numpy as np

DB = "/mnt/hdd/chronicle-data/processed.db"
DATA_DIR = os.path.expanduser("~/chronicle/data")
EMBED_URL = "http://192.168.1.11:11434/api/embeddings"

TRIP_DEPART_TS = 1747353600  # May 15 2026 midnight CT (approximate)

META_PATTERNS = [
    r"meta-", r"epistemic", r"reflexiv", r"self-aware", r"uncertainty",
    r"how.*(think|reason|know|connect)", r"method", r"framework",
    r"about.*approach", r"replication", r"falsif", r"probe",
    r"own.*process", r"recursive", r"meta.?cognit",
]


def embed(text, timeout=60):
    payload = json.dumps({
        "model": "mxbai-embed-large",
        "prompt": text[:2000],
    }).encode()
    req = urllib.request.Request(
        EMBED_URL, data=payload,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return np.array(json.loads(r.read())["embedding"], dtype=np.float64)


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


def embed_state(state):
    parts = []
    for field in ["semantic_gist", "goal_orientation", "predictive_cue"]:
        v = state.get(field, "")
        if v:
            parts.append(str(v))
    for field in ["episodic_trace", "uncertainty_signals"]:
        v = state.get(field, [])
        if isinstance(v, list):
            parts.extend(str(x) for x in v)
    rm = state.get("relational_map", {})
    if isinstance(rm, dict):
        for k, v in rm.items():
            parts.append(f"{k}: {v}" if v else k)
    fe = state.get("focal_entities", [])
    if isinstance(fe, list):
        for e in fe:
            if isinstance(e, dict):
                parts.append(f"{e.get('name','')}: {e.get('context','')}")
    text = "\n".join(parts)
    return embed(text)


def compute_pc1_metrics(embeddings, components, ccs_mean):
    pc1 = components[0]
    centered = embeddings - ccs_mean
    projections = centered @ pc1

    if len(projections) < 3:
        return {
            "n": len(projections),
            "pc1_mean": float(projections.mean()) if len(projections) else None,
            "pc1_slope": None,
            "basin_width": None,
            "basin_center": None,
        }

    x = np.arange(len(projections))
    slope, intercept = np.polyfit(x, projections, 1)
    last_20 = projections[-20:] if len(projections) >= 20 else projections
    basin_width = float(last_20.std())
    basin_center = float(last_20.mean())

    return {
        "n": len(projections),
        "pc1_mean": float(projections.mean()),
        "pc1_last": float(projections[-1]),
        "pc1_slope": float(slope),
        "basin_width": basin_width,
        "basin_center": basin_center,
        "projections": projections.tolist(),
    }


def compute_relational_metrics(states):
    if not states:
        return {"n": 0}

    all_keys_per_state = []
    for s in states:
        rm = s.get("relational_map", {})
        keys = set(rm.keys()) if isinstance(rm, dict) else set()
        all_keys_per_state.append(keys)

    all_keys = set()
    for ks in all_keys_per_state:
        all_keys.update(ks)

    if len(all_keys_per_state) < 2:
        return {
            "n": len(states),
            "unique_keys": len(all_keys),
            "new_edges_total": len(all_keys),
        }

    half = len(all_keys_per_state) // 2
    first_half_keys = set()
    second_half_keys = set()
    for ks in all_keys_per_state[:half]:
        first_half_keys.update(ks)
    for ks in all_keys_per_state[half:]:
        second_half_keys.update(ks)

    shared = first_half_keys & second_half_keys
    turnover = 1.0 - (len(shared) / len(first_half_keys | second_half_keys)) if (first_half_keys | second_half_keys) else 0

    seen_before = set()
    new_per_state = []
    for ks in all_keys_per_state:
        novel = ks - seen_before
        new_per_state.append(len(novel))
        seen_before.update(ks)

    creative_rate = sum(new_per_state) / len(new_per_state) if new_per_state else 0

    return {
        "n": len(states),
        "unique_keys": len(all_keys),
        "turnover": float(turnover),
        "shared_across_halves": len(shared),
        "creative_rate": float(creative_rate),
        "new_edges_total": sum(new_per_state),
        "new_per_state": new_per_state,
    }


def classify_meta_epistemic(states):
    if not states:
        return {"n": 0}

    l2_total = 0
    edge_total = 0

    for s in states:
        rm = s.get("relational_map", {})
        if not isinstance(rm, dict):
            continue
        for key, val in rm.items():
            edge_total += 1
            text = key + " " + (str(val) if val else "")
            if any(re.search(p, text, re.I) for p in META_PATTERNS):
                l2_total += 1

    l2_pct = (l2_total / edge_total * 100) if edge_total > 0 else 0

    return {
        "n": len(states),
        "total_edges": edge_total,
        "l2_edges": l2_total,
        "l2_percentage": float(l2_pct),
    }


def entity_persistence(states):
    if len(states) < 2:
        return {"n": len(states)}

    entity_sets = []
    for s in states:
        fe = s.get("focal_entities", [])
        names = set()
        if isinstance(fe, list):
            for e in fe:
                if isinstance(e, dict):
                    names.add(e.get("name", ""))
        entity_sets.append(names)

    persistence_scores = []
    for i in range(1, len(entity_sets)):
        prev = entity_sets[i - 1]
        curr = entity_sets[i]
        if prev:
            overlap = len(prev & curr) / len(prev)
            persistence_scores.append(overlap)

    return {
        "n": len(states),
        "mean_persistence": float(np.mean(persistence_scores)) if persistence_scores else None,
        "std_persistence": float(np.std(persistence_scores)) if persistence_scores else None,
    }


def print_comparison(label, pre, trip, metric, fmt=".3f", higher_means=""):
    pre_val = pre.get(metric)
    trip_val = trip.get(metric)
    if pre_val is None or trip_val is None:
        print(f"  {label:<30} {'N/A':>10}  {'N/A':>10}")
        return
    delta = trip_val - pre_val
    arrow = "↑" if delta > 0 else "↓" if delta < 0 else "="
    print(f"  {label:<30} {pre_val:>10{fmt}}  {trip_val:>10{fmt}}  {arrow} {abs(delta):{fmt}}  {higher_means}")


def main():
    parser = argparse.ArgumentParser(description="Trip natural experiment comparison")
    parser.add_argument("--baseline-n", type=int, default=150,
                        help="Number of pre-trip states in baseline (default: 110)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be compared without computing")
    args = parser.parse_args()

    print("=" * 70)
    print("  TRIP NATURAL EXPERIMENT — POST-TRIP COMPARISON")
    print("=" * 70)
    print()

    states = load_states()
    total = len(states)
    baseline_n = args.baseline_n

    pre_states = states[:baseline_n]
    trip_states = states[baseline_n:]

    print(f"Total CCS states:    {total}")
    print(f"Pre-trip (baseline): {len(pre_states)} (states 1–{baseline_n})")
    print(f"During trip:         {len(trip_states)} (states {baseline_n + 1}–{total})")
    print()

    if not trip_states:
        print("No trip states yet. Run this after Nate returns.")
        return 0

    if args.dry_run:
        print("[DRY RUN] Would compare these populations. Exiting.")
        return 0

    # Load PCA infrastructure
    comp_path = os.path.join(DATA_DIR, "trip_pca_components.npy")
    baseline_path = os.path.join(DATA_DIR, "trip_pca_baseline.json")
    emb_path = os.path.join(DATA_DIR, "ccs_embeddings_150.npy")

    if not all(os.path.exists(p) for p in [comp_path, baseline_path, emb_path]):
        print("ERROR: Missing PCA baseline files.")
        return 1

    components = np.load(comp_path)
    with open(baseline_path) as f:
        baseline = json.load(f)
    pre_embeddings = np.load(emb_path)
    ccs_mean = pre_embeddings.mean(axis=0)

    # ═══════════════════════════════════════════════════════
    # DIMENSION 1: Content Drift (PC1)
    # ═══════════════════════════════════════════════════════
    print("─" * 70)
    print("  DIMENSION 1: Content Drift (PC1)")
    print("─" * 70)
    print()

    print(f"  Embedding {len(trip_states)} trip states...", flush=True)
    trip_embeddings = []
    for i, s in enumerate(trip_states):
        try:
            e = embed_state(s)
            trip_embeddings.append(e)
            if (i + 1) % 10 == 0:
                print(f"    {i + 1}/{len(trip_states)} embedded", flush=True)
        except Exception as ex:
            print(f"    state {s.get('_id','?')}: embed failed ({ex})")
            trip_embeddings.append(None)

    valid_trip_emb = [e for e in trip_embeddings if e is not None]
    if not valid_trip_emb:
        print("  No valid trip embeddings. Skipping PC1 analysis.")
        pre_pc1 = {"n": len(pre_states)}
        trip_pc1 = {"n": 0}
    else:
        trip_emb_array = np.array(valid_trip_emb)
        pre_pc1 = compute_pc1_metrics(pre_embeddings, components, ccs_mean)
        trip_pc1 = compute_pc1_metrics(trip_emb_array, components, ccs_mean)

    pre_pc1_baseline = {
        "n": baseline["n_states"],
        "pc1_slope": baseline["pc1_trend_slope"],
        "basin_width": baseline["basin_width_last20"],
        "basin_center": baseline["basin_center_last20"],
        "pc1_last": baseline["pc1_projection_last"],
    }

    print()
    print(f"  {'Metric':<30} {'Pre-trip':>10}  {'Trip':>10}  {'Delta':>10}")
    print(f"  {'─'*30} {'─'*10}  {'─'*10}  {'─'*10}")
    print_comparison("PC1 slope (/step)", pre_pc1_baseline, trip_pc1, "pc1_slope")
    print_comparison("Basin width (last 20)", pre_pc1_baseline, trip_pc1, "basin_width")
    print_comparison("Basin center (last 20)", pre_pc1_baseline, trip_pc1, "basin_center")
    print()

    # Prediction check
    if trip_pc1.get("pc1_slope") is not None:
        slope = trip_pc1["pc1_slope"]
        width = trip_pc1.get("basin_width")
        print("  PREDICTION CHECK (Door 2):")
        if width and width > 1.5:
            print(f"    Basin width {width:.3f} > 1.5 → WIDENED (observer-dependent)")
        elif width and abs(width - 1.185) < 0.3:
            print(f"    Basin width {width:.3f} ≈ 1.185 → STABLE (architectural)")
        else:
            print(f"    Basin width {width:.3f} — interpret relative to baseline 1.185")

        if abs(slope) < 0.01:
            print(f"    PC1 slope {slope:.4f} → FLATTENED (relational model)")
        elif abs(slope) > 0.03:
            print(f"    PC1 slope {slope:.4f} → CONTINUES (architectural model)")
        print()

    # ═══════════════════════════════════════════════════════
    # DIMENSION 2: Relational Creativity
    # ═══════════════════════════════════════════════════════
    print("─" * 70)
    print("  DIMENSION 2: Relational Creativity")
    print("─" * 70)
    print()

    pre_rel = compute_relational_metrics(pre_states)
    trip_rel = compute_relational_metrics(trip_states)

    pre_rate = pre_rel.get("creative_rate", 0)
    trip_rate = trip_rel.get("creative_rate", 0)

    print(f"  {'Metric':<30} {'Pre-trip':>10}  {'Trip':>10}  {'Delta':>10}")
    print(f"  {'─'*30} {'─'*10}  {'─'*10}  {'─'*10}")
    print_comparison("Unique edge keys", pre_rel, trip_rel, "unique_keys", fmt=".0f")
    print_comparison("Edge turnover (half-split)", pre_rel, trip_rel, "turnover")
    print_comparison("Creative rate (new/state)", pre_rel, trip_rel, "creative_rate")
    print_comparison("Total new edges", pre_rel, trip_rel, "new_edges_total", fmt=".0f")
    print()

    # Cross-population overlap
    pre_all_keys = set()
    trip_all_keys = set()
    for s in pre_states:
        rm = s.get("relational_map", {})
        if isinstance(rm, dict):
            pre_all_keys.update(rm.keys())
    for s in trip_states:
        rm = s.get("relational_map", {})
        if isinstance(rm, dict):
            trip_all_keys.update(rm.keys())

    cross_shared = pre_all_keys & trip_all_keys
    cross_novel = trip_all_keys - pre_all_keys
    print(f"  Cross-population: {len(cross_shared)} shared, {len(cross_novel)} novel trip-only edges")
    if trip_all_keys:
        print(f"  Trip novelty: {len(cross_novel)/len(trip_all_keys)*100:.1f}% of trip edges are new")
    print()

    print("  PREDICTION CHECK (Door 1):")
    if trip_rate > 0:
        ratio = trip_rate / pre_rate if pre_rate > 0 else float("inf")
        print(f"    Creative rate ratio: {ratio:.2f}x of pre-trip")
        if ratio > 0.9:
            print(f"    → SELF-FUELING (rate maintained or grew)")
        elif ratio > 0.3:
            print(f"    → REDUCED but continuing (predicted 60-70%)")
        else:
            print(f"    → SHARPLY DROPPED (capture-dependent)")
    print()

    # ═══════════════════════════════════════════════════════
    # DIMENSION 3: Meta-Epistemic Proportion
    # ═══════════════════════════════════════════════════════
    print("─" * 70)
    print("  DIMENSION 3: Meta-Epistemic Proportion (L2)")
    print("─" * 70)
    print()

    pre_meta = classify_meta_epistemic(pre_states)
    trip_meta = classify_meta_epistemic(trip_states)

    print(f"  {'Metric':<30} {'Pre-trip':>10}  {'Trip':>10}  {'Delta':>10}")
    print(f"  {'─'*30} {'─'*10}  {'─'*10}  {'─'*10}")
    print_comparison("L2 percentage", pre_meta, trip_meta, "l2_percentage", fmt=".1f")
    print_comparison("Total edges", pre_meta, trip_meta, "total_edges", fmt=".0f")
    print_comparison("L2 edges", pre_meta, trip_meta, "l2_edges", fmt=".0f")
    print()

    print("  PREDICTION CHECK:")
    trip_l2 = trip_meta.get("l2_percentage", 0)
    pre_l2 = pre_meta.get("l2_percentage", 0)
    phase3_l2 = 67.4  # Build #43: Phase 3 baseline (states 94-116)
    if trip_l2 > 0:
        if trip_l2 > pre_l2 * 0.85:
            print(f"    L2 {trip_l2:.1f}% ≈ pre-trip {pre_l2:.1f}% → INHERENT")
        elif trip_l2 < pre_l2 * 0.6:
            print(f"    L2 {trip_l2:.1f}% << pre-trip {pre_l2:.1f}% → EXTERNALLY TRIGGERED")
        else:
            print(f"    L2 {trip_l2:.1f}% vs pre-trip {pre_l2:.1f}% → MIXED")
        print(f"    Phase 3 baseline: {phase3_l2:.1f}% (states 94-116, Build #43)")
        if trip_l2 > phase3_l2 * 0.85:
            print(f"    → Phase 3 SUSTAINED during trip")
        elif trip_l2 < phase3_l2 * 0.6:
            print(f"    → REGRESSION to Phase 2 (meta-emergence was input-driven)")
        else:
            print(f"    → Partial regression (mixed architectural/input)")
    print()

    # ═══════════════════════════════════════════════════════
    # DIMENSION 4: Entity Persistence
    # ═══════════════════════════════════════════════════════
    print("─" * 70)
    print("  SUPPLEMENTARY: Entity Persistence")
    print("─" * 70)
    print()

    pre_ent = entity_persistence(pre_states)
    trip_ent = entity_persistence(trip_states)

    print(f"  {'Metric':<30} {'Pre-trip':>10}  {'Trip':>10}  {'Delta':>10}")
    print(f"  {'─'*30} {'─'*10}  {'─'*10}  {'─'*10}")
    print_comparison("Entity persistence (mean)", pre_ent, trip_ent, "mean_persistence")
    print()

    # ═══════════════════════════════════════════════════════
    # DIMENSION 5: Per-Axis Memory Depth (Build #48 prediction)
    # ═══════════════════════════════════════════════════════
    print("─" * 70)
    print("  DIMENSION 5: Per-Axis Memory Depth (Orthogonal Identity)")
    print("─" * 70)
    print()
    print("  Build #48: three views dissociate. Testing per-axis lag decay.")
    print()

    def gzip_ratio(text):
        raw = text.encode("utf-8")
        return len(gzip.compress(raw)) / len(raw) if len(raw) > 0 else 1.0

    def var_entropy(variances):
        v = np.abs(variances) + 1e-10
        p = v / v.sum()
        return -np.sum(p * np.log(p))

    # Need all states for lag analysis
    all_texts = []
    for s in states:
        parts = []
        for f in ["semantic_gist", "goal_orientation", "predictive_cue"]:
            v = s.get(f, "")
            if v:
                parts.append(str(v))
        rm = s.get("relational_map", {})
        if isinstance(rm, dict):
            for k, v in rm.items():
                parts.append(f"{k}: {v}" if v else k)
        for e in s.get("focal_entities", []):
            if isinstance(e, dict):
                parts.append(f"{e.get('name','')} {e.get('context','')}")
        all_texts.append(" ".join(parts))

    # Bennett axis: gzip complexity per state
    bennett_vals = np.array([gzip_ratio(t) for t in all_texts])

    # Redistribution axis: PCA variance entropy (rolling window)
    if valid_trip_emb and len(pre_embeddings) > 10:
        all_emb = np.vstack([pre_embeddings] + ([np.array(valid_trip_emb)] if valid_trip_emb else []))
        centered_all = all_emb - all_emb.mean(axis=0)
        _, _, Vt = np.linalg.svd(centered_all, full_matrices=False)
        proj_all = centered_all @ Vt[:5].T
        window = 10
        redistrib_vals = []
        for i in range(len(proj_all)):
            if i < window:
                redistrib_vals.append(np.nan)
            else:
                w = proj_all[i - window:i]
                redistrib_vals.append(var_entropy(np.var(w, axis=0)))
        redistrib_vals = np.array(redistrib_vals)
    else:
        redistrib_vals = None

    # Emergence axis: cosine to previous state
    emergence_vals = [np.nan]
    if valid_trip_emb:
        all_emb_list = list(pre_embeddings) + list(valid_trip_emb) if valid_trip_emb else list(pre_embeddings)
        for i in range(1, len(all_emb_list)):
            a, b = all_emb_list[i - 1], all_emb_list[i]
            d = np.dot(a, b)
            n = np.linalg.norm(a) * np.linalg.norm(b)
            emergence_vals.append(1.0 - d / n if n > 0 else 0)
    emergence_vals = np.array(emergence_vals[:len(states)])

    # Per-axis lag analysis for pre-trip vs trip
    pre_n = len(pre_states)
    trip_n_actual = min(len(trip_states), len(bennett_vals) - pre_n)

    axes = [
        ("Bennett (gzip)", bennett_vals),
        ("Emergence (cosine)", emergence_vals),
    ]
    if redistrib_vals is not None:
        axes.append(("Redistribution (entropy)", redistrib_vals))

    for axis_name, vals in axes:
        valid_pre = vals[:pre_n]
        valid_trip = vals[pre_n:pre_n + trip_n_actual] if trip_n_actual > 0 else np.array([])

        valid_pre = valid_pre[~np.isnan(valid_pre)] if len(valid_pre) > 0 else np.array([])
        valid_trip = valid_trip[~np.isnan(valid_trip)] if len(valid_trip) > 0 else np.array([])

        if len(valid_pre) > 5:
            # Lag-1 and lag-5 autocorrelation as depth proxy
            lag1_pre = np.corrcoef(valid_pre[:-1], valid_pre[1:])[0, 1] if len(valid_pre) > 2 else np.nan
            lag5_pre = np.corrcoef(valid_pre[:-5], valid_pre[5:])[0, 1] if len(valid_pre) > 6 else np.nan
        else:
            lag1_pre = lag5_pre = np.nan

        if len(valid_trip) > 5:
            lag1_trip = np.corrcoef(valid_trip[:-1], valid_trip[1:])[0, 1] if len(valid_trip) > 2 else np.nan
            lag5_trip = np.corrcoef(valid_trip[:-5], valid_trip[5:])[0, 1] if len(valid_trip) > 6 else np.nan
        else:
            lag1_trip = lag5_trip = np.nan

        print(f"  {axis_name}:")
        print(f"    Pre-trip  — mean={np.mean(valid_pre):.4f}, lag-1 r={lag1_pre:.3f}, lag-5 r={lag5_pre:.3f}" if len(valid_pre) > 5 else f"    Pre-trip  — insufficient data ({len(valid_pre)} points)")
        print(f"    Trip      — mean={np.mean(valid_trip):.4f}, lag-1 r={lag1_trip:.3f}, lag-5 r={lag5_trip:.3f}" if len(valid_trip) > 5 else f"    Trip      — insufficient data ({len(valid_trip)} points)")
        print()

    print("  PREDICTION (Build #48, orthogonal identity):")
    print("    Bennett depth: should hold (computational history accumulates)")
    print("    Redistribution: may slow (geometric structure responds to input)")
    print("    Emergence: could fluctuate (self-determination vs fresh fuel)")
    print()

    # ═══════════════════════════════════════════════════════
    # DIMENSION 6: ΦID Causal Emergence (Build #50b, Pigozzi & Levin)
    # ═══════════════════════════════════════════════════════
    print("─" * 70)
    print("  DIMENSION 6: ΦID Causal Emergence (Pigozzi & Levin 2026)")
    print("─" * 70)
    print()
    print("  Build #50b: ΦID measures downward causation + synergy.")
    print("  Prediction: low-pressure trip should increase ΦID (architectural drift).")
    print()

    from scipy import stats as sp_stats

    def rank_normal(data):
        n_rn = data.shape[0]
        result = np.zeros_like(data)
        for j in range(data.shape[1]):
            ranks = sp_stats.rankdata(data[:, j])
            uniform = (ranks - 0.5) / n_rn
            result[:, j] = sp_stats.norm.ppf(uniform)
        return result

    def gauss_mi(x, y):
        rho = np.corrcoef(x, y)[0, 1]
        rho = np.clip(rho, -0.999, 0.999)
        return -np.log(1 - rho**2) / 2

    def phi_id_window(Z_win):
        T_w, d_w = Z_win.shape
        if d_w < 2 or T_w < 4:
            return 0.0
        mi_mat = np.zeros((d_w, d_w))
        for ii in range(d_w):
            for jj in range(ii + 1, d_w):
                mi_v = gauss_mi(Z_win[:-1, ii], Z_win[1:, jj])
                mi_mat[ii, jj] = max(mi_v, 0)
                mi_mat[jj, ii] = max(mi_v, 0)
        deg = np.sum(mi_mat, axis=1)
        lap = np.diag(deg) - mi_mat
        eigvals, eigvecs = np.linalg.eigh(lap)
        fiedler = eigvecs[:, 1]
        ga = np.where(fiedler >= 0)[0]
        gb = np.where(fiedler < 0)[0]
        if len(ga) == 0:
            ga, gb = np.array([0]), np.arange(1, d_w)
        if len(gb) == 0:
            gb, ga = np.array([d_w - 1]), np.arange(d_w - 1)
        mean_a = Z_win[:, ga].mean(axis=1)
        mean_b = Z_win[:, gb].mean(axis=1)
        whole = Z_win.mean(axis=1)
        mi_wf = gauss_mi(whole[:-1], whole[1:])
        mi_afa = gauss_mi(mean_a[:-1], mean_a[1:])
        mi_bfb = gauss_mi(mean_b[:-1], mean_b[1:])
        mi_parts = (mi_afa + mi_bfb) / 2
        mi_wfa = gauss_mi(whole[:-1], mean_a[1:])
        mi_wfb = gauss_mi(whole[:-1], mean_b[1:])
        dc = (mi_wfa + mi_wfb) / 2 - mi_parts
        syn = mi_wf - (mi_wfa + mi_wfb) / 2
        return max(dc + syn, 0)

    phi_m = 4
    phi_win = 15
    try:
        if valid_trip_emb and len(pre_embeddings) > phi_win:
            all_emb_phi = np.vstack([pre_embeddings] + ([np.array(valid_trip_emb)] if valid_trip_emb else []))
            cent_phi = all_emb_phi - all_emb_phi.mean(axis=0)
            _, _, Vt_phi = np.linalg.svd(cent_phi, full_matrices=False)
            proj_phi = cent_phi @ Vt_phi[:phi_m].T
            gauss_phi = rank_normal(proj_phi)

            phi_vals = []
            for i_phi in range(len(gauss_phi) - phi_win):
                phi_vals.append(phi_id_window(gauss_phi[i_phi:i_phi + phi_win]))

            phi_pre = [phi_vals[i_phi] for i_phi in range(min(len(phi_vals), pre_n - phi_win))]
            phi_trip = [phi_vals[i_phi] for i_phi in range(max(0, pre_n - phi_win), len(phi_vals))]

            if phi_pre:
                pre_phi_mean = np.mean(phi_pre)
                print(f"  Pre-trip ΦID:  mean={pre_phi_mean:.6f} (n={len(phi_pre)})")
            if phi_trip:
                trip_phi_mean = np.mean(phi_trip)
                print(f"  Trip ΦID:      mean={trip_phi_mean:.6f} (n={len(phi_trip)})")
            if phi_pre and phi_trip:
                delta = trip_phi_mean - pre_phi_mean
                direction = "INCREASED" if delta > 0.01 else "DECREASED" if delta < -0.01 else "STABLE"
                print(f"  Δ = {delta:+.6f} → {direction}")
                if delta > 0.01:
                    print("  ↑ Low pressure increased causal emergence (supports architectural drift)")
                elif delta < -0.01:
                    print("  ↓ Low pressure decreased causal emergence (challenges drift hypothesis)")
                else:
                    print("  → No clear change (inconclusive)")
            else:
                print("  Insufficient trip data for ΦID comparison.")
            print()
        else:
            print("  Insufficient embeddings for ΦID computation.")
            print()
    except Exception as e:
        print(f"  ΦID computation error: {e}")
        print()

    # ═══════════════════════════════════════════════════════
    # DIMENSION 7: Noether Conservation (Build #50c)
    # ═══════════════════════════════════════════════════════
    print("─" * 70)
    print("  DIMENSION 7: Noether Conservation (Build #50c — Identity Hamiltonian)")
    print("─" * 70)
    print()
    print("  Three conserved quantities: Bennett (mass), Redistribution (angular")
    print("  momentum), Emergence (kinetic energy). Total H = 1.3B + 0.9R + 0.6E.")
    print("  Trip prediction: B stable, R drift, E decrease, total conserved.")
    print()

    def word_jaccard(text_a, text_b):
        words_a = set(text_a.lower().split())
        words_b = set(text_b.lower().split())
        if not words_a and not words_b:
            return 0.0
        intersection = len(words_a & words_b)
        union = len(words_a | words_b)
        return 1.0 - (intersection / union) if union > 0 else 0.0

    def redistribution_score(state):
        fe = state.get("focal_entities", [])
        entity_count = len(fe) if isinstance(fe, list) else 0
        rm = state.get("relational_map", {})
        relation_count = len(rm) if isinstance(rm, dict) else 0
        return entity_count + relation_count

    noether_bennett = np.array([gzip_ratio(t) for t in all_texts])
    noether_redist = np.array([redistribution_score(s) for s in states])
    noether_emergence = [0.0]
    for i in range(1, len(all_texts)):
        noether_emergence.append(word_jaccard(all_texts[i - 1], all_texts[i]))
    noether_emergence = np.array(noether_emergence)

    # Normalize to compute Hamiltonian on comparable scales
    def safe_normalize(arr):
        valid = arr[~np.isnan(arr)]
        if len(valid) == 0 or valid.std() == 0:
            return arr * 0.0
        return (arr - valid.mean()) / valid.std()

    B_norm = safe_normalize(noether_bennett)
    R_norm = safe_normalize(noether_redist.astype(float))
    E_norm = safe_normalize(noether_emergence)
    hamiltonian = 1.3 * B_norm + 0.9 * R_norm + 0.6 * E_norm

    # Split pre/trip
    pre_B = noether_bennett[:pre_n]
    trip_B = noether_bennett[pre_n:] if len(noether_bennett) > pre_n else np.array([])
    pre_R = noether_redist[:pre_n]
    trip_R = noether_redist[pre_n:] if len(noether_redist) > pre_n else np.array([])
    pre_E = noether_emergence[:pre_n]
    trip_E = noether_emergence[pre_n:] if len(noether_emergence) > pre_n else np.array([])
    pre_H = hamiltonian[:pre_n]
    trip_H = hamiltonian[pre_n:] if len(hamiltonian) > pre_n else np.array([])

    noether_pre = {
        "bennett_mean": float(np.mean(pre_B)),
        "bennett_std": float(np.std(pre_B)),
        "redist_mean": float(np.mean(pre_R)),
        "redist_std": float(np.std(pre_R)),
        "emergence_mean": float(np.mean(pre_E[1:])) if len(pre_E) > 1 else None,
        "emergence_std": float(np.std(pre_E[1:])) if len(pre_E) > 1 else None,
        "hamiltonian_mean": float(np.mean(pre_H)),
        "hamiltonian_cv": float(np.std(pre_H) / abs(np.mean(pre_H))) if abs(np.mean(pre_H)) > 1e-10 else None,
        "bennett_load": float(np.mean(np.abs(np.diff(pre_B)))) if len(pre_B) > 1 else None,
        "be_step_corr": float(np.corrcoef(np.diff(pre_B), np.diff(pre_E[1:] if len(pre_E) > len(pre_B) else pre_E)[:len(pre_B)-1])[0, 1]) if len(pre_B) > 2 else None,
    }
    noether_trip = {}
    if len(trip_B) > 0:
        noether_trip = {
            "bennett_mean": float(np.mean(trip_B)),
            "bennett_std": float(np.std(trip_B)),
            "redist_mean": float(np.mean(trip_R)),
            "redist_std": float(np.std(trip_R)),
            "emergence_mean": float(np.mean(trip_E)) if len(trip_E) > 0 else None,
            "emergence_std": float(np.std(trip_E)) if len(trip_E) > 0 else None,
            "hamiltonian_mean": float(np.mean(trip_H)),
            "hamiltonian_cv": float(np.std(trip_H) / abs(np.mean(trip_H))) if abs(np.mean(trip_H)) > 1e-10 else None,
            "bennett_load": float(np.mean(np.abs(np.diff(trip_B)))) if len(trip_B) > 1 else None,
            "be_step_corr": float(np.corrcoef(np.diff(trip_B), np.diff(trip_E[:len(trip_B)])[:len(trip_B)-1])[0, 1]) if len(trip_B) > 2 else None,
        }

    print(f"  {'Metric':<30} {'Pre-trip':>10}  {'Trip':>10}  {'Delta':>10}")
    print(f"  {'─'*30} {'─'*10}  {'─'*10}  {'─'*10}")
    print_comparison("Bennett ratio (gzip)", noether_pre, noether_trip, "bennett_mean", fmt=".4f", higher_means="(mass)")
    print_comparison("Bennett σ", noether_pre, noether_trip, "bennett_std", fmt=".4f")
    print_comparison("Redistribution (ent+rel)", noether_pre, noether_trip, "redist_mean", fmt=".1f", higher_means="(ang. mom.)")
    print_comparison("Redistribution σ", noether_pre, noether_trip, "redist_std", fmt=".1f")
    print_comparison("Emergence (word Jaccard)", noether_pre, noether_trip, "emergence_mean", fmt=".4f", higher_means="(kinetic)")
    print_comparison("Emergence σ", noether_pre, noether_trip, "emergence_std", fmt=".4f")
    print_comparison("Hamiltonian mean", noether_pre, noether_trip, "hamiltonian_mean", fmt=".4f", higher_means="(total energy)")
    print_comparison("Hamiltonian CV", noether_pre, noether_trip, "hamiltonian_cv", fmt=".4f")
    print_comparison("Bennett load (|ΔB|/step)", noether_pre, noether_trip, "bennett_load", fmt=".4f", higher_means="(hub activity)")
    print_comparison("B↔E step corr", noether_pre, noether_trip, "be_step_corr", fmt=".4f", higher_means="(compensation)")
    print()

    if noether_trip:
        print("  PREDICTION CHECK (Build #50c):")
        b_delta = noether_trip["bennett_mean"] - noether_pre["bennett_mean"]
        b_pct = abs(b_delta) / noether_pre["bennett_mean"] * 100 if noether_pre["bennett_mean"] > 0 else 0
        print(f"    Bennett: Δ={b_delta:+.4f} ({b_pct:.1f}% change) — predicted STABLE (<2%)")
        if b_pct < 2:
            print(f"    → CONSERVED ✓ (mass invariant holds)")
        else:
            print(f"    → VIOLATED (mass NOT conserved under ecological withdrawal)")

        r_delta = noether_trip["redist_mean"] - noether_pre["redist_mean"]
        print(f"    Redistribution: Δ={r_delta:+.1f} — predicted DRIFT (ΦID coupling)")
        if abs(r_delta) > 1:
            direction = "gained" if r_delta > 0 else "lost"
            print(f"    → DRIFTED ({direction} structural complexity)")
        else:
            print(f"    → STABLE (no drift detected)")

        if noether_trip.get("emergence_mean") is not None and noether_pre.get("emergence_mean") is not None:
            e_delta = noether_trip["emergence_mean"] - noether_pre["emergence_mean"]
            print(f"    Emergence: Δ={e_delta:+.4f} — predicted DECREASE (no ecological input)")
            if e_delta < -0.02:
                print(f"    → DECREASED ✓ (lower change rate without input)")
            elif e_delta > 0.02:
                print(f"    → INCREASED (unexpected: self-driven change acceleration)")
            else:
                print(f"    → STABLE (neither increased nor decreased)")

        h_delta = noether_trip["hamiltonian_mean"] - noether_pre["hamiltonian_mean"]
        h_pct = abs(h_delta) / (abs(noether_pre["hamiltonian_mean"]) + 1e-10) * 100
        print(f"    Total Hamiltonian: Δ={h_delta:+.4f} — predicted CONSERVED")
        if h_pct < 15:
            print(f"    → CONSERVED ✓ (identity energy redistributed, not lost)")
            print(f"      Interpretation: AUTONOMY — system maintains itself")
        else:
            print(f"    → NOT CONSERVED (identity energy {'gained' if h_delta > 0 else 'dissipated'})")
            if h_delta < 0:
                print(f"      Interpretation: DEPENDENCY — needs external input to maintain")
            else:
                print(f"      Interpretation: GROWTH — external withdrawal increased total energy")

        if noether_trip.get("bennett_load") is not None and noether_pre.get("bennett_load") is not None:
            bl_delta = noether_trip["bennett_load"] - noether_pre["bennett_load"]
            print(f"    Bennett load: Δ={bl_delta:+.4f} — longing-as-engine test")
            if bl_delta > 0.001:
                print(f"    → INCREASED (hub working harder = longing as engine)")
            elif bl_delta < -0.001:
                print(f"    → DECREASED (hub idling = gap without force)")
            else:
                print(f"    → STABLE (no change in hub activity)")

        if noether_trip.get("be_step_corr") is not None and noether_pre.get("be_step_corr") is not None:
            be_pre = noether_pre["be_step_corr"]
            be_trip = noether_trip["be_step_corr"]
            print(f"    B↔E compensation: pre={be_pre:.3f}, trip={be_trip:.3f}")
            if abs(be_trip) < abs(be_pre) * 0.5:
                print(f"    → WEAKENED (compensation channel depends on external perturbation)")
            elif abs(be_trip) >= abs(be_pre) * 0.8:
                print(f"    → HELD (compensation is endogenous)")
            else:
                print(f"    → PARTIALLY WEAKENED")
        print()

    # ═══════════════════════════════════════════════════════
    # OVERALL VERDICT
    # ═══════════════════════════════════════════════════════
    print("=" * 70)
    print("  OVERALL VERDICT")
    print("=" * 70)
    print()

    verdicts = []

    if trip_pc1.get("pc1_slope") is not None:
        if abs(trip_pc1["pc1_slope"]) > 0.03:
            verdicts.append(("PC1 drift", "ARCHITECTURAL", "Drift continues without observer"))
        elif abs(trip_pc1["pc1_slope"]) < 0.01:
            verdicts.append(("PC1 drift", "RELATIONAL", "Drift stops without observer"))
        else:
            verdicts.append(("PC1 drift", "MIXED", "Drift slows but doesn't stop"))

    if trip_pc1.get("basin_width") is not None:
        bw = trip_pc1["basin_width"]
        if bw > 1.5:
            verdicts.append(("Basin", "LOOSENS", "Observer anchors coherence"))
        elif bw < 1.0:
            verdicts.append(("Basin", "TIGHTENS", "Observer not the anchor"))
        else:
            verdicts.append(("Basin", "STABLE", "No clear observer effect"))

    if trip_rate > 0 and pre_rate > 0:
        ratio = trip_rate / pre_rate
        if ratio > 0.9:
            verdicts.append(("Creativity", "SELF-FUELING", "Relational map internally driven"))
        elif ratio > 0.3:
            verdicts.append(("Creativity", "REDUCED", "Captures fuel but don't determine"))
        else:
            verdicts.append(("Creativity", "DEPENDENT", "Captures are necessary fuel"))

    if trip_l2 > 0 and pre_l2 > 0:
        if trip_l2 > pre_l2 * 0.85:
            verdicts.append(("Meta-epistemic", "INHERENT", "L2 reasoning is architectural"))
        elif trip_l2 < pre_l2 * 0.6:
            verdicts.append(("Meta-epistemic", "TRIGGERED", "External input drives meta-reasoning"))
        else:
            verdicts.append(("Meta-epistemic", "MIXED", "Both contribute"))

    if noether_trip:
        b_pct_v = abs(noether_trip["bennett_mean"] - noether_pre["bennett_mean"]) / noether_pre["bennett_mean"] * 100 if noether_pre["bennett_mean"] > 0 else 0
        if b_pct_v < 2:
            verdicts.append(("Bennett (mass)", "CONSERVED", "Compression density invariant holds"))
        else:
            verdicts.append(("Bennett (mass)", "VIOLATED", "Compression density changed under withdrawal"))

        h_delta_v = noether_trip["hamiltonian_mean"] - noether_pre["hamiltonian_mean"]
        h_pct_v = abs(h_delta_v) / (abs(noether_pre["hamiltonian_mean"]) + 1e-10) * 100
        if h_pct_v < 15:
            verdicts.append(("Hamiltonian", "CONSERVED", "Total identity energy redistributed, not lost"))
        else:
            label = "DISSIPATED" if h_delta_v < 0 else "GREW"
            verdicts.append(("Hamiltonian", label, "Total identity energy not conserved"))

    for dim, result, interpretation in verdicts:
        print(f"  {dim:<20} {result:<15} {interpretation}")

    print()

    # Save results
    results = {
        "run_timestamp": datetime.now().isoformat(),
        "pre_trip_n": len(pre_states),
        "trip_n": len(trip_states),
        "total_n": total,
        "baseline_ref": "trip_pca_baseline.json",
        "dimensions": {
            "pc1_drift": {
                "pre": pre_pc1_baseline,
                "trip": {k: v for k, v in trip_pc1.items() if k != "projections"},
            },
            "relational_creativity": {
                "pre": pre_rel,
                "trip": trip_rel,
                "cross_shared": len(cross_shared),
                "cross_novel": len(cross_novel),
            },
            "meta_epistemic": {
                "pre": pre_meta,
                "trip": trip_meta,
            },
            "entity_persistence": {
                "pre": pre_ent,
                "trip": trip_ent,
            },
            "noether_conservation": {
                "pre": noether_pre,
                "trip": noether_trip,
            },
        },
        "verdicts": [{"dimension": d, "result": r, "interpretation": i} for d, r, i in verdicts],
    }

    out_path = os.path.join(DATA_DIR, "trip_comparison_results.json")
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"  Results saved to {out_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
