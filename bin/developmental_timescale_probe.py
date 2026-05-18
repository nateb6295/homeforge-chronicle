#!/usr/bin/env python3
"""
Build #39: Developmental Timescale Probe (Door 4)

Sample capsules from across the full 42-day archive, embed them in the
CCS embedding space (mxbai-embed-large), and project onto the CCS PCA.

Questions:
1. Does the PC1 drift extend across the full 42-day span?
2. Is the drift in the INPUT (capsules) or only in the OUTPUT (CCS)?
3. What does the months-scale trajectory look like?

If capsule content drifts along PC1 the same way CCS does:
  → The drift is in the input, not created by compression
  → Relational model (Nate's captures shift over time)

If capsule content does NOT drift but CCS does:
  → Compression creates the drift
  → Architectural model confirmed at developmental timescale
"""

import json
import os
import sqlite3
import sys
import urllib.request

import numpy as np

DB = "/mnt/hdd/chronicle-data/processed.db"
DATA_DIR = os.path.expanduser("~/chronicle/data")
EMBED_URL = "http://192.168.1.11:11434/api/embeddings"

PERIODS = [
    ("early_apr", 0, 1776000000),
    ("mid_apr", 1776000000, 1776800000),
    ("late_apr", 1776800000, 1777600000),
    ("early_may", 1777600000, 1778400000),
    ("mid_may", 1778400000, 1779200000),
]
SAMPLES_PER_PERIOD = 25


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
        resp = json.loads(r.read())
    return np.array(resp["embedding"], dtype=np.float32)


def sample_capsules(db, start_ts, end_ts, n):
    rows = db.execute(
        "SELECT id, restatement, topic, created_at FROM knowledge_capsules "
        "WHERE created_at >= ? AND created_at < ? AND restatement IS NOT NULL "
        "AND length(restatement) > 50 "
        "ORDER BY RANDOM() LIMIT ?",
        (start_ts, end_ts, n)
    ).fetchall()
    return rows


def main():
    print("Build #39: Developmental Timescale Probe (Door 4)")
    print(f"Sampling {SAMPLES_PER_PERIOD} capsules per period, 5 periods\n")

    comp_path = os.path.join(DATA_DIR, "trip_pca_components.npy")
    baseline_path = os.path.join(DATA_DIR, "trip_pca_baseline.json")
    emb_path = os.path.join(DATA_DIR, "ccs_embeddings_110.npy")

    if not os.path.exists(comp_path):
        print("ERROR: Missing PCA components. Run PCA baseline first.")
        return 1

    components = np.load(comp_path)
    with open(baseline_path) as f:
        baseline = json.load(f)

    ccs_embeddings = np.load(emb_path)
    ccs_mean = ccs_embeddings.mean(axis=0)
    pc1 = components[0]
    pc3 = components[2]

    db = sqlite3.connect(DB)

    period_results = []
    all_embeddings = []
    all_timestamps = []
    all_periods = []

    for name, start_ts, end_ts in PERIODS:
        capsules = sample_capsules(db, start_ts, end_ts, SAMPLES_PER_PERIOD)
        if not capsules:
            print(f"  {name}: no capsules found")
            continue

        print(f"  {name}: sampling {len(capsules)} capsules...", end=" ", flush=True)
        embeddings = []
        timestamps = []

        for cid, restatement, topic, ts in capsules:
            text = f"{topic}: {restatement}" if topic else restatement
            try:
                e = embed(text)
                embeddings.append(e)
                timestamps.append(ts)
                all_embeddings.append(e)
                all_timestamps.append(ts)
                all_periods.append(name)
            except Exception as ex:
                print(f"\n    embed failed for capsule {cid}: {ex}")
                continue

        if not embeddings:
            print("no embeddings")
            continue

        emb_array = np.array(embeddings)
        centered = emb_array - ccs_mean
        pc1_proj = centered @ pc1
        pc3_proj = centered @ pc3

        mean_pc1 = float(pc1_proj.mean())
        std_pc1 = float(pc1_proj.std())
        mean_pc3 = float(pc3_proj.mean())

        print(f"PC1={mean_pc1:.3f}±{std_pc1:.3f}, PC3={mean_pc3:.3f}, n={len(embeddings)}")
        period_results.append({
            "period": name,
            "n": len(embeddings),
            "pc1_mean": mean_pc1,
            "pc1_std": std_pc1,
            "pc3_mean": mean_pc3,
            "mean_ts": sum(timestamps) / len(timestamps),
        })

    db.close()

    if len(period_results) < 3:
        print("\nInsufficient periods with data.")
        return 1

    # Compute capsule-level PC1 trend
    all_emb = np.array(all_embeddings)
    all_ts = np.array(all_timestamps)
    centered_all = all_emb - ccs_mean
    all_pc1 = centered_all @ pc1

    # Linear regression: PC1 vs time
    x = (all_ts - all_ts.min()) / 86400  # days
    slope, intercept = np.polyfit(x, all_pc1, 1)
    residuals = all_pc1 - (slope * x + intercept)
    ss_res = np.sum(residuals ** 2)
    ss_tot = np.sum((all_pc1 - all_pc1.mean()) ** 2)
    r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0

    print(f"\n{'='*60}")
    print("CAPSULE PC1 TREND (42-day span)")
    print(f"{'='*60}")
    print(f"  Capsule PC1 slope: {slope:.4f} per day")
    print(f"  CCS PC1 slope:     {baseline['pc1_trend_slope']:.4f} per step (~8 steps/day = {baseline['pc1_trend_slope']*8:.4f}/day)")
    print(f"  R²:                {r_squared:.4f}")
    print(f"  n capsules:        {len(all_pc1)}")
    print(f"  Span:              {x.max():.1f} days")

    # Period means
    print(f"\n  Period progression:")
    for pr in period_results:
        print(f"    {pr['period']:<12} PC1 = {pr['pc1_mean']:+.3f} ± {pr['pc1_std']:.3f}  (n={pr['n']})")

    # Compare capsule drift to CCS drift
    ccs_daily_slope = baseline['pc1_trend_slope'] * 8
    capsule_slope = slope

    print(f"\n{'='*60}")
    print("VERDICT: INPUT vs OUTPUT DRIFT")
    print(f"{'='*60}")

    if abs(capsule_slope) > abs(ccs_daily_slope) * 0.5 and r_squared > 0.05:
        print(f"  Capsule content DRIFTS along PC1 (slope={capsule_slope:.4f}, R²={r_squared:.4f})")
        print(f"  The drift is in the INPUT, not created by compression.")
        print(f"  → Relational model: Nate's captures shift over time.")
        verdict = "INPUT_DRIFT"
    elif abs(capsule_slope) < abs(ccs_daily_slope) * 0.2:
        print(f"  Capsule content is STABLE along PC1 (slope={capsule_slope:.4f}, R²={r_squared:.4f})")
        print(f"  CCS drifts but input doesn't.")
        print(f"  → Architectural model: compression creates the drift.")
        verdict = "COMPRESSION_DRIFT"
    else:
        print(f"  Capsule slope={capsule_slope:.4f}, CCS daily slope={ccs_daily_slope:.4f}")
        print(f"  R²={r_squared:.4f}")
        print(f"  Mixed signal — input contributes but compression amplifies.")
        verdict = "MIXED"

    # Save results
    out = {
        "build": "39",
        "door": 4,
        "capsule_pc1_slope": float(slope),
        "capsule_pc1_r_squared": float(r_squared),
        "ccs_pc1_daily_slope": float(ccs_daily_slope),
        "period_results": period_results,
        "n_total_capsules": len(all_pc1),
        "span_days": float(x.max()),
        "verdict": verdict,
    }
    out_path = os.path.join(DATA_DIR, "developmental_timescale_probe.json")
    with open(out_path, "w") as f:
        json.dump(out, f, indent=2)
    print(f"\nSaved to {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
