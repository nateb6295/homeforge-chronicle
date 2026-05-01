#!/usr/bin/env python3
"""Crossref Regime Detector — Track distribution shifts in connection similarity.

Ada's idea: "Track the divergence between predicted and observed stress vectors;
a rapid increase in residuals signals the scaffold's curvature is no longer
negligible." Implemented as sliding-window statistics on crossref similarity.

When the similarity distribution shifts — variance spikes, heavy tails emerge,
or the mean drifts — it signals domain bridges forming faster than baseline.
This IS the grounding signal for Thread #295's map-territory thesis.

Usage:
    python3 crossref_regime.py snapshot         # current distribution stats
    python3 crossref_regime.py track            # store snapshot + detect shift
    python3 crossref_regime.py history          # past regime snapshots
    python3 crossref_regime.py compare 6 24     # compare 6h vs 24h windows

Build #86. Ada's sliding-window variance idea, made concrete.
"""

import os
import sys
import json
import math
import sqlite3
import time
from datetime import datetime

DB_PATH = "/mnt/hdd/chronicle-data/processed.db"


def _get_similarities(hours: int = 24) -> list:
    """Get non-zero crossref similarities from the last N hours."""
    db = sqlite3.connect(DB_PATH)
    rows = db.execute(
        "SELECT similarity FROM crossref_connections "
        "WHERE similarity > 0 AND created_at > strftime('%s','now',? || ' hours')",
        (f"-{hours}",)
    ).fetchall()
    db.close()
    return [r[0] for r in rows]


def _compute_stats(values: list) -> dict:
    """Compute distribution statistics."""
    if not values:
        return {"count": 0, "mean": 0, "variance": 0, "skewness": 0,
                "tail_weight": 0, "high_count": 0, "low_count": 0}

    n = len(values)
    mean = sum(values) / n
    variance = sum((x - mean) ** 2 for x in values) / n if n > 1 else 0
    std = math.sqrt(variance) if variance > 0 else 0

    # Skewness (Fisher's)
    skewness = 0
    if std > 0 and n > 2:
        skewness = sum(((x - mean) / std) ** 3 for x in values) / n

    # Tail weight: fraction of values above 0.75 (strong connections)
    high_count = sum(1 for x in values if x >= 0.75)
    low_count = sum(1 for x in values if x < 0.55)
    tail_weight = high_count / n if n > 0 else 0

    return {
        "count": n,
        "mean": round(mean, 4),
        "variance": round(variance, 6),
        "std": round(std, 4),
        "skewness": round(skewness, 4),
        "tail_weight": round(tail_weight, 4),
        "high_count": high_count,
        "low_count": low_count,
        "min": round(min(values), 4),
        "max": round(max(values), 4),
        "median": round(sorted(values)[n // 2], 4),
    }


def _detect_regime_shift(recent: dict, baseline: dict) -> list:
    """Detect significant distribution shifts between windows."""
    alerts = []

    if baseline["count"] < 10 or recent["count"] < 5:
        return alerts

    # Variance spike: recent variance > 2x baseline
    if baseline["variance"] > 0:
        var_ratio = recent["variance"] / baseline["variance"]
        if var_ratio > 2.0:
            alerts.append({
                "type": "variance_spike",
                "severity": "high" if var_ratio > 3.0 else "medium",
                "detail": f"Variance {var_ratio:.1f}x baseline ({recent['variance']:.4f} vs {baseline['variance']:.4f})",
            })
        elif var_ratio < 0.5:
            alerts.append({
                "type": "variance_collapse",
                "severity": "medium",
                "detail": f"Variance collapsed to {var_ratio:.1f}x baseline — distribution narrowing",
            })

    # Mean drift: recent mean shifted > 1 std from baseline mean
    if baseline["std"] > 0:
        mean_drift = abs(recent["mean"] - baseline["mean"]) / baseline["std"]
        if mean_drift > 1.0:
            direction = "up" if recent["mean"] > baseline["mean"] else "down"
            alerts.append({
                "type": "mean_drift",
                "severity": "high" if mean_drift > 2.0 else "medium",
                "detail": f"Mean drifted {direction} by {mean_drift:.1f}σ ({recent['mean']:.3f} vs {baseline['mean']:.3f})",
            })

    # Tail emergence: recent tail_weight > 1.5x baseline
    if baseline["tail_weight"] > 0:
        tail_ratio = recent["tail_weight"] / baseline["tail_weight"]
        if tail_ratio > 1.5:
            alerts.append({
                "type": "heavy_tail",
                "severity": "high" if tail_ratio > 2.0 else "medium",
                "detail": f"Heavy tail emerging: {recent['tail_weight']:.1%} vs baseline {baseline['tail_weight']:.1%} above 0.75",
            })

    # Skewness flip
    if baseline["skewness"] != 0 and recent["skewness"] != 0:
        if (baseline["skewness"] > 0) != (recent["skewness"] > 0):
            alerts.append({
                "type": "skew_flip",
                "severity": "medium",
                "detail": f"Skewness flipped: {recent['skewness']:.3f} vs baseline {baseline['skewness']:.3f}",
            })

    return alerts


def snapshot():
    """Print current distribution statistics across time windows."""
    windows = [6, 24, 72, 168]  # 6h, 24h, 3d, 7d
    print("\nCrossref Similarity Distribution:")
    print(f"{'Window':>8} {'Count':>6} {'Mean':>7} {'StdDev':>7} {'Skew':>7} {'Tail%':>7} {'Range':>15}")
    print("-" * 65)

    for hours in windows:
        values = _get_similarities(hours)
        stats = _compute_stats(values)
        if stats["count"] > 0:
            print(f"  {hours:>4}h  {stats['count']:>5}  {stats['mean']:>6.3f}  {stats['std']:>6.3f}  "
                  f"{stats['skewness']:>6.3f}  {stats['tail_weight']:>5.1%}  "
                  f"[{stats['min']:.2f}-{stats['max']:.2f}]")
        else:
            print(f"  {hours:>4}h      0   (no data)")


def track():
    """Store snapshot and detect regime shifts."""
    # Recent window (6h) vs baseline (7d)
    recent_vals = _get_similarities(6)
    baseline_vals = _get_similarities(168)
    recent = _compute_stats(recent_vals)
    baseline = _compute_stats(baseline_vals)

    # Store snapshot
    db = sqlite3.connect(DB_PATH)
    db.execute("""
        CREATE TABLE IF NOT EXISTS regime_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            window_hours INTEGER NOT NULL,
            stats_json TEXT NOT NULL,
            alerts_json TEXT,
            created_at INTEGER NOT NULL
        )
    """)

    alerts = _detect_regime_shift(recent, baseline)

    db.execute(
        "INSERT INTO regime_snapshots (window_hours, stats_json, alerts_json, created_at) "
        "VALUES (?, ?, ?, ?)",
        (6, json.dumps(recent), json.dumps(alerts), int(time.time()))
    )
    db.commit()
    db.close()

    # Print results
    print(f"\nRegime Snapshot — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  Recent (6h): {recent['count']} connections, mean={recent['mean']:.3f}, "
          f"std={recent['std']:.3f}, tail={recent['tail_weight']:.1%}")
    print(f"  Baseline (7d): {baseline['count']} connections, mean={baseline['mean']:.3f}, "
          f"std={baseline['std']:.3f}, tail={baseline['tail_weight']:.1%}")

    if alerts:
        print(f"\n  ⚠ {len(alerts)} regime shift(s) detected:")
        for a in alerts:
            severity_icon = "🔴" if a["severity"] == "high" else "🟡"
            print(f"    {severity_icon} [{a['type']}] {a['detail']}")
    else:
        print("\n  ✓ No regime shift detected. Distribution stable.")


def compare(hours_a: int, hours_b: int):
    """Compare two time windows."""
    vals_a = _get_similarities(hours_a)
    vals_b = _get_similarities(hours_b)
    stats_a = _compute_stats(vals_a)
    stats_b = _compute_stats(vals_b)

    print(f"\nComparing {hours_a}h vs {hours_b}h windows:")
    print(f"  {'Metric':>12} {'Window A':>10} {'Window B':>10} {'Delta':>10}")
    print("  " + "-" * 45)

    for key in ["count", "mean", "std", "skewness", "tail_weight"]:
        a = stats_a[key]
        b = stats_b[key]
        delta = a - b
        fmt = ".3f" if key != "count" else "d"
        if key == "tail_weight":
            print(f"  {key:>12} {a:>9.1%} {b:>9.1%} {delta:>+9.1%}")
        elif key == "count":
            print(f"  {key:>12} {a:>10d} {b:>10d} {delta:>+10d}")
        else:
            print(f"  {key:>12} {a:>10{fmt}} {b:>10{fmt}} {delta:>+10{fmt}}")

    alerts = _detect_regime_shift(stats_a, stats_b)
    if alerts:
        print(f"\n  ⚠ {len(alerts)} shift(s):")
        for a in alerts:
            print(f"    [{a['severity']}] {a['type']}: {a['detail']}")
    else:
        print("\n  ✓ No significant shifts.")


def history(limit: int = 10):
    """Show past regime snapshots."""
    db = sqlite3.connect(DB_PATH)
    try:
        rows = db.execute(
            "SELECT stats_json, alerts_json, created_at FROM regime_snapshots "
            "ORDER BY created_at DESC LIMIT ?",
            (limit,)
        ).fetchall()
    except sqlite3.OperationalError:
        print("No regime history yet. Run 'track' first.")
        return
    finally:
        db.close()

    if not rows:
        print("No regime history yet.")
        return

    print(f"\nRegime History ({len(rows)} snapshots):")
    for stats_json, alerts_json, ts in rows:
        stats = json.loads(stats_json)
        alerts = json.loads(alerts_json) if alerts_json else []
        ts_str = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
        alert_str = f" ⚠ {len(alerts)} shift(s)" if alerts else " ✓"
        print(f"  {ts_str}  n={stats['count']:>4}  μ={stats['mean']:.3f}  "
              f"σ={stats['std']:.3f}  tail={stats['tail_weight']:.1%}{alert_str}")


# ═══════════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  crossref_regime.py snapshot         # current distribution stats")
        print("  crossref_regime.py track             # store snapshot + detect shift")
        print("  crossref_regime.py history            # past regime snapshots")
        print("  crossref_regime.py compare 6 24       # compare 6h vs 24h windows")
        sys.exit(0)

    cmd = sys.argv[1]

    if cmd == "snapshot":
        snapshot()
    elif cmd == "track":
        track()
    elif cmd == "compare":
        if len(sys.argv) < 4:
            print("Usage: crossref_regime.py compare HOURS_A HOURS_B")
            sys.exit(1)
        compare(int(sys.argv[2]), int(sys.argv[3]))
    elif cmd == "history":
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else 10
        history(limit)
    else:
        print(f"Unknown command: {cmd}")
