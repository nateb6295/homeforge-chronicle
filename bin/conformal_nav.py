#!/usr/bin/env python3
"""
Conformal prediction sets for CCS navigation quality.

Instead of a point estimate (nav_mean = 0.66), produces a prediction
interval with guaranteed coverage:
  "With 90% probability, the next nav_mean will be in [0.60, 0.72]"

Uses split conformal prediction on the calibration_nav_trials history.
Inspired by arxiv:2604.14621 (Differentially Private Conformal Prediction)
but without the DP component — we don't need privacy here, just calibration.

The prediction set itself IS the calibration-beats-effort thesis instantiated:
  - Effort approach: run many trials, report the mean
  - Calibration approach: use the distribution to give a coverage-guaranteed set

Usage:
  conformal_nav.py                 # overall prediction set
  conformal_nav.py --per-question  # per-question prediction sets
  conformal_nav.py --coverage 0.95 # 95% coverage (default 90%)
  conformal_nav.py --regime        # include regime-specific sets
"""
import argparse
import json
import math
import sqlite3
import sys
from pathlib import Path

DB = "/mnt/hdd/chronicle-data/processed.db"


def get_nav_trials():
    """Load all calibration nav trials."""
    db = sqlite3.connect(DB)
    db.row_factory = sqlite3.Row
    rows = db.execute(
        "SELECT run_at, nav_mean, per_question_json FROM calibration_nav_trials "
        "ORDER BY run_at"
    ).fetchall()
    db.close()
    return [dict(r) for r in rows]


def get_cross_model_trials():
    """Load cross-model nav trials keyed by model."""
    db = sqlite3.connect(DB)
    db.row_factory = sqlite3.Row
    rows = db.execute(
        "SELECT run_at, model, nav_mean, per_question_json FROM cross_model_nav_trials "
        "ORDER BY run_at"
    ).fetchall()
    db.close()
    by_model = {}
    for r in rows:
        by_model.setdefault(r["model"], []).append(dict(r))
    return by_model


def conformal_quantile(scores, alpha):
    """Compute the conformal quantile for coverage 1-alpha.

    For n calibration scores, the conformal quantile is the
    ceil((n+1)(1-alpha))/n-th empirical quantile. This gives
    finite-sample coverage >= 1-alpha.
    """
    n = len(scores)
    if n == 0:
        return 0.0, 1.0  # no data, return full range

    scores_sorted = sorted(scores)
    # Conformal prediction uses the ceil((n+1)(1-alpha))-th order statistic
    # for the UPPER bound of nonconformity scores.
    # For prediction INTERVALS on the scores themselves:
    # Lower bound: alpha/2 quantile
    # Upper bound: 1-alpha/2 quantile

    def quantile_idx(p):
        """Index for the p-th quantile with conformal correction."""
        k = math.ceil((n + 1) * p) - 1
        return max(0, min(n - 1, k))

    lo_idx = quantile_idx(alpha / 2)
    hi_idx = quantile_idx(1 - alpha / 2)

    return scores_sorted[lo_idx], scores_sorted[hi_idx]


def prediction_set(scores, alpha):
    """Build a conformal prediction set from calibration scores."""
    lo, hi = conformal_quantile(scores, alpha)
    n = len(scores)
    mean = sum(scores) / n if n else 0
    std = (sum((s - mean) ** 2 for s in scores) / n) ** 0.5 if n else 0

    return {
        "n": n,
        "coverage": round(1 - alpha, 3),
        "interval": [round(lo, 4), round(hi, 4)],
        "width": round(hi - lo, 4),
        "mean": round(mean, 4),
        "std": round(std, 4),
        "empirical_coverage": round(
            sum(1 for s in scores if lo <= s <= hi) / n, 3
        ) if n else 0,
    }


def main():
    parser = argparse.ArgumentParser(description="Conformal prediction sets for nav scores")
    parser.add_argument("--coverage", type=float, default=0.90, help="Coverage level (default 0.90)")
    parser.add_argument("--per-question", action="store_true", help="Per-question prediction sets")
    parser.add_argument("--regime", action="store_true", help="Include cross-model regime sets")
    args = parser.parse_args()

    alpha = 1 - args.coverage
    trials = get_nav_trials()
    if not trials:
        print("No calibration nav trials found.")
        sys.exit(1)

    # Overall prediction set
    scores = [t["nav_mean"] for t in trials]
    ps = prediction_set(scores, alpha)

    print(f"{'=' * 60}")
    print(f"CONFORMAL PREDICTION SET — CCS Navigation Quality")
    print(f"{'=' * 60}")
    print(f"  Calibration trials: {ps['n']}")
    print(f"  Coverage guarantee: {ps['coverage']:.0%}")
    print(f"  Prediction interval: [{ps['interval'][0]:.4f}, {ps['interval'][1]:.4f}]")
    print(f"  Width: {ps['width']:.4f}")
    print(f"  Point estimate: {ps['mean']:.4f} +/- {ps['std']:.4f}")
    print(f"  Empirical coverage: {ps['empirical_coverage']:.0%}")
    print()

    # Interpretation
    width = ps["width"]
    if width < 0.10:
        print(f"  Tight set (width < 0.10) — nav quality is stable and predictable.")
    elif width < 0.20:
        print(f"  Moderate set (width 0.10-0.20) — some variance, CCS freshness matters.")
    else:
        print(f"  Wide set (width > 0.20) — high variance, investigate CCS staleness or backend instability.")

    if args.per_question:
        print(f"\n{'=' * 60}")
        print(f"PER-QUESTION PREDICTION SETS")
        print(f"{'=' * 60}")

        # Collect per-question scores across all trials
        q_scores = {}
        for t in trials:
            pq = json.loads(t["per_question_json"])
            for q, v in pq.items():
                score = v if isinstance(v, (int, float)) else v.get("nav_score", 0)
                q_scores.setdefault(q, []).append(score)

        for q, sc in q_scores.items():
            qps = prediction_set(sc, alpha)
            short_q = q[:55]
            print(f"\n  {short_q}...")
            print(f"    [{qps['interval'][0]:.4f}, {qps['interval'][1]:.4f}]  "
                  f"width={qps['width']:.4f}  mean={qps['mean']:.4f}")
            if qps["width"] > ps["width"] * 1.5:
                print(f"    ⚠ This question has unusually high variance")

    if args.regime:
        print(f"\n{'=' * 60}")
        print(f"CROSS-MODEL REGIME SETS")
        print(f"{'=' * 60}")

        by_model = get_cross_model_trials()
        for model, model_trials in sorted(by_model.items()):
            model_scores = [t["nav_mean"] for t in model_trials]
            n = len(model_scores)
            if n < 3:
                mean = sum(model_scores) / n
                print(f"\n  {model} (n={n}, too few for conformal set)")
                print(f"    Point estimate: {mean:.4f}")
                print(f"    Range: [{min(model_scores):.4f}, {max(model_scores):.4f}]")
            else:
                mps = prediction_set(model_scores, alpha)
                print(f"\n  {model} (n={mps['n']})")
                print(f"    [{mps['interval'][0]:.4f}, {mps['interval'][1]:.4f}]  "
                      f"width={mps['width']:.4f}  mean={mps['mean']:.4f}")

        # Gap analysis vs default backend
        if trials and by_model:
            default_mean = ps["mean"]
            print(f"\n  Gap from default backend (mean={default_mean:.4f}):")
            for model, model_trials in sorted(by_model.items()):
                model_mean = sum(t["nav_mean"] for t in model_trials) / len(model_trials)
                gap = default_mean - model_mean
                print(f"    {model}: gap={gap:+.4f} ({gap/default_mean*100:+.1f}%)")

    print(f"\n{'=' * 60}")


if __name__ == "__main__":
    main()
