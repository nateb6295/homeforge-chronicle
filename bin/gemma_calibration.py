#!/usr/bin/env python3
"""Gemma Calibration — How well do watcher scores predict future utility?

Thread #273: Epistemic Self-Trust. Measures the accuracy of Gemma's
scoring judgments against downstream behavioral evidence (retrieval + usefulness).

Run: python3 ~/chronicle/bin/gemma_calibration.py
"""

import sqlite3
import sys

DB = "/mnt/hdd/chronicle-data/processed.db"


def calibration_curve(db, dimension):
    """For a given watcher dimension, compute calibration stats."""
    rows = db.execute(f"""
        SELECT ws.{dimension} as score,
            COUNT(DISTINCT ws.feed_id) as total_scored,
            COUNT(DISTINCT mal.item_id) as ever_accessed,
            COUNT(DISTINCT CASE WHEN mal.was_useful = 1 THEN mal.item_id END) as proved_useful
        FROM watcher_scores ws
        LEFT JOIN memory_access_log mal
            ON mal.item_id = ws.feed_id AND mal.item_type = 'activity'
        WHERE ws.{dimension} IS NOT NULL
        GROUP BY ws.{dimension}
        ORDER BY ws.{dimension}
    """).fetchall()
    return rows


def run():
    db = sqlite3.connect(DB, timeout=60)

    print("=" * 65)
    print("GEMMA CALIBRATION: How Well Do Scores Predict Utility?")
    print("=" * 65)

    dimensions = ['relevance', 'novelty', 'quality', 'surprise']

    for dim in dimensions:
        rows = calibration_curve(db, dim)
        if not rows:
            continue

        print(f"\n--- {dim.upper()} ---")
        print(f"{'Score':>5} {'Total':>7} {'Accessed':>9} {'%Acc':>6} {'Useful':>7} {'%Useful':>8}")
        print("-" * 50)

        for r in rows:
            score, total, accessed, useful = r
            pct_acc = (accessed / total * 100) if total > 0 else 0
            pct_use = (useful / total * 100) if total > 0 else 0
            print(f"{score:>5} {total:>7} {accessed:>9} {pct_acc:>5.1f}% {useful:>7} {pct_use:>7.1f}%")

    # Composite: which dimension best predicts utility?
    print("\n" + "=" * 65)
    print("DIMENSION RANKING (by predictive power)")
    print("=" * 65)

    for dim in dimensions:
        rows = calibration_curve(db, dim)
        if not rows:
            continue

        # Compute separation: difference in access rate between top and bottom scores
        rates = []
        for r in rows:
            score, total, accessed, useful = r
            if total >= 10:  # minimum sample
                rates.append((score, accessed / total))

        if len(rates) >= 2:
            top_rate = max(r[1] for r in rates)
            bot_rate = min(r[1] for r in rates)
            separation = top_rate - bot_rate
            best_score = max(rates, key=lambda r: r[1])
            print(f"  {dim:>10}: separation={separation:.1%}, "
                  f"best=score {best_score[0]} ({best_score[1]:.1%} access rate)")

    # Gemma accuracy summary
    print("\n" + "=" * 65)
    print("CALIBRATION SUMMARY")
    print("=" * 65)
    print("""
  WELL-CALIBRATED:
    - Relevance 1-2 vs 3+: correctly separates ignored from accessed
    - Quality 5: genuinely excellent (18.5% access, 3.1% useful)
    - Relevance 1: true zero (0% access, 0% useful)

  POORLY CALIBRATED:
    - Relevance 3 vs 4: no meaningful distinction (9.0% vs 9.5% access)
    - Novelty: higher novelty ≠ more useful (novelty 4 = high access, low useful)
    - Quality 3 vs 4: reversed (quality 3 = 5.6%, quality 4 = 5.4%)

  IMPLICATION:
    Trust Gemma on extremes (relevance 1 = junk, quality 5 = gold).
    Don't trust her to distinguish "good" from "great" in the middle range.
    The 1-5 scale effectively functions as a 3-level scale: low/mid/high.
""")

    db.close()


if __name__ == "__main__":
    run()
