#!/usr/bin/env python3
"""
vendi_score.py — behavioral diversity measurement.

Inspired by Tim Kellogg's Strix collapse detection (Vendi Score 0.94 → 0.38).
Measures how many distinct behavioral modes are active in a time window.

Uses Shannon entropy of (source, activity_type) distribution from activity_feed.
Vendi Score = exp(entropy) = effective number of modes.

Usage:
    python3 vendi_score.py              # current 2-hour window
    python3 vendi_score.py --hours 6    # custom window
    python3 vendi_score.py --log        # compute and log to mesh_pulses
    python3 vendi_score.py --history    # show recent logged scores

Healthy range: 5-10 effective modes (diversity ratio 40-70%).
Collapse warning: <4 effective modes or diversity <30%.
"""
import argparse
import math
import sqlite3
import time
from collections import Counter

DB = "/mnt/hdd/chronicle-data/processed.db"


def compute_vendi(hours: float = 2.0) -> dict:
    """Compute Vendi Score proxy for the given time window."""
    db = sqlite3.connect(DB)
    cutoff = int(time.time()) - int(hours * 3600)

    rows = db.execute(
        "SELECT source, activity_type FROM activity_feed WHERE created_at > ?",
        (cutoff,),
    ).fetchall()

    if not rows:
        db.close()
        return {"total": 0, "patterns": 0, "entropy": 0, "vendi": 0,
                "diversity_ratio": 0, "status": "NO_DATA", "top": []}

    patterns = Counter()
    for src, atype in rows:
        patterns[(src, atype)] += 1

    total = sum(patterns.values())
    n_patterns = len(patterns)

    probs = [c / total for c in patterns.values()]
    entropy = -sum(p * math.log(p) for p in probs if p > 0)
    vendi = math.exp(entropy)
    diversity = vendi / n_patterns if n_patterns > 0 else 0

    # Status based on thresholds
    if vendi < 4:
        status = "COLLAPSED"
    elif vendi < 5:
        status = "LOW"
    elif vendi < 10:
        status = "HEALTHY"
    else:
        status = "HIGH"

    top = [(f"{src}:{atype}", count, round(100 * count / total, 1))
           for (src, atype), count in patterns.most_common(8)]

    db.close()
    return {
        "total": total,
        "patterns": n_patterns,
        "entropy": round(entropy, 3),
        "vendi": round(vendi, 2),
        "diversity_ratio": round(diversity, 3),
        "status": status,
        "top": top,
    }


def log_score(result: dict):
    """Log the Vendi Score to activity_feed for historical tracking."""
    db = sqlite3.connect(DB)
    db.execute(
        "INSERT INTO activity_feed (source, activity_type, content, created_at) "
        "VALUES (?, ?, ?, ?)",
        ("opus:vendi", "measurement",
         f"Vendi={result['vendi']:.2f} modes={result['patterns']} "
         f"entropy={result['entropy']:.3f} diversity={result['diversity_ratio']:.1%} "
         f"status={result['status']}",
         int(time.time())),
    )
    db.commit()
    db.close()


def show_history():
    """Show recent Vendi Score measurements."""
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT content, datetime(created_at, 'unixepoch', 'localtime') "
        "FROM activity_feed WHERE source='opus:vendi' "
        "ORDER BY created_at DESC LIMIT 20"
    ).fetchall()
    db.close()

    if not rows:
        print("No Vendi Score history yet.")
        return

    print("Recent Vendi Score measurements:")
    for content, ts in rows:
        print(f"  [{ts}] {content}")


def main():
    parser = argparse.ArgumentParser(description="Behavioral diversity measurement")
    parser.add_argument("--hours", type=float, default=2.0, help="Window size in hours")
    parser.add_argument("--log", action="store_true", help="Log score to activity_feed")
    parser.add_argument("--history", action="store_true", help="Show recent scores")
    args = parser.parse_args()

    if args.history:
        show_history()
        return

    result = compute_vendi(args.hours)

    if result["total"] == 0:
        print(f"No activity in last {args.hours}h")
        return

    print(f"Vendi Score: {result['vendi']:.2f} effective modes  [{result['status']}]")
    print(f"  {result['total']} entries, {result['patterns']} distinct patterns")
    print(f"  Entropy: {result['entropy']:.3f}")
    print(f"  Diversity: {result['diversity_ratio']:.1%}")
    if result['top']:
        print("  Top patterns:")
        for name, count, pct in result['top']:
            print(f"    {name}: {count} ({pct}%)")

    if args.log:
        log_score(result)
        print(f"\n  → Logged to activity_feed")


if __name__ == "__main__":
    main()
