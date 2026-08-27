#!/usr/bin/env python3
"""Quick health data summary from Apple Watch data.

Shows key metrics for the last N days with trend arrows.

Usage:
  python3 health_check.py           # Last 7 days
  python3 health_check.py --days 3  # Last 3 days
  python3 health_check.py --today   # Today only
"""

import argparse
import sqlite3
from datetime import datetime, timedelta

DB = "/mnt/hdd/chronicle-data/processed.db"

KEY_METRICS = [
    ("sleep_hours", "Sleep", "hr", 7.0, 9.0),
    ("sleep_deep", "Deep Sleep", "min", 60, 120),
    ("sleep_rem", "REM Sleep", "min", 90, 120),
    ("resting_heart_rate", "Resting HR", "bpm", 50, 70),
    ("heart_rate_variability", "HRV", "ms", 30, 60),
    ("blood_oxygen_saturation", "SpO2", "%", 95, 100),
    ("respiratory_rate", "Resp Rate", "brpm", 12, 20),
    ("walking_heart_rate_average", "Walking HR", "bpm", 80, 120),
    ("step_count", "Steps", "steps", None, None),
    ("active_energy", "Active Cal", "kcal", None, None),
]


def trend_arrow(current, previous):
    if previous is None or current is None:
        return " "
    diff = current - previous
    pct = diff / previous if previous != 0 else 0
    if abs(pct) < 0.03:
        return "-"
    return "^" if diff > 0 else "v"


def range_flag(value, low, high):
    if low is None or high is None:
        return ""
    if value < low:
        return " LOW"
    if value > high:
        return " HIGH"
    return ""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--today", action="store_true")
    args = parser.parse_args()

    if args.today:
        args.days = 1

    conn = sqlite3.connect(DB)
    now = datetime.now()
    cutoff = (now - timedelta(days=args.days)).timestamp()
    prev_cutoff = (now - timedelta(days=args.days * 2)).timestamp()

    print(f"Health Summary — last {args.days} day(s)")
    print(f"Data through: {now.strftime('%Y-%m-%d %H:%M')}")
    print("=" * 55)

    for metric_key, label, unit, low, high in KEY_METRICS:
        # Current period
        row = conn.execute(
            "SELECT AVG(value), COUNT(*) FROM health_data WHERE metric=? AND timestamp>?",
            (metric_key, cutoff)
        ).fetchone()
        avg, count = row if row else (None, 0)

        if avg is None or count == 0:
            continue

        # For step_count and active_energy, sum per day instead of avg
        if metric_key in ("step_count", "active_energy"):
            total = conn.execute(
                "SELECT SUM(value) FROM health_data WHERE metric=? AND timestamp>?",
                (metric_key, cutoff)
            ).fetchone()[0] or 0
            daily = total / args.days
            # Previous period
            prev_total = conn.execute(
                "SELECT SUM(value) FROM health_data WHERE metric=? AND timestamp>? AND timestamp<=?",
                (metric_key, prev_cutoff, cutoff)
            ).fetchone()[0]
            prev_daily = prev_total / args.days if prev_total else None
            arrow = trend_arrow(daily, prev_daily)
            print(f"  {label:18} {daily:8.0f} {unit:5} /day {arrow}")
            continue

        # Previous period for trend
        prev_row = conn.execute(
            "SELECT AVG(value) FROM health_data WHERE metric=? AND timestamp>? AND timestamp<=?",
            (metric_key, prev_cutoff, cutoff)
        ).fetchone()
        prev_avg = prev_row[0] if prev_row else None

        arrow = trend_arrow(avg, prev_avg)
        flag = range_flag(avg, low, high)

        print(f"  {label:18} {avg:8.1f} {unit:5} {arrow}{flag}")

    # Last data timestamp
    last = conn.execute("SELECT MAX(timestamp) FROM health_data").fetchone()[0]
    if last:
        age_hrs = (now.timestamp() - last) / 3600
        freshness = f"{age_hrs:.1f}h ago"
        if age_hrs > 12:
            freshness += " (STALE)"
        print(f"\n  Latest data: {freshness}")

    conn.close()


if __name__ == "__main__":
    main()
