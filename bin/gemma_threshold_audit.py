#!/usr/bin/env python3
"""Gemma threshold audit — histogram the adjusted_score distribution vs routes.

Purpose: see where items sit relative to THRESH_DEDUP=0.15 / THRESH_ASSESS=0.20
and how often Darby (the gate) disagrees with score-alone routing. Lets us answer:

  - Are the thresholds in the right place, or is most traffic squashed against
    a single cutoff?
  - When score >= 0.20 and goes to Darby, what's her noise/signal/alarm split?
  - Are there high-score items Darby calls ignore? Low-score dissent-promotes?

Run after the schema migration has accumulated a day+ of data.

Usage:
    gemma_threshold_audit.py                    # last 24h
    gemma_threshold_audit.py --window-hr 168    # last week
    gemma_threshold_audit.py --by-source        # bucket per source
"""

import argparse
import os
import sqlite3
import time

DB = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")
THRESH_DEDUP = 0.15
THRESH_ASSESS = 0.20

BUCKETS = [
    (0.00, 0.05), (0.05, 0.10), (0.10, 0.15),
    (0.15, 0.20), (0.20, 0.30), (0.30, 0.40),
    (0.40, 0.60), (0.60, 1.01),
]


def conn():
    c = sqlite3.connect(DB, timeout=30.0)
    c.row_factory = sqlite3.Row
    return c


def bucket_of(score):
    for lo, hi in BUCKETS:
        if lo <= score < hi:
            return (lo, hi)
    return (1.0, 1.01)


def fmt_bucket(lo, hi):
    return f"{lo:.2f}-{hi:.2f}"


def audit(c, window_hr):
    since = int(time.time()) - int(window_hr * 3600)
    rows = c.execute(
        "SELECT route, adjusted_score FROM seed_routing_log "
        "WHERE timestamp > ? AND adjusted_score IS NOT NULL "
        "AND route != 'stochastic_reset'",
        (since,)).fetchall()

    if not rows:
        print(f"No rows with adjusted_score in last {window_hr}h. "
              "(Has Gemma been restarted since the schema migration?)")
        return

    # by-bucket counts per route
    routes = ["ignore", "think", "deep"]
    grid = {b: {r: 0 for r in routes} for b in BUCKETS}
    total_by_route = {r: 0 for r in routes}

    for r in rows:
        b = bucket_of(r["adjusted_score"])
        route = r["route"] if r["route"] in routes else "ignore"
        grid[b][route] += 1
        total_by_route[route] += 1

    total = sum(total_by_route.values())
    print(f"Gemma threshold audit — window={window_hr}h — "
          f"n={total} routed ({total_by_route})\n")
    print(f"{'bucket':<12} {'ignore':>8} {'think':>8} {'deep':>8} {'total':>8}  gates")
    print("-" * 60)
    for lo, hi in BUCKETS:
        cell = grid[(lo, hi)]
        tot = sum(cell.values())
        gates = ""
        if lo < THRESH_DEDUP <= hi:
            gates += f" ← DEDUP={THRESH_DEDUP}"
        if lo < THRESH_ASSESS <= hi:
            gates += f" ← ASSESS={THRESH_ASSESS}"
        print(f"{fmt_bucket(lo, hi):<12} {cell['ignore']:>8} {cell['think']:>8} "
              f"{cell['deep']:>8} {tot:>8} {gates}")

    # Surprises: score >= THRESH_ASSESS but route=ignore → Darby said noise
    high_ignored = sum(grid[(lo, hi)]["ignore"]
                       for lo, hi in BUCKETS if lo >= THRESH_ASSESS)
    # Score < THRESH_DEDUP but route != ignore → dissent/priority promoted
    low_promoted = sum(grid[(lo, hi)]["think"] + grid[(lo, hi)]["deep"]
                       for lo, hi in BUCKETS if hi <= THRESH_DEDUP)
    print()
    print(f"Darby-disagreed-with-score: {high_ignored} "
          f"(score≥{THRESH_ASSESS} but route=ignore — Darby said noise)")
    print(f"Promoted-below-dedup: {low_promoted} "
          f"(score<{THRESH_DEDUP} but route≠ignore — dissent/priority fired)")


def audit_by_source(c, window_hr, top=10):
    since = int(time.time()) - int(window_hr * 3600)
    rows = c.execute(
        "SELECT o.source, r.route, r.adjusted_score "
        "FROM seed_routing_log r JOIN seed_observations o ON o.id=r.observation_id "
        "WHERE r.timestamp > ? AND r.adjusted_score IS NOT NULL "
        "AND r.route != 'stochastic_reset'",
        (since,)).fetchall()

    from collections import defaultdict
    per = defaultdict(lambda: {"n": 0, "sum": 0.0, "think": 0, "deep": 0})
    for r in rows:
        s = per[r["source"]]
        s["n"] += 1
        s["sum"] += r["adjusted_score"] or 0
        if r["route"] == "think":
            s["think"] += 1
        elif r["route"] == "deep":
            s["deep"] += 1

    ranked = sorted(per.items(), key=lambda kv: -kv[1]["n"])[:top]
    print(f"\nTop {top} sources by volume (last {window_hr}h):")
    print(f"{'source':<40} {'n':>5} {'avg':>6} {'think':>6} {'deep':>5}  signal%")
    for src, s in ranked:
        avg = s["sum"] / s["n"] if s["n"] else 0
        signal = (s["think"] + s["deep"]) / s["n"] * 100 if s["n"] else 0
        print(f"{src[:40]:<40} {s['n']:>5} {avg:>6.3f} "
              f"{s['think']:>6} {s['deep']:>5}  {signal:>5.1f}%")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--window-hr", type=float, default=24.0)
    ap.add_argument("--by-source", action="store_true")
    args = ap.parse_args()

    c = conn()
    audit(c, args.window_hr)
    if args.by_source:
        audit_by_source(c, args.window_hr)


if __name__ == "__main__":
    main()
