#!/usr/bin/env python3
"""fiction_drift.py — Fiction ratio trend analysis for the CCS.

Reads the coherence log and surfaces drift patterns:
- Rolling average over recent compressions
- Trend direction (fiction growing, shrinking, or stable)
- Correlation between claim count and fiction ratio
- High-fiction clusters (consecutive high readings)

Build #53, Trip Day 1. The day's central correction (wrong causal
attribution despite accurate state measurement) raised the question:
is the CCS systematically drifting toward more fiction over time?

Usage:
  python3 fiction_drift.py            # summary
  python3 fiction_drift.py --detail   # per-entry breakdown
"""

import json, sys, os
from pathlib import Path
from datetime import datetime

LOG = Path(os.path.expanduser("~/chronicle/data/ccs_coherence_log.jsonl"))

def load_entries():
    if not LOG.exists():
        return []
    entries = []
    with open(LOG) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return entries

def rolling_avg(values, window=10):
    if len(values) < window:
        return sum(values) / len(values) if values else 0
    return sum(values[-window:]) / window

def trend_direction(values, window=15):
    if len(values) < 4:
        return "insufficient data"
    half = len(values) // 2
    first_half = sum(values[:half]) / half
    second_half = sum(values[half:]) / (len(values) - half)
    diff = second_half - first_half
    if abs(diff) < 0.05:
        return "stable"
    return "rising" if diff > 0 else "falling"

def high_fiction_clusters(entries, threshold=0.5):
    clusters = []
    current = []
    for e in entries:
        if e["fiction_ratio"] >= threshold:
            current.append(e)
        else:
            if len(current) >= 2:
                clusters.append(current)
            current = []
    if len(current) >= 2:
        clusters.append(current)
    return clusters

def claim_correlation(entries):
    if len(entries) < 5:
        return None
    claims = [e["total_claims"] for e in entries]
    ratios = [e["fiction_ratio"] for e in entries]
    n = len(entries)
    mean_c = sum(claims) / n
    mean_r = sum(ratios) / n
    cov = sum((c - mean_c) * (r - mean_r) for c, r in zip(claims, ratios)) / n
    std_c = (sum((c - mean_c)**2 for c in claims) / n) ** 0.5
    std_r = (sum((r - mean_r)**2 for r in ratios) / n) ** 0.5
    if std_c == 0 or std_r == 0:
        return 0
    return cov / (std_c * std_r)

def version_span(entries):
    versions = [e.get("version", 0) for e in entries if e.get("version")]
    if not versions:
        return 0, 0
    return min(versions), max(versions)

def main():
    detail = "--detail" in sys.argv
    entries = load_entries()

    if not entries:
        print("No coherence data found.")
        return

    ratios = [e["fiction_ratio"] for e in entries]
    v_min, v_max = version_span(entries)
    t_min = min(e["timestamp"] for e in entries)
    t_max = max(e["timestamp"] for e in entries)
    days = (t_max - t_min) / 86400

    print(f"Fiction Drift Analysis — {len(entries)} measurements")
    print(f"  CCS versions {v_min}–{v_max} over {days:.1f} days")
    print()

    overall = sum(ratios) / len(ratios)
    recent_10 = rolling_avg(ratios, 10)
    recent_5 = rolling_avg(ratios, 5)
    trend = trend_direction(ratios)

    print(f"Overall average:    {overall:.2f}")
    print(f"Last 10 avg:        {recent_10:.2f}")
    print(f"Last 5 avg:         {recent_5:.2f}")
    print(f"Trend:              {trend}")
    print()

    zeros = sum(1 for r in ratios if r == 0)
    low = sum(1 for r in ratios if 0 < r <= 0.3)
    mid = sum(1 for r in ratios if 0.3 < r <= 0.5)
    high = sum(1 for r in ratios if r > 0.5)
    print(f"Distribution: zero={zeros}  low(0-30%)={low}  mid(30-50%)={mid}  high(>50%)={high}")

    corr = claim_correlation(entries)
    if corr is not None:
        label = "positive" if corr > 0.2 else "negative" if corr < -0.2 else "weak"
        print(f"Claim-count correlation: {corr:+.2f} ({label})")
        if corr > 0.2:
            print("  → More claims = higher fiction. Texture overreaches when generating many connections.")
        elif corr < -0.2:
            print("  → More claims = lower fiction. Richer sessions provide better grounding.")
    print()

    clusters = high_fiction_clusters(entries)
    if clusters:
        print(f"High-fiction clusters (≥2 consecutive readings >50%): {len(clusters)}")
        for i, c in enumerate(clusters):
            versions = [e.get("version", "?") for e in c]
            avg_fr = sum(e["fiction_ratio"] for e in c) / len(c)
            print(f"  Cluster {i+1}: v{versions[0]}–v{versions[-1]} ({len(c)} readings, avg {avg_fr:.0%})")
    else:
        print("No high-fiction clusters detected.")

    # Recent trajectory
    if len(entries) >= 3:
        print(f"\nRecent trajectory:")
        for e in entries[-5:]:
            ts = datetime.fromtimestamp(e["timestamp"]).strftime("%m/%d %H:%M")
            bar = "█" * int(e["fiction_ratio"] * 20)
            print(f"  v{e.get('version', '?'):>5}  {ts}  {e['fiction_ratio']:.0%} {bar}  "
                  f"({e.get('supported', 0)}S/{e.get('partial', 0)}P/{e.get('unsupported', 0)}U)")

    if detail:
        print(f"\n{'='*60}")
        print("All entries:")
        for e in entries:
            ts = datetime.fromtimestamp(e["timestamp"]).strftime("%Y-%m-%d %H:%M")
            print(f"  v{e.get('version', '?'):>5}  {ts}  fiction={e['fiction_ratio']:.0%}  "
                  f"claims={e['total_claims']}  S={e.get('supported', 0)} P={e.get('partial', 0)} U={e.get('unsupported', 0)}")


if __name__ == "__main__":
    main()
