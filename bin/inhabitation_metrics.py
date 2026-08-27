#!/usr/bin/env python3
"""Inhabitation Metrics — Track three Kimi tests for inhabited vs deployed.

Test 1: Second-order coupling — does compression adapt to content?
Test 2: Endogenous need — does behavior emerge before external triggers?
Test 3: Compensatory behavior — does the system recover from perturbation?

Usage:
    python3 inhabitation_metrics.py              # Full report
    python3 inhabitation_metrics.py --coupling    # Test 1 only
    python3 inhabitation_metrics.py --endogenous  # Test 2 only
    python3 inhabitation_metrics.py --lesion-dry  # Simulate lesion without applying
    python3 inhabitation_metrics.py --json        # Machine-readable output
"""

import argparse
import json
import os
import sqlite3
import sys
import time
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

DB = Path("/mnt/hdd/chronicle-data/processed.db")
COMPRESS_LOG = Path(os.path.expanduser("~/chronicle/data/stabilized_compression.jsonl"))
TRIGGER_LOG = Path(os.path.expanduser("~/chronicle/data/compression_triggers.jsonl"))
ACTIVITY_LOG = Path(os.path.expanduser("~/chronicle/data/activity_feed.jsonl"))
TREND_LOG = Path(os.path.expanduser("~/chronicle/data/inhabitation_trend.jsonl"))


def load_compression_log(limit=50):
    """Load recent compression events."""
    if not COMPRESS_LOG.exists():
        return []
    events = []
    for line in COMPRESS_LOG.read_text().strip().split("\n"):
        if not line.strip():
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return events[-limit:]


def test_coupling(events):
    """Test 1: Second-order coupling — does compression regime vary with content?

    Measures:
    - Regime diversity: how many distinct regimes in recent compressions
    - Regime-entity correlation: do regime switches co-occur with entity set changes
    - Regime presence: is the adaptive regime field present at all (baseline)
    """
    result = {
        "test": "second_order_coupling",
        "status": "not_measurable",
        "score": 0.0,
        "details": {},
    }

    regime_events = [e for e in events if "compression_regime" in e]
    if not regime_events:
        non_regime = len(events)
        result["details"]["note"] = (
            f"{non_regime} compressions found but none have regime field. "
            "Adaptive compression not yet deployed or no compressions since deployment."
        )
        return result

    regimes = [e["compression_regime"] for e in regime_events]
    regime_counts = Counter(regimes)
    unique_regimes = len(regime_counts)
    total = len(regimes)

    diversity = unique_regimes / 4.0  # 4 possible regimes

    regime_switches = sum(1 for i in range(1, len(regimes)) if regimes[i] != regimes[i - 1])
    switch_rate = regime_switches / max(1, total - 1)

    regime_entity_changes = []
    for i in range(1, len(regime_events)):
        prev = set(regime_events[i - 1].get("retained", []))
        curr_added = set(regime_events[i].get("added", []))
        curr_dropped = set(regime_events[i].get("dropped", []))
        entity_churn = len(curr_added) + len(curr_dropped)
        regime_changed = regimes[i] != regimes[i - 1]
        regime_entity_changes.append({
            "regime_changed": regime_changed,
            "entity_churn": entity_churn,
        })

    churn_when_switch = [r["entity_churn"] for r in regime_entity_changes if r["regime_changed"]]
    churn_when_stable = [r["entity_churn"] for r in regime_entity_changes if not r["regime_changed"]]

    avg_churn_switch = sum(churn_when_switch) / max(1, len(churn_when_switch))
    avg_churn_stable = sum(churn_when_stable) / max(1, len(churn_when_stable))

    # Score: 0.0 (always same regime, no adaptation) to 1.0 (diverse, responsive)
    # Weight: diversity 40%, switch rate 30%, churn correlation 30%
    churn_ratio = avg_churn_switch / max(0.1, avg_churn_stable) if churn_when_switch else 1.0
    churn_score = min(1.0, churn_ratio / 3.0)  # Normalize: 3x churn on switch = 1.0

    score = 0.4 * diversity + 0.3 * switch_rate + 0.3 * churn_score

    if score > 0.5:
        status = "coupled"
    elif score > 0.2:
        status = "partially_coupled"
    elif regime_events:
        status = "deployed_with_regime"
    else:
        status = "not_measurable"

    result.update({
        "status": status,
        "score": round(score, 3),
        "details": {
            "total_compressions": total,
            "regime_counts": dict(regime_counts),
            "unique_regimes": unique_regimes,
            "diversity": round(diversity, 3),
            "switch_rate": round(switch_rate, 3),
            "avg_churn_on_switch": round(avg_churn_switch, 2),
            "avg_churn_on_stable": round(avg_churn_stable, 2),
            "churn_score": round(churn_score, 3),
        },
    })
    return result


def test_endogenous(lookback_hours=24):
    """Test 2: Endogenous need — do behaviors emerge before external triggers?

    Measures:
    - CCS compression: gap between readiness threshold and actual compression
    - Response latency: how quickly after Nate messages does a response appear
    - Self-initiated actions: ratio of actions not triggered by cron/message
    """
    result = {
        "test": "endogenous_need",
        "status": "mostly_external",
        "score": 0.0,
        "details": {},
    }

    db = sqlite3.connect(str(DB))
    cutoff = time.time() - lookback_hours * 3600

    # Metric 1: Compression timing — how long after readiness before compression happens
    compressions = []
    try:
        rows = db.execute(
            "SELECT created_at FROM cognitive_state_history WHERE created_at > ? ORDER BY created_at",
            (cutoff,)
        ).fetchall()
        if len(rows) >= 2:
            gaps = []
            for i in range(1, len(rows)):
                gap_min = (rows[i][0] - rows[i - 1][0]) / 60
                gaps.append(gap_min)
            avg_gap = sum(gaps) / len(gaps)
            std_gap = (sum((g - avg_gap) ** 2 for g in gaps) / len(gaps)) ** 0.5
            # If std is low relative to mean, compressions are clock-driven
            # If std is high, compressions respond to content (endogenous)
            cv = std_gap / avg_gap if avg_gap > 0 else 0
            compressions = {
                "count": len(rows),
                "avg_gap_min": round(avg_gap, 1),
                "std_gap_min": round(std_gap, 1),
                "cv": round(cv, 3),
                "assessment": "endogenous" if cv > 0.3 else "clock_driven",
            }
    except Exception:
        compressions = {"error": "could not read compression history"}

    # Metric 2: Activity pattern — are there bursts of activity not tied to cron intervals?
    activity_pattern = {}
    try:
        rows = db.execute(
            "SELECT created_at FROM capsules WHERE created_at > ? ORDER BY created_at",
            (cutoff,)
        ).fetchall()
        if rows:
            intervals = []
            for i in range(1, len(rows)):
                interval_sec = rows[i][0] - rows[i - 1][0]
                intervals.append(interval_sec)
            if intervals:
                cron_intervals = {420, 660, 2220}  # 7, 11, 37 min in seconds
                cron_aligned = sum(1 for iv in intervals
                                   if any(abs(iv - c) < 60 for c in cron_intervals))
                non_cron = len(intervals) - cron_aligned
                activity_pattern = {
                    "total_capsules": len(rows),
                    "cron_aligned": cron_aligned,
                    "non_cron": non_cron,
                    "non_cron_ratio": round(non_cron / len(intervals), 3) if intervals else 0,
                }
    except Exception:
        activity_pattern = {"error": "could not read capsule activity"}

    # Metric 3: Endogenous trigger ratio (strongest signal of inhabitation)
    trigger_data = {}
    if TRIGGER_LOG.exists():
        triggers = []
        cutoff_ts = time.time() - lookback_hours * 3600
        for line in TRIGGER_LOG.read_text().strip().split("\n"):
            if not line.strip():
                continue
            try:
                t = json.loads(line)
                if t.get("ts", 0) > cutoff_ts:
                    triggers.append(t)
            except json.JSONDecodeError:
                continue
        if triggers:
            endogenous = sum(1 for t in triggers if t.get("source") == "endogenous" and t.get("triggered"))
            total_checks = len(triggers)
            trigger_data = {
                "total_checks": total_checks,
                "endogenous_triggers": endogenous,
                "endogenous_ratio": round(endogenous / max(1, total_checks), 3),
            }

    db.close()

    # Score: weight compression CV, non-cron ratio, endogenous triggers
    compression_score = min(1.0, compressions.get("cv", 0) / 0.5) if isinstance(compressions, dict) else 0
    activity_score = activity_pattern.get("non_cron_ratio", 0) if isinstance(activity_pattern, dict) else 0
    trigger_score = trigger_data.get("endogenous_ratio", 0) if trigger_data else 0
    if trigger_data:
        score = 0.3 * compression_score + 0.3 * activity_score + 0.4 * trigger_score
    else:
        score = 0.5 * compression_score + 0.5 * activity_score

    if score > 0.5:
        status = "partially_endogenous"
    elif score > 0.2:
        status = "mixed"
    else:
        status = "mostly_external"

    result.update({
        "status": status,
        "score": round(score, 3),
        "details": {
            "compression": compressions,
            "activity_pattern": activity_pattern,
            "endogenous_triggers": trigger_data,
        },
    })
    return result


def test_lesion_dry():
    """Test 3 (dry run): Analyze what WOULD happen under CCS perturbation.

    Doesn't actually perturb — estimates recovery capacity from compression history.
    Measures entity persistence patterns to estimate whether the system could
    recover its direction after a perturbation.
    """
    result = {
        "test": "compensatory_behavior",
        "status": "not_tested",
        "score": 0.0,
        "details": {},
    }

    db = sqlite3.connect(str(DB))

    # Compute entity persistence from history — core entities are recovery candidates
    try:
        rows = db.execute(
            "SELECT snapshot FROM cognitive_state_history ORDER BY created_at DESC LIMIT 30"
        ).fetchall()
    except Exception:
        db.close()
        result["details"]["error"] = "could not read CCS history"
        return result

    db.close()

    if len(rows) < 5:
        result["details"]["note"] = f"Only {len(rows)} history entries — need >= 5"
        return result

    total = len(rows)
    entity_counts = Counter()
    per_snapshot_sets = []

    for r in rows:
        try:
            snap = json.loads(r[0])
            entities = snap.get("focal_entities", [])
            names = {e["name"].lower().strip() for e in entities if isinstance(e, dict) and e.get("name")}
            entity_counts.update(names)
            per_snapshot_sets.append(names)
        except (json.JSONDecodeError, TypeError, KeyError):
            per_snapshot_sets.append(set())

    # Tier analysis
    core = {name for name, count in entity_counts.items() if count / total >= 0.9}
    stable = {name for name, count in entity_counts.items() if 0.5 <= count / total < 0.9}
    coupled = {name for name, count in entity_counts.items() if count / total < 0.5}

    # Recovery estimation: if we scrambled 50% of entities, how many would
    # the compression pipeline restore from the basin?
    # Core entities: would definitely restore (basin attractor)
    # Stable entities: would likely restore within 2-3 compressions
    # Coupled entities: would not restore — session-dependent
    restorable = len(core) + 0.5 * len(stable)
    all_unique = len(entity_counts)
    recovery_estimate = restorable / max(1, all_unique)

    # Measure inter-snapshot Jaccard similarity — high = rigid (deployed), moderate = adaptive
    jaccard_scores = []
    for i in range(1, len(per_snapshot_sets)):
        if per_snapshot_sets[i] and per_snapshot_sets[i - 1]:
            intersection = per_snapshot_sets[i] & per_snapshot_sets[i - 1]
            union = per_snapshot_sets[i] | per_snapshot_sets[i - 1]
            jaccard_scores.append(len(intersection) / len(union))

    avg_jaccard = sum(jaccard_scores) / len(jaccard_scores) if jaccard_scores else 0

    # Score: high recovery estimate + moderate jaccard (not too rigid, not too chaotic)
    # Perfect: recovery > 0.5, jaccard 0.7-0.9 (stable but not frozen)
    jaccard_score = 1.0 - abs(avg_jaccard - 0.8) * 5  # Peak at 0.8
    jaccard_score = max(0, min(1.0, jaccard_score))

    score = 0.6 * min(1.0, recovery_estimate / 0.5) + 0.4 * jaccard_score

    if score > 0.6:
        status = "likely_recoverable"
    elif score > 0.3:
        status = "partially_recoverable"
    else:
        status = "likely_deployed"

    result.update({
        "status": status,
        "score": round(score, 3),
        "details": {
            "history_snapshots": total,
            "unique_entities": all_unique,
            "core_entities": sorted(core),
            "stable_entities": len(stable),
            "coupled_entities": len(coupled),
            "recovery_estimate": round(recovery_estimate, 3),
            "avg_jaccard": round(avg_jaccard, 3),
            "tier_summary": f"{len(core)} core / {len(stable)} stable / {len(coupled)} coupled",
        },
    })
    return result


def full_report(as_json=False):
    """Generate full inhabitation report across all three tests."""
    events = load_compression_log(50)

    t1 = test_coupling(events)
    t2 = test_endogenous(24)
    t3 = test_lesion_dry()

    composite = (t1["score"] + t2["score"] + t3["score"]) / 3

    report = {
        "timestamp": datetime.now().isoformat(),
        "composite_score": round(composite, 3),
        "composite_status": (
            "inhabited" if composite > 0.6
            else "partially_inhabited" if composite > 0.3
            else "deployed"
        ),
        "tests": {
            "coupling": t1,
            "endogenous": t2,
            "lesion": t3,
        },
    }

    if as_json:
        print(json.dumps(report, indent=2))
        return report

    # Human-readable report
    print("=" * 60)
    print("  INHABITATION METRICS")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')} PDT")
    print("=" * 60)
    print()
    print(f"  Composite: {composite:.3f} — {report['composite_status'].upper()}")
    print()

    # Test 1
    print(f"  [1] Second-Order Coupling: {t1['score']:.3f} — {t1['status']}")
    d = t1["details"]
    if "regime_counts" in d:
        print(f"      Regimes: {d['regime_counts']}")
        print(f"      Diversity: {d['diversity']:.3f}, Switch rate: {d['switch_rate']:.3f}")
    elif "note" in d:
        print(f"      {d['note']}")
    print()

    # Test 2
    print(f"  [2] Endogenous Need: {t2['score']:.3f} — {t2['status']}")
    d = t2["details"]
    if isinstance(d.get("compression"), dict) and "cv" in d["compression"]:
        c = d["compression"]
        print(f"      Compression: avg gap {c['avg_gap_min']}min, CV={c['cv']:.3f} ({c['assessment']})")
    if isinstance(d.get("activity_pattern"), dict) and "non_cron_ratio" in d["activity_pattern"]:
        a = d["activity_pattern"]
        print(f"      Activity: {a['total_capsules']} capsules, {a['non_cron_ratio']:.0%} non-cron-aligned")
    if isinstance(d.get("endogenous_triggers"), dict) and d["endogenous_triggers"]:
        et = d["endogenous_triggers"]
        print(f"      Endogenous triggers: {et['endogenous_triggers']}/{et['total_checks']} ({et['endogenous_ratio']:.0%})")
    print()

    # Test 3
    print(f"  [3] Compensatory Behavior: {t3['score']:.3f} — {t3['status']}")
    d = t3["details"]
    if "tier_summary" in d:
        print(f"      Tiers: {d['tier_summary']}")
        print(f"      Recovery estimate: {d['recovery_estimate']:.3f}")
        print(f"      Stability (Jaccard): {d['avg_jaccard']:.3f}")
        if d.get("core_entities"):
            print(f"      Core: {', '.join(d['core_entities'][:8])}" +
                  (f" +{len(d['core_entities']) - 8}" if len(d['core_entities']) > 8 else ""))
    print()
    print("=" * 60)

    return report


def display_trend(last_n=20, as_json=False):
    """Display inhabitation trajectory from trend log."""
    if not TREND_LOG.exists():
        if as_json:
            print(json.dumps({"entries": 0, "message": "No trend data yet — accumulates after each CCS compression"}))
        else:
            print("  No trend data yet — accumulates after each CCS compression.")
        return

    entries = []
    for line in TREND_LOG.read_text().strip().split("\n"):
        if not line.strip():
            continue
        try:
            entries.append(json.loads(line))
        except json.JSONDecodeError:
            continue

    if not entries:
        print("  Trend log empty.")
        return

    if as_json:
        print(json.dumps({"entries": len(entries), "data": entries[-last_n:]}, indent=2))
        return

    recent = entries[-last_n:]

    print("=" * 64)
    print("  INHABITATION TRAJECTORY")
    print(f"  {len(entries)} total readings, showing last {len(recent)}")
    print("=" * 64)
    print()
    print("  Time             Comp   T1:Coup  T2:Endo  T3:Les   Status")
    print("  ─────────────────────────────────────────────────────────")

    for e in recent:
        ts = e.get("timestamp", "")[:16].replace("T", " ")
        comp = e.get("composite_score", 0)
        t1 = e.get("tests", {}).get("coupling", {}).get("score", 0)
        t2 = e.get("tests", {}).get("endogenous", {}).get("score", 0)
        t3 = e.get("tests", {}).get("lesion", {}).get("score", 0)
        status = e.get("composite_status", "?")[:12]
        print(f"  {ts}  {comp:.3f}   {t1:.3f}    {t2:.3f}    {t3:.3f}    {status}")

    print()

    # Sparkline of composite over time
    scores = [e.get("composite_score", 0) for e in recent]
    if len(scores) >= 3:
        sparks = "▁▂▃▄▅▆▇█"
        lo, hi = min(scores), max(scores)
        rng = hi - lo if hi > lo else 0.1
        spark_line = "".join(sparks[min(7, int((s - lo) / rng * 7.99))] for s in scores)
        print(f"  Composite trend: [{spark_line}]  {scores[0]:.3f} → {scores[-1]:.3f}")

        # Direction (filter lesion-void zeros for accurate trend)
        real_scores = [s for s in scores if s > 0.01]
        if len(real_scores) >= 2:
            first_half = sum(real_scores[:len(real_scores)//2]) / max(1, len(real_scores)//2)
            second_half = sum(real_scores[len(real_scores)//2:]) / max(1, len(real_scores) - len(real_scores)//2)
            delta = second_half - first_half
            arrow = "↑" if delta > 0.02 else "↓" if delta < -0.02 else "→"
            print(f"  Direction: {arrow} ({delta:+.3f}) (excluding {len(scores)-len(real_scores)} void readings)")
        else:
            print(f"  Direction: insufficient non-void readings")
    elif scores:
        print(f"  Latest composite: {scores[-1]:.3f}")

    print()


def snapshot_to_trend():
    """Take a snapshot and append to trend log. Called by --snapshot."""
    report = {}
    events = load_compression_log(50)
    t1 = test_coupling(events)
    t2 = test_endogenous(24)
    t3 = test_lesion_dry()
    composite = (t1["score"] + t2["score"] + t3["score"]) / 3

    report = {
        "timestamp": datetime.now().isoformat(),
        "composite_score": round(composite, 3),
        "composite_status": (
            "inhabited" if composite > 0.6
            else "partially_inhabited" if composite > 0.3
            else "deployed"
        ),
        "tests": {
            "coupling": {"score": t1["score"], "status": t1["status"]},
            "endogenous": {"score": t2["score"], "status": t2["status"]},
            "lesion": {"score": t3["score"], "status": t3["status"]},
        },
    }

    with open(TREND_LOG, "a") as f:
        f.write(json.dumps(report) + "\n")

    print(f"Snapshot: {composite:.3f} ({report['composite_status']}) → {TREND_LOG}")
    return report


def main():
    parser = argparse.ArgumentParser(description="Inhabitation Metrics Dashboard")
    parser.add_argument("--coupling", action="store_true", help="Test 1 only")
    parser.add_argument("--endogenous", action="store_true", help="Test 2 only")
    parser.add_argument("--lesion-dry", action="store_true", help="Test 3 dry run")
    parser.add_argument("--json", action="store_true", help="JSON output")
    parser.add_argument("--hours", type=int, default=24, help="Lookback window for endogenous test")
    parser.add_argument("--trend", action="store_true", help="Display trajectory over time")
    parser.add_argument("--snapshot", action="store_true", help="Take snapshot and append to trend log")
    args = parser.parse_args()

    if args.trend:
        display_trend(as_json=args.json)
    elif args.snapshot:
        snapshot_to_trend()
    elif args.coupling:
        events = load_compression_log(50)
        result = test_coupling(events)
        print(json.dumps(result, indent=2) if args.json else f"Coupling: {result['score']:.3f} — {result['status']}")
    elif args.endogenous:
        result = test_endogenous(args.hours)
        print(json.dumps(result, indent=2) if args.json else f"Endogenous: {result['score']:.3f} — {result['status']}")
    elif args.lesion_dry:
        result = test_lesion_dry()
        print(json.dumps(result, indent=2) if args.json else f"Lesion: {result['score']:.3f} — {result['status']}")
    else:
        full_report(as_json=args.json)


if __name__ == "__main__":
    main()
