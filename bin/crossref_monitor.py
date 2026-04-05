#!/usr/bin/env python3
"""Crossref Throughput Monitor — meta-instrument for crossref health (Mod #94).

Runs via cron every 4h. Detects:
  - Connection rate drops (drought detection)
  - High rejection rates (environment shift)
  - Same-event filter effectiveness
  - Feed diversity collapse

Thread #254 identified the need: point-wise gate correctness doesn't imply
system-level health. This monitors the system level.
"""
import sqlite3, os, sys, time, json, subprocess
from datetime import datetime

DB_PATH = os.path.expanduser("~/.homeforge-chronicle/processed.db")
BIN_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_PATH = os.path.expanduser("~/chronicle/crossref_monitor.log")

# Thresholds
MIN_CONNECTIONS_PER_12H = 2  # alert if fewer than this in 12h
GATE_REJECT_RATE_ALERT = 0.95  # alert if >95% of evaluated candidates rejected by gate
DROUGHT_HOURS = 8  # alert if 0 connections in this window


def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG_PATH, "a") as f:
        f.write(line + "\n")


def discord_alert(message):
    """Post alert to Discord via posse.py."""
    try:
        result = subprocess.run(
            [sys.executable, os.path.join(BIN_DIR, "posse.py"),
             "publish", "--title", "Crossref Monitor",
             "--content", message, "--source", "opus", "--discord"],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode == 0:
            log("Discord alert sent")
        else:
            log(f"Discord alert failed: {result.stderr[:200]}")
    except Exception as e:
        log(f"Discord alert error: {e}")


def get_connection_rates(db):
    """Get connection counts at various windows."""
    now = int(time.time())
    rates = {}
    for label, hours in [("1h", 1), ("4h", 4), ("12h", 12), ("24h", 24), ("3d", 72)]:
        cutoff = now - hours * 3600
        count = db.execute(
            "SELECT COUNT(*) FROM crossref_connections WHERE created_at > ?",
            (cutoff,)
        ).fetchone()[0]
        rates[label] = count
    return rates


def get_cycle_stats(db):
    """Get aggregated crossref cycle stats from last 12h."""
    now = int(time.time())
    cutoff = now - 43200  # 12h
    stats = {}
    for metric in ["candidates", "same_event_skip", "llm_reject", "gate_reject",
                    "text_dedup_skip", "mechanism_skip", "capsule_cap_skip", "accepted"]:
        row = db.execute(
            "SELECT SUM(value), COUNT(value) FROM cycle_metrics "
            "WHERE metric = ? AND created_at > ?",
            (f"crossref_{metric}", cutoff)
        ).fetchone()
        stats[metric] = {"total": int(row[0] or 0), "cycles": int(row[1] or 0)}
    return stats


def get_feed_diversity(db):
    """Check source diversity in recent briefs."""
    now = int(time.time())
    cutoff = now - 43200  # 12h
    rows = db.execute(
        "SELECT source, COUNT(*) as cnt FROM seed_observations "
        "WHERE timestamp > ? AND source IN ('canister:capsule', 'activity:intern:brief') "
        "GROUP BY source",
        (cutoff,)
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def record_snapshot(db, rates, cycle_stats):
    """Record monitor snapshot for trend analysis."""
    now = int(time.time())
    ts_label = f"cxmon_{now}"

    # Connection rates
    for window, count in rates.items():
        db.execute(
            "INSERT INTO cycle_metrics (cycle_ts, metric, value, detail, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (ts_label, f"crossref_rate_{window}", count, None, now)
        )

    # Rejection breakdown from cycle stats
    if cycle_stats.get("candidates", {}).get("total", 0) > 0:
        total_cands = cycle_stats["candidates"]["total"]
        total_accepted = cycle_stats["accepted"]["total"]
        accept_rate = total_accepted / total_cands if total_cands > 0 else 0
        db.execute(
            "INSERT INTO cycle_metrics (cycle_ts, metric, value, detail, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (ts_label, "crossref_accept_rate_12h", round(accept_rate, 4),
             json.dumps({k: v["total"] for k, v in cycle_stats.items()}), now)
        )

    db.commit()


def analyze_and_alert(rates, cycle_stats):
    """Check for anomalies and alert."""
    alerts = []

    # Drought detection
    if rates.get("12h", 0) < MIN_CONNECTIONS_PER_12H:
        if rates.get("3d", 0) > 10:  # was producing before
            alerts.append(
                f"**Crossref drought**: {rates['12h']} connections in 12h "
                f"(vs {rates['3d']} in 3d). Feed homogeneity or gate miscalibration?"
            )

    # Zero connections in DROUGHT_HOURS
    if rates.get(f"{DROUGHT_HOURS}h", rates.get("4h", 0)) == 0:
        # Check closest window
        for window in ["4h", "12h"]:
            if rates.get(window, 0) == 0:
                alerts.append(f"**Zero connections** in last {window}.")
                break

    # High rejection rate from cycle stats
    if cycle_stats.get("candidates", {}).get("total", 0) > 0:
        total_cands = cycle_stats["candidates"]["total"]
        total_accepted = cycle_stats["accepted"]["total"]
        if total_cands >= 10:  # enough data
            reject_rate = 1 - (total_accepted / total_cands)
            if reject_rate >= GATE_REJECT_RATE_ALERT:
                # Breakdown
                same_event = cycle_stats["same_event_skip"]["total"]
                llm_rej = cycle_stats["llm_reject"]["total"]
                gate_rej = cycle_stats["gate_reject"]["total"]
                alerts.append(
                    f"**High crossref rejection** (12h): {reject_rate:.0%} of {total_cands} candidates rejected. "
                    f"Breakdown: {same_event} same-event, {llm_rej} LLM-reject, {gate_rej} gate-reject."
                )

    # Same-event filter effectiveness
    if cycle_stats.get("same_event_skip", {}).get("total", 0) > 0:
        same_pct = cycle_stats["same_event_skip"]["total"] / max(1, cycle_stats["candidates"]["total"])
        if same_pct > 0.5:
            alerts.append(
                f"**Feed homogeneity**: {same_pct:.0%} of candidates are same-event pairs. "
                f"Breaking news may be dominating the feed."
            )

    return alerts


def main():
    log("═══ Crossref Monitor ═══")

    db = sqlite3.connect(DB_PATH)

    # Gather data
    rates = get_connection_rates(db)
    cycle_stats = get_cycle_stats(db)
    diversity = get_feed_diversity(db)

    # Log current state
    log(f"Connection rates: {rates}")
    if cycle_stats.get("candidates", {}).get("cycles", 0) > 0:
        log(f"Cycle stats (12h, {cycle_stats['candidates']['cycles']} cycles):")
        for k, v in cycle_stats.items():
            if v["total"] > 0:
                log(f"  {k}: {v['total']}")
    else:
        log("No cycle stats yet (instrumentation just deployed)")
    log(f"Feed diversity (12h): {diversity}")

    # Record snapshot
    record_snapshot(db, rates, cycle_stats)

    # Check for anomalies
    alerts = analyze_and_alert(rates, cycle_stats)

    if alerts:
        log(f"⚠ {len(alerts)} alert(s):")
        for a in alerts:
            log(f"  {a}")
        discord_alert("\n".join(alerts))
    else:
        log("✓ Crossref health OK")

    # Rate trend (compare to historical)
    try:
        # Average daily rate over last 7 days
        week_count = db.execute(
            "SELECT COUNT(*) FROM crossref_connections WHERE created_at > ?",
            (int(time.time()) - 604800,)
        ).fetchone()[0]
        avg_daily = week_count / 7
        log(f"7-day average: {avg_daily:.1f}/day (current 24h: {rates['24h']})")
        if rates["24h"] < avg_daily * 0.3 and avg_daily > 5:
            log(f"⚠ 24h rate is {rates['24h']/avg_daily:.0%} of 7-day average")
    except Exception as e:
        log(f"Trend analysis error: {e}")

    db.close()
    log("Done.")


if __name__ == "__main__":
    main()
