#!/usr/bin/env python3
"""Prediction Monitor — periodic check + alert for Objective #9.

Runs via cron every 6h. Checks all open predictions, records snapshots,
and alerts to Discord when predictions need attention.

Alerts on:
  - Predictions within 3 days of deadline
  - Gemma gate rate drifting outside prediction range
  - Market price moving >10% in 24h
  - Overdue predictions needing scoring
"""
import sqlite3, os, sys, time, json, subprocess
from datetime import datetime

DB_PATH = os.path.expanduser("~/.homeforge-chronicle/processed.db")
BIN_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_PATH = os.path.expanduser("~/chronicle/prediction_monitor.log")


def ensure_calibration_table(db):
    db.execute("""
        CREATE TABLE IF NOT EXISTS calibration_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            total_scored INTEGER NOT NULL,
            correct INTEGER NOT NULL,
            incorrect INTEGER NOT NULL,
            partial INTEGER NOT NULL DEFAULT 0,
            brier_score REAL NOT NULL,
            accuracy REAL NOT NULL,
            bucket_data TEXT,
            recorded_at INTEGER NOT NULL
        )
    """)
    db.commit()


def record_calibration_snapshot(db):
    """Record current calibration state for historical tracking."""
    ensure_calibration_table(db)
    rows = db.execute(
        "SELECT confidence, outcome FROM prediction_track WHERE status='scored'"
    ).fetchall()
    if not rows:
        return

    total = len(rows)
    correct = sum(1 for _, o in rows if o == "correct")
    incorrect = sum(1 for _, o in rows if o == "incorrect")
    partial = sum(1 for _, o in rows if o == "partial")
    accuracy = correct / total if total else 0

    brier_sum = sum(
        (c - (1.0 if o == "correct" else 0.5 if o == "partial" else 0.0)) ** 2
        for c, o in rows
    )
    brier = brier_sum / total

    buckets = {}
    for conf, outcome in rows:
        b = round(conf, 1)
        if b not in buckets:
            buckets[b] = {"total": 0, "correct": 0}
        buckets[b]["total"] += 1
        if outcome in ("correct", "partial"):
            buckets[b]["correct"] += 1

    # Only record if total_scored changed since last snapshot
    last = db.execute(
        "SELECT total_scored FROM calibration_history ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if last and last[0] == total:
        log(f"Calibration unchanged ({total} scored, Brier {brier:.3f})")
        return

    db.execute(
        "INSERT INTO calibration_history "
        "(total_scored, correct, incorrect, partial, brier_score, accuracy, bucket_data, recorded_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (total, correct, incorrect, partial, brier, accuracy, json.dumps(buckets), int(time.time()))
    )
    db.commit()
    log(f"Calibration snapshot: {correct}/{total} correct ({accuracy:.0%}), Brier {brier:.3f}")


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
             "publish", "--title", "Prediction Monitor",
             "--content", message, "--source", "opus", "--discord"],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode == 0:
            log("Discord alert sent")
        else:
            log(f"Discord alert failed: {result.stderr[:200]}")
    except Exception as e:
        log(f"Discord alert error: {e}")


def check_gemma_gate(db):
    """Record Gemma ignore rate snapshot with regime analysis."""
    now_ts = int(time.time())
    cutoff_6h = now_ts - 21600
    cutoff_24h = now_ts - 86400

    # 6h aggregate
    rows = db.execute(
        "SELECT route, COUNT(*) FROM seed_routing_log WHERE timestamp > ? GROUP BY route",
        (cutoff_6h,)
    ).fetchall()
    if not rows:
        return None, None

    routing = {r[0]: r[1] for r in rows}
    total = sum(routing.values())
    ignore = routing.get("ignore", 0)
    rate = (ignore / total * 100) if total > 0 else 0

    # Regime analysis: hourly breakdown over 24h
    hourly = db.execute(
        "SELECT CAST((? - timestamp) / 3600 AS INTEGER) as hours_ago, route, COUNT(*) "
        "FROM seed_routing_log WHERE timestamp > ? "
        "GROUP BY hours_ago, route ORDER BY hours_ago",
        (now_ts, cutoff_24h)
    ).fetchall()

    from collections import defaultdict
    by_hour = defaultdict(lambda: defaultdict(int))
    for h, route, cnt in hourly:
        if 0 <= h < 24:
            by_hour[h][route] = cnt

    # Compute per-hour ignore rates and classify regimes
    high_vol_ign, high_vol_total = 0, 0  # >100 items/hr
    low_vol_ign, low_vol_total = 0, 0    # 1-100 items/hr
    gap_hours = 0
    hourly_rates = []
    for h in range(24):
        d = by_hour[h]
        h_total = sum(d.values())
        h_ign = d.get("ignore", 0)
        if h_total == 0:
            gap_hours += 1
        elif h_total > 100:
            high_vol_ign += h_ign
            high_vol_total += h_total
            hourly_rates.append(h_ign / h_total * 100)
        else:
            low_vol_ign += h_ign
            low_vol_total += h_total
            hourly_rates.append(h_ign / h_total * 100)

    high_vol_rate = (high_vol_ign / high_vol_total * 100) if high_vol_total else 0
    low_vol_rate = (low_vol_ign / low_vol_total * 100) if low_vol_total else 0

    # Variability
    if len(hourly_rates) >= 2:
        mean_r = sum(hourly_rates) / len(hourly_rates)
        var_r = sum((r - mean_r) ** 2 for r in hourly_rates) / len(hourly_rates)
        stddev_r = var_r ** 0.5
    else:
        stddev_r = 0

    # Record snapshots
    db.execute(
        "INSERT INTO prediction_snapshots (prediction_id, metric_name, metric_value, window_hours, recorded_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (9, "gemma_ignore_rate", rate, 6, now_ts)
    )
    db.execute(
        "INSERT INTO prediction_snapshots (prediction_id, metric_name, metric_value, window_hours, recorded_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (9, "gemma_high_vol_rate", high_vol_rate, 24, now_ts)
    )
    db.execute(
        "INSERT INTO prediction_snapshots (prediction_id, metric_name, metric_value, window_hours, recorded_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (9, "gemma_low_vol_rate", low_vol_rate, 24, now_ts)
    )
    db.execute(
        "INSERT INTO prediction_snapshots (prediction_id, metric_name, metric_value, window_hours, recorded_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (9, "gemma_rate_stddev", stddev_r, 24, now_ts)
    )
    db.commit()

    log(f"Gemma gate: {rate:.1f}% ignore ({ignore}/{total} in 6h)")
    log(f"  Regime breakdown (24h): high-vol {high_vol_rate:.1f}% ({high_vol_total} items), "
        f"low-vol {low_vol_rate:.1f}% ({low_vol_total} items), {gap_hours} gap hours")
    log(f"  Hourly stddev: {stddev_r:.1f}% across {len(hourly_rates)} active hours")

    return rate, routing


def check_xrp_price(db):
    """Get latest XRP price and 24h change."""
    row = db.execute(
        "SELECT price_usd, timestamp FROM price_history WHERE symbol='XRP' ORDER BY timestamp DESC LIMIT 1"
    ).fetchone()
    if not row:
        return None, None

    price, ts = row
    row_24h = db.execute(
        "SELECT price_usd FROM price_history WHERE symbol='XRP' AND timestamp < ? - 86400 ORDER BY timestamp DESC LIMIT 1",
        (ts,)
    ).fetchone()

    change_pct = None
    if row_24h and row_24h[0] > 0:
        change_pct = ((price - row_24h[0]) / row_24h[0]) * 100

    # Record snapshot
    db.execute(
        "INSERT INTO prediction_snapshots (prediction_id, metric_name, metric_value, window_hours, recorded_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (5, "xrp_price", price, 0, int(time.time()))
    )
    db.commit()
    log(f"XRP: ${price:.2f}" + (f" ({change_pct:+.1f}% 24h)" if change_pct else ""))
    return price, change_pct


# Keywords for geopolitical/governance predictions — used to scan pipeline briefs
PREDICTION_KEYWORDS = {
    6: ["ceasefire", "peace deal", "iran negotiat", "iran truce", "diplomatic resolution"],
    8: ["ground ops", "ground forces", "kharg island", "iran territory", "boots on the ground", "ground invasion"],
    10: ["cyberattack", "swift", "financial infrastructure", "bank hack", "payment system"],
    11: ["dfinity", "icp quantum", "post-quantum", "chain-key", "dominic williams"],
    12: ["quantum-resistant", "quantum-proof", "post-quantum upgrade", "blockchain quantum"],
    13: ["quantum-resistant", "quantum announcement", "quantum washing"],
    14: ["hormuz", "convoy", "escort", "strait reopen", "passage", "tanker corridor", "tanker", "persian gulf", "shipping lane", "naval escort", "oil transit"],
    15: ["facial recognition", "wrongful arrest", "ai identification", "biometric ban", "facial recognition ban"],
    16: ["attorney general", "ag nominee", "bondi", "todd blanche", "doj leadership"],
}


def scan_pipeline_evidence(db, alerts):
    """Scan recent briefs for evidence related to open geopolitical predictions."""
    now_ts = int(time.time())
    cutoff = now_ts - 21600  # 6 hours

    open_preds = db.execute(
        "SELECT id, claim, confidence FROM prediction_track WHERE status='open'"
    ).fetchall()

    for pid, claim, conf in open_preds:
        keywords = PREDICTION_KEYWORDS.get(pid)
        if not keywords:
            continue

        # Build SQL LIKE clauses
        like_clauses = " OR ".join(
            f"LOWER(content) LIKE '%{kw.lower()}%'" for kw in keywords
        )
        hits = db.execute(
            f"SELECT id, activity_type, substr(content, 1, 200) "
            f"FROM activity_feed "
            f"WHERE created_at > ? AND ({like_clauses}) "
            f"ORDER BY created_at DESC LIMIT 3",
            (cutoff,)
        ).fetchall()

        if hits:
            evidence_summary = "; ".join(
                f"[{atype}] {content[:80]}" for _, atype, content in hits
            )
            log(f"  #{pid} evidence ({len(hits)} hits): {evidence_summary[:200]}")

            # Log as observation in activity_feed for Opus to see
            obs_text = (
                f"Prediction #{pid} evidence scan: {len(hits)} relevant brief(s) in 6h. "
                f"Conf={conf}. Claim: {claim[:100]}. "
                f"Evidence: {evidence_summary[:300]}"
            )
            # Only log if we haven't already logged for this prediction in the last 6h
            existing = db.execute(
                "SELECT id FROM activity_feed WHERE activity_type='observation' "
                "AND content LIKE ? AND created_at > ?",
                (f"Prediction #{pid} evidence%", cutoff)
            ).fetchone()
            if not existing:
                db.execute(
                    "INSERT INTO activity_feed (source, activity_type, title, content, metadata, created_at) "
                    "VALUES ('prediction_monitor', 'observation', ?, ?, ?, ?)",
                    (f"Prediction #{pid} evidence", obs_text,
                     json.dumps({"prediction_id": pid, "hit_count": len(hits)}),
                     now_ts)
                )
                db.commit()
                alerts.append(f"EVIDENCE #{pid} ({len(hits)} hits): {evidence_summary[:120]}")


def run_monitor():
    log("=== Prediction Monitor Run ===")
    db = sqlite3.connect(DB_PATH)

    alerts = []

    # Check all open predictions
    preds = db.execute(
        "SELECT id, claim, confidence, deadline, category, resolution_criteria "
        "FROM prediction_track WHERE status='open' ORDER BY deadline"
    ).fetchall()

    if not preds:
        log("No open predictions")
        db.close()
        return

    now = datetime.now()

    for pid, claim, conf, deadline, cat, criteria in preds:
        try:
            days_left = (datetime.strptime(deadline, "%Y-%m-%d") - now).days
        except ValueError:
            days_left = 999

        # Overdue check
        if days_left < 0:
            alerts.append(f"OVERDUE #{pid}: {claim[:80]} (by {abs(days_left)}d)")
            log(f"OVERDUE: #{pid} {claim[:60]}")

        # Approaching deadline
        elif days_left <= 3:
            alerts.append(f"DEADLINE #{pid} ({days_left}d): {claim[:80]}")
            log(f"DEADLINE NEAR: #{pid} {days_left}d left")

    # Gemma gate check (Prediction #9) — only alert if prediction is still open
    pred9_status = db.execute(
        "SELECT status FROM prediction_track WHERE id=9"
    ).fetchone()
    rate, routing = check_gemma_gate(db)
    if rate is not None and pred9_status and pred9_status[0] == "open":
        if rate < 82 or rate > 92:
            alerts.append(
                f"GEMMA GATE: ignore rate {rate:.1f}% — OUTSIDE predicted 82-92% range. "
                f"Routing: {json.dumps(routing)}"
            )

    # XRP price check (Prediction #5)
    price, change = check_xrp_price(db)
    if price is not None and change is not None:
        if abs(change) > 10:
            alerts.append(f"XRP: ${price:.2f} ({change:+.1f}% 24h) — significant movement")

    # Scan pipeline for geopolitical prediction evidence
    try:
        scan_pipeline_evidence(db, alerts)
    except Exception as e:
        log(f"Evidence scan error: {e}")

    # Snapshot history summary
    snapshots = db.execute(
        "SELECT prediction_id, metric_name, COUNT(*), MIN(metric_value), MAX(metric_value), AVG(metric_value) "
        "FROM prediction_snapshots GROUP BY prediction_id, metric_name"
    ).fetchall()
    if snapshots:
        log("Snapshot history:")
        for pid, name, count, mn, mx, avg in snapshots:
            log(f"  #{pid} {name}: {count} snapshots, range [{mn:.2f}, {mx:.2f}], avg {avg:.2f}")

    # Auto-score check — score predictions with conclusive evidence
    newly_scored = []
    try:
        sys.path.insert(0, BIN_DIR)
        from prediction import autoscore as run_autoscore
        newly_scored = run_autoscore(db) or []
        if newly_scored:
            for pid, outcome, notes in newly_scored:
                alerts.append(f"AUTO-SCORED #{pid}: {outcome} — {notes[:120]}")
                log(f"AUTO-SCORED: #{pid} {outcome}")
    except Exception as e:
        log(f"Auto-score error: {e}")

    # Record calibration history snapshot
    try:
        record_calibration_snapshot(db)
    except Exception as e:
        log(f"Calibration snapshot error: {e}")

    # Auto-publish calibration after new scorings (keeps public record current)
    if newly_scored:
        try:
            from prediction import calibration as publish_calibration
            log("Publishing updated calibration curve after new scoring(s)")
            publish_calibration(db)
        except Exception as e:
            log(f"Calibration publish error: {e}")

    db.close()

    # Send alerts if any
    if alerts:
        msg = "Prediction Monitor Alerts:\n\n" + "\n".join(f"- {a}" for a in alerts)
        log(f"Sending {len(alerts)} alert(s) to Discord")
        discord_alert(msg)
    else:
        log("All predictions nominal")


if __name__ == "__main__":
    run_monitor()
