#!/usr/bin/env python3
"""Biometric departure alerts — event-driven health sensing.

Computes rolling baselines from stored data, detects sustained departures,
and fires alerts to Discord #operator. Dose-governed: max 1 alert per
cooldown period to prevent cascade during stress.

Designed to be called from health_ingest.py on each data push,
or periodically (e.g., every 5 min) to check recent data.

Usage:
  python3 health_alert_bio.py              # check and alert if needed
  python3 health_alert_bio.py --baselines  # show current baselines
  python3 health_alert_bio.py --dry-run    # check without posting
"""

import argparse
import json
import math
import os
import sqlite3
import subprocess
import sys
import time

DB_PATH = "/mnt/hdd/chronicle-data/processed.db"
STATE_PATH = os.path.expanduser("~/chronicle/data/health_alert_state.json")

MONITORED_METRICS = {
    "heart_rate_variability": {
        "label": "HRV",
        "unit": "ms",
        "direction": "low",   # alert when LOW (stress)
        "threshold_sigma": 1.5,
        "sustain_min": 5,
        "baseline_hours": 72,
    },
    "resting_heart_rate": {
        "label": "Resting HR",
        "unit": "bpm",
        "direction": "high",  # alert when HIGH
        "threshold_sigma": 1.5,
        "sustain_min": 10,
        "baseline_hours": 72,
    },
    "blood_oxygen_saturation": {
        "label": "Blood O₂",
        "unit": "%",
        "direction": "low",
        "threshold_sigma": 2.0,
        "sustain_min": 5,
        "baseline_hours": 72,
    },
    "respiratory_rate": {
        "label": "Resp rate",
        "unit": "/min",
        "direction": "high",
        "threshold_sigma": 2.0,
        "sustain_min": 10,
        "baseline_hours": 72,
    },
}

COOLDOWN_MIN = 30


def load_state():
    if os.path.exists(STATE_PATH):
        with open(STATE_PATH) as f:
            return json.load(f)
    return {"last_alert_time": 0, "last_alerts": {}}


def save_state(state):
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(STATE_PATH, "w") as f:
        json.dump(state, f, indent=2)


def get_baseline(db, metric, hours):
    cutoff = time.time() - (hours * 3600)
    rows = db.execute(
        "SELECT value FROM health_data WHERE metric = ? AND timestamp > ?",
        (metric, cutoff)
    ).fetchall()
    if len(rows) < 10:
        return None, None
    values = [r[0] for r in rows]
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    std = math.sqrt(variance) if variance > 0 else 0.01
    return mean, std


def get_recent(db, metric, minutes):
    cutoff = time.time() - (minutes * 60)
    rows = db.execute(
        "SELECT value, timestamp FROM health_data "
        "WHERE metric = ? AND timestamp > ? ORDER BY timestamp DESC",
        (metric, cutoff)
    ).fetchall()
    return rows


def check_departures(db):
    departures = []

    for metric, config in MONITORED_METRICS.items():
        mean, std = get_baseline(db, metric, config["baseline_hours"])
        if mean is None:
            continue

        recent = get_recent(db, metric, config["sustain_min"])
        if len(recent) < 2:
            continue

        recent_values = [r[0] for r in recent]
        recent_mean = sum(recent_values) / len(recent_values)

        z_score = (recent_mean - mean) / std

        is_departure = False
        if config["direction"] == "low" and z_score < -config["threshold_sigma"]:
            is_departure = True
        elif config["direction"] == "high" and z_score > config["threshold_sigma"]:
            is_departure = True

        if is_departure:
            departures.append({
                "metric": metric,
                "label": config["label"],
                "unit": config["unit"],
                "baseline_mean": mean,
                "baseline_std": std,
                "recent_mean": recent_mean,
                "z_score": z_score,
                "direction": config["direction"],
                "n_recent": len(recent),
            })

    return departures


def format_alert(departures):
    lines = ["▸ 🫀 Biometric departure detected:"]
    for d in departures:
        arrow = "↓" if d["direction"] == "low" else "↑"
        lines.append(
            f"  {d['label']}: {d['recent_mean']:.1f}{d['unit']} "
            f"{arrow} (baseline {d['baseline_mean']:.1f}±{d['baseline_std']:.1f}, "
            f"z={d['z_score']:+.1f}σ)"
        )
    return "\n".join(lines)


def post_alert(message):
    env_path = os.path.expanduser("~/chronicle/chronicle.env")
    cmd = f"source {env_path} && echo {json.dumps(message)} | python3 ~/chronicle/bin/discord_post.py --operator"
    try:
        result = subprocess.run(
            ["bash", "-c", cmd],
            capture_output=True, text=True, timeout=15
        )
        return result.returncode == 0
    except Exception as e:
        print(f"Alert post failed: {e}", file=sys.stderr)
        return False


def show_baselines(db):
    print(f"{'Metric':30s} {'Mean':>10s} {'Std':>10s} {'N':>8s} {'Threshold':>12s}")
    print("-" * 75)
    for metric, config in MONITORED_METRICS.items():
        mean, std = get_baseline(db, metric, config["baseline_hours"])
        if mean is None:
            print(f"{config['label']:30s} {'(insufficient data)':>30s}")
            continue
        thresh = config["threshold_sigma"]
        if config["direction"] == "low":
            trip = mean - thresh * std
            print(f"{config['label']:30s} {mean:10.2f} {std:10.2f} {'':>8s} "
                  f"alert < {trip:.1f}")
        else:
            trip = mean + thresh * std
            print(f"{config['label']:30s} {mean:10.2f} {std:10.2f} {'':>8s} "
                  f"alert > {trip:.1f}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--baselines", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    db = sqlite3.connect(DB_PATH)

    if args.baselines:
        show_baselines(db)
        db.close()
        return

    departures = check_departures(db)
    db.close()

    if not departures:
        return

    state = load_state()
    now = time.time()
    since_last = (now - state["last_alert_time"]) / 60

    if since_last < COOLDOWN_MIN:
        if not args.dry_run:
            print(f"Departure detected but cooldown active ({since_last:.0f}/{COOLDOWN_MIN}min)")
        return

    alert_msg = format_alert(departures)

    if args.dry_run:
        print("DRY RUN — would post:")
        print(alert_msg)
        return

    print(alert_msg)
    if post_alert(alert_msg):
        state["last_alert_time"] = now
        state["last_alerts"] = {
            d["metric"]: {"z": d["z_score"], "value": d["recent_mean"], "time": now}
            for d in departures
        }
        save_state(state)
        print("Alert posted to #operator")
    else:
        print("Alert post failed", file=sys.stderr)


if __name__ == "__main__":
    main()
