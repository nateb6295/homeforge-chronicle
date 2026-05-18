#!/usr/bin/env python3
"""Phase 5.1 Proprioception Report — routing confidence calibration diagnostics.

Usage:
    python3 proprioception_report.py           # Last 24h summary
    python3 proprioception_report.py --hours N # Last N hours
    python3 proprioception_report.py --all     # All data
"""
import json
import os
import sqlite3
import sys
import time

DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")


def report(hours=24):
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    cutoff = int(time.time()) - hours * 3600

    rows = db.execute(
        "SELECT * FROM routing_proprioception WHERE timestamp > ? ORDER BY timestamp",
        (cutoff,),
    ).fetchall()

    # ACI threshold
    aci_row = db.execute("SELECT value FROM kv_store WHERE key = 'aci_threshold'").fetchone()
    aci_threshold = float(aci_row[0]) if aci_row else 0.65
    print(f"ACI threshold: {aci_threshold:.1%} (promotion below this confidence)\n")

    if not rows:
        print(f"No proprioception data in last {hours}h.")
        return

    total = len(rows)
    by_route = {"1": [], "2": []}
    by_source = {}
    by_feedback = {}
    confidences = []

    for r in rows:
        route = r["route"]
        conf = r["confidence"]
        source = r["source"]
        feedback = r["feedback"] or "pending"

        by_route.setdefault(route, []).append(conf)
        by_source.setdefault(source, []).append({"route": route, "conf": conf, "feedback": feedback})
        by_feedback[feedback] = by_feedback.get(feedback, 0) + 1
        if conf is not None:
            confidences.append(conf)

    print(f"=== Proprioception Report ({hours}h, {total} decisions) ===\n")

    # Overall confidence distribution
    if confidences:
        avg = sum(confidences) / len(confidences)
        high = sum(1 for c in confidences if c > 0.9)
        mid = sum(1 for c in confidences if 0.6 <= c <= 0.9)
        low = sum(1 for c in confidences if c < 0.6)
        print(f"Confidence: avg={avg:.1%}  high(>90%)={high}  mid(60-90%)={mid}  low(<60%)={low}")

    # By route
    for route, confs in sorted(by_route.items()):
        label = "noise" if route == "1" else "signal"
        avg = sum(confs) / len(confs) if confs else 0
        print(f"  Route {route} ({label}): n={len(confs)}  avg_conf={avg:.1%}")

    print()

    # By feedback
    print("Feedback:")
    for fb, count in sorted(by_feedback.items()):
        print(f"  {fb}: {count}")

    # Calibration quality
    confirmed_correct = by_feedback.get("confirmed_noise", 0) + by_feedback.get("confirmed_signal", 0)
    errors = by_feedback.get("missed_signal", 0) + by_feedback.get("false_alarm", 0)
    scored = confirmed_correct + errors
    if scored > 0:
        accuracy = confirmed_correct / scored
        print(f"\nCalibration accuracy: {accuracy:.1%} ({confirmed_correct}/{scored})")
        if by_feedback.get("missed_signal", 0) > 0:
            print(f"  ⚠ Missed signals: {by_feedback['missed_signal']} (noise routes that were actually signal)")
        if by_feedback.get("false_alarm", 0) > 0:
            print(f"  ⚠ False alarms: {by_feedback['false_alarm']} (signal routes with no downstream engagement)")

    print()

    # By source (top sources)
    print("By source:")
    for source, items in sorted(by_source.items(), key=lambda x: -len(x[1]))[:10]:
        noise = sum(1 for i in items if i["route"] == "1")
        signal = sum(1 for i in items if i["route"] == "2")
        avg_conf = sum(i["conf"] for i in items if i["conf"]) / len(items) if items else 0
        errors = sum(1 for i in items if i["feedback"] in ("missed_signal", "false_alarm"))
        print(f"  {source}: n={len(items)} noise={noise} signal={signal} avg_conf={avg_conf:.1%}" +
              (f" ERRORS={errors}" if errors else ""))

    # Confidence-correctness correlation (are high-confidence decisions more accurate?)
    correct_confs = []
    error_confs = []
    for r in rows:
        fb = r["feedback"]
        conf = r["confidence"]
        if conf is None or fb is None:
            continue
        if fb in ("confirmed_noise", "confirmed_signal"):
            correct_confs.append(conf)
        elif fb in ("missed_signal", "false_alarm"):
            error_confs.append(conf)

    if correct_confs and error_confs:
        avg_correct = sum(correct_confs) / len(correct_confs)
        avg_error = sum(error_confs) / len(error_confs)
        print(f"\nCalibration gap: correct decisions avg {avg_correct:.1%} vs errors avg {avg_error:.1%}")
        if avg_correct > avg_error:
            print("  ✓ Model IS more confident when correct — confidence is informative")
        else:
            print("  ✗ Model is NOT more confident when correct — confidence needs recalibration")

    db.close()


if __name__ == "__main__":
    if "--all" in sys.argv:
        report(hours=24*365)
    elif "--hours" in sys.argv:
        idx = sys.argv.index("--hours")
        report(hours=int(sys.argv[idx + 1]))
    else:
        report()
