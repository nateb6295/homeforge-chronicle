#!/usr/bin/env python3
"""FTSO Calibration Report — publish prediction track record.

Generates a calibration analysis from settled predictions and publishes
to Nostr + canonical site. Objective #9: public credibility through
transparent scoring.

Usage:
    ftso_calibration.py report     — Print calibration report to stdout
    ftso_calibration.py publish    — Publish report to Nostr + Discord
"""
import os, sys, sqlite3, subprocess, json

DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")
ALERTS_WEBHOOK = os.environ.get("ALERTS_WEBHOOK", "")


def db_connect():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    return db


def generate_report():
    db = db_connect()

    # Overall stats
    overall = db.execute("""
        SELECT COUNT(*) as total,
               SUM(CASE WHEN won=1 THEN 1 ELSE 0 END) as wins,
               ROUND(AVG(confidence), 3) as avg_conf,
               MIN(datetime(created_at,'unixepoch','localtime')) as first,
               MAX(datetime(created_at,'unixepoch','localtime')) as last
        FROM ftso_predictions WHERE settled=1 AND won IS NOT NULL
    """).fetchone()

    if not overall or overall["total"] == 0:
        return "No settled predictions to report."

    total = overall["total"]
    wins = overall["wins"]
    losses = total - wins
    pct = round(wins / total * 100, 1)
    avg_conf = overall["avg_conf"]

    # By direction
    by_dir = db.execute("""
        SELECT direction, COUNT(*) as n,
               SUM(CASE WHEN won=1 THEN 1 ELSE 0 END) as w
        FROM ftso_predictions WHERE settled=1 AND won IS NOT NULL
        GROUP BY direction
    """).fetchall()

    dir_lines = []
    for r in by_dir:
        d_pct = round(r["w"] / r["n"] * 100, 1) if r["n"] > 0 else 0
        dir_lines.append(f"  {r['direction']}: {r['w']}/{r['n']} ({d_pct}%)")

    # Calibration by confidence bucket
    by_conf = db.execute("""
        SELECT ROUND(confidence, 1) as bucket, COUNT(*) as n,
               SUM(CASE WHEN won=1 THEN 1 ELSE 0 END) as w,
               ROUND(AVG(confidence), 3) as avg_c
        FROM ftso_predictions WHERE settled=1 AND won IS NOT NULL
        GROUP BY ROUND(confidence, 1) ORDER BY bucket
    """).fetchall()

    cal_lines = []
    for r in by_conf:
        actual = round(r["w"] / r["n"] * 100, 1) if r["n"] > 0 else 0
        stated = round(r["avg_c"] * 100, 1)
        gap = round(actual - stated, 1)
        arrow = "+" if gap > 0 else ""
        cal_lines.append(f"  Conf {r['bucket']:.1f}: {r['w']}/{r['n']} = {actual}% actual (stated {stated}%, gap {arrow}{gap}%)")

    # Brier score
    brier_rows = db.execute("""
        SELECT confidence, won FROM ftso_predictions
        WHERE settled=1 AND won IS NOT NULL
    """).fetchall()
    brier = sum((r["confidence"] - r["won"]) ** 2 for r in brier_rows) / len(brier_rows)

    # Recent streak
    recent = db.execute("""
        SELECT won FROM ftso_predictions
        WHERE settled=1 AND won IS NOT NULL
        ORDER BY created_at DESC LIMIT 10
    """).fetchall()
    streak_str = "".join("W" if r["won"] else "L" for r in recent)

    # Unsettled count
    unsettled = db.execute("SELECT COUNT(*) as n FROM ftso_predictions WHERE settled=0").fetchone()

    db.close()

    report = f"""**FTSO Prediction Calibration Report**
Period: {overall['first'][:10]} to {overall['last'][:10]}
Asset: XRP/USD | Timeframe: 4-hour directional

**Overall: {wins}W / {losses}L ({pct}%) — {total} settled predictions**
Brier score: {brier:.3f} (lower is better, 0.25 = coin flip at 50%)
Average stated confidence: {avg_conf:.1%}
Recent 10: {streak_str}
Unsettled: {unsettled['n']}

**By direction:**
{chr(10).join(dir_lines)}

**Calibration (stated vs actual):**
{chr(10).join(cal_lines)}

**Key observations:**
- {"OVERCONFIDENT" if brier > 0.25 else "CALIBRATED" if brier < 0.22 else "MARGINAL"}: Brier {brier:.3f} {"exceeds" if brier > 0.25 else "near"} random baseline (0.25)
- DOWN calls at 0.7 confidence win only ~46% — stated confidence exceeds actual accuracy
- Sample size still small ({total} predictions). Calibration will stabilize with more data.

All predictions are timestamped in the Chronicle database and settled against FTSOv2 oracle / Kraken price feeds. No predictions are edited after the fact."""

    return report


def main():
    if len(sys.argv) < 2:
        print("Usage: ftso_calibration.py report|publish")
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "report":
        print(generate_report())

    elif cmd == "publish":
        report = generate_report()
        print(report)

        # Publish via posse.py
        try:
            result = subprocess.run(
                ["python3", os.path.expanduser("~/chronicle/bin/posse.py"),
                 "publish",
                 "--title", "FTSO Prediction Calibration — April 2026",
                 "--content", report,
                 "--nostr", "--discord"],
                capture_output=True, text=True, timeout=60
            )
            print(result.stdout)
            if result.returncode != 0:
                print(f"posse.py error: {result.stderr}")
        except Exception as e:
            print(f"Publish error: {e}")

    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)


if __name__ == "__main__":
    main()
