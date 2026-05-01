#!/usr/bin/env python3
"""
Daily digest of calibration_trials to #operator.

Reads the last 24h of trials, posts trend + latest per-question scores.
Cron daily at 18:00 PDT (or run manually).
"""
import json
import os
import sqlite3
import sys
import time
import urllib.request

DB = "/mnt/hdd/chronicle-data/processed.db"


def latest_trials(hours=24, limit=24):
    since = int(time.time()) - hours * 3600
    db = sqlite3.connect(DB)
    db.row_factory = sqlite3.Row
    rows = db.execute(
        "SELECT * FROM calibration_trials WHERE run_at >= ? "
        "ORDER BY run_at ASC LIMIT ?",
        (since, limit),
    ).fetchall()
    db.close()
    return [dict(r) for r in rows]


def format_digest(trials):
    if not trials:
        return "**Calibration 24h digest** — no trials yet."
    overall = [t["overall_jaccard"] for t in trials]
    first, last = overall[0], overall[-1]
    delta = last - first
    arrow = "→" if abs(delta) < 0.01 else ("↑" if delta > 0 else "↓")
    mean = sum(overall) / len(overall)

    latest_pq = json.loads(trials[-1]["per_question_json"] or "{}")

    lines = [
        "**Calibration score — 24h digest**",
        "",
        f"Trials: **{len(trials)}** | mean: **{mean:.3f}** | "
        f"first→last: **{first:.3f} {arrow} {last:.3f}** (Δ {delta:+.3f})",
        "",
        "Latest per-question:",
    ]
    for q, s in latest_pq.items():
        tag = "—" if s is None else f"{s:.3f}"
        lines.append(f"  [{tag}] {q}")
    lines.append("")
    if delta > 0.01:
        lines.append("Trend: rising. Substrate touch is gaining coverage.")
    elif delta < -0.01:
        lines.append("Trend: falling. Touch may be missing drift types.")
    else:
        lines.append("Trend: flat. Substrate coverage stable over window.")
    return "\n".join(lines)


def post(text):
    url = os.environ.get("OPERATOR_WEBHOOK")
    if not url:
        print(text)
        return
    body = json.dumps({"content": text[:1900]}).encode()
    req = urllib.request.Request(
        url, data=body, headers={"Content-Type": "application/json"}
    )
    urllib.request.urlopen(req, timeout=10).read()


if __name__ == "__main__":
    trials = latest_trials()
    digest = format_digest(trials)
    if "--dry" in sys.argv:
        print(digest)
    else:
        post(digest)
