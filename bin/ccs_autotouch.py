#!/usr/bin/env python3
"""
Self-triggering CCS refresh — organizational closure step.

Instead of refreshing CCS on a fixed schedule (*/10), this checks the
most recent nav score. If the score has dropped below threshold, the
system's own measurement drives the update. If the score is healthy,
no refresh needed — the CCS is still navigable.

This is the first loop where Chronicle's output (nav score measurement)
becomes the input that determines what runs next (CCS update).

Usage:
  python3 ccs_autotouch.py           # check score, refresh if needed
  python3 ccs_autotouch.py --dry     # show what would happen
  python3 ccs_autotouch.py --force   # refresh regardless of score
"""
import json
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

DB = "/mnt/hdd/chronicle-data/processed.db"
NAV_THRESHOLD_FIXED = 0.55  # legacy fixed floor
CONFORMAL_ALPHA = 0.10  # 90% coverage
CONFORMAL_MIN_TRIALS = 10  # need enough data before trusting conformal bound
STALE_SECONDS = 1800  # 30 min — if no nav score in this window, refresh anyway
TOUCH_SCRIPT = str(Path.home() / "chronicle" / "bin" / "ccs_touch.py")


def latest_nav_score(db):
    row = db.execute(
        "SELECT nav_mean, run_at FROM calibration_nav_trials "
        "ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if not row:
        return None, None
    return row[0], row[1]


def conformal_threshold(db):
    """Compute the conformal lower bound from calibration history.

    Returns the lower bound of the 90% prediction interval, or None
    if insufficient data. This replaces the fixed NAV_THRESHOLD with
    a data-driven bound that adapts as more trials accumulate.
    """
    rows = db.execute(
        "SELECT nav_mean FROM calibration_nav_trials ORDER BY run_at"
    ).fetchall()
    if len(rows) < CONFORMAL_MIN_TRIALS:
        return None
    scores = sorted(r[0] for r in rows)
    n = len(scores)
    import math
    lo_idx = max(0, min(n - 1, math.ceil((n + 1) * CONFORMAL_ALPHA / 2) - 1))
    return scores[lo_idx]


def ccs_age(db):
    row = db.execute(
        "SELECT updated_at FROM cognitive_state ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if not row:
        return None
    return int(time.time()) - row[0]


def should_refresh(db, dry=False):
    nav_score, nav_ts = latest_nav_score(db)
    age = ccs_age(db)

    if nav_score is None:
        reason = "no nav score data yet — refresh as fallback"
        return True, reason, {"nav_score": None, "ccs_age": age}

    nav_age = int(time.time()) - nav_ts if nav_ts else None

    # Use conformal lower bound if available, else fixed threshold
    conf_bound = conformal_threshold(db)
    threshold = conf_bound if conf_bound is not None else NAV_THRESHOLD_FIXED
    threshold_source = "conformal" if conf_bound is not None else "fixed"

    info = {
        "nav_score": nav_score,
        "nav_age_min": round(nav_age / 60, 1) if nav_age else None,
        "ccs_age_min": round(age / 60, 1) if age else None,
        "threshold": threshold,
        "threshold_source": threshold_source,
        "stale_limit_min": STALE_SECONDS / 60,
    }

    if nav_score < threshold:
        reason = f"nav score {nav_score:.3f} < {threshold_source} threshold {threshold:.3f}"
        return True, reason, info

    if nav_age and nav_age > STALE_SECONDS:
        reason = f"nav score is {nav_age // 60}m old (stale limit {STALE_SECONDS // 60}m)"
        return True, reason, info

    if age and age > STALE_SECONDS * 2:
        reason = f"CCS is {age // 60}m old despite healthy nav score"
        return True, reason, info

    reason = f"nav score {nav_score:.3f} >= {threshold_source} threshold {threshold:.3f}, CCS age {age // 60 if age else '?'}m — no refresh needed"
    return False, reason, info


def run_touch(dry=False):
    args = [sys.executable, TOUCH_SCRIPT]
    if dry:
        args.append("--dry")
    r = subprocess.run(args, capture_output=True, text=True, timeout=15)
    return r.stdout.strip(), r.returncode


def main():
    dry = "--dry" in sys.argv
    force = "--force" in sys.argv

    db = sqlite3.connect(DB)
    needed, reason, info = should_refresh(db)
    db.close()

    print(f"Nav score: {info.get('nav_score', 'N/A')}")
    print(f"CCS age: {info.get('ccs_age_min', 'N/A')}m")
    print(f"Decision: {'REFRESH' if (needed or force) else 'SKIP'}")
    print(f"Reason: {reason}")

    if force:
        print("(--force override)")

    if needed or force:
        out, rc = run_touch(dry=dry)
        if out:
            print(out)
        return rc
    return 0


if __name__ == "__main__":
    sys.exit(main())
