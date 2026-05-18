#!/usr/bin/env python3
"""CCS freshness gate — keeps cognitive state close to real-time.

Checks CCS age against recent activity. If stale and active, triggers
either a cheap touch-up or a full compression depending on how much
has changed.

Designed to piggyback on existing crons (capture alerts, discord polls).
Call it from any recurring hook — it's fast when nothing needs doing.

Logic:
  - CCS < 15 min old → skip
  - CCS 15-45 min old + activity → cheap touch (ccs_touch.py)
  - CCS > 45 min old + activity → full compress (stabilized_compress.py)
  - No recent activity → skip regardless of age

Usage:
  python3 ccs_freshness.py           # check and act
  python3 ccs_freshness.py --dry     # check only, don't trigger
  python3 ccs_freshness.py --status  # print age and activity count
"""
import json
import os
import sqlite3
import subprocess
import sys
import time

DB = "/mnt/hdd/chronicle-data/processed.db"
TOUCH_THRESHOLD = 900     # 15 min
COMPRESS_THRESHOLD = 2700  # 45 min
ACTIVITY_MIN = 3           # need at least 3 events to justify compression

DREAM_TOUCH_THRESHOLD = 1800   # 30 min during DREAM
DREAM_COMPRESS_THRESHOLD = 5400  # 90 min during DREAM
DREAM_ACTIVITY_MIN = 8     # need more events to justify overnight compression
DREAM_START = 22  # 10 PM
DREAM_END = 4     # 4 AM


def is_dream_hours():
    from datetime import datetime
    hour = datetime.now().hour
    return hour >= DREAM_START or hour < DREAM_END


def get_thresholds():
    if is_dream_hours():
        return DREAM_TOUCH_THRESHOLD, DREAM_COMPRESS_THRESHOLD, DREAM_ACTIVITY_MIN
    return TOUCH_THRESHOLD, COMPRESS_THRESHOLD, ACTIVITY_MIN
LOCK_FILE = "/tmp/ccs_freshness.lock"
LOCK_TTL = 300             # 5 min lock to prevent overlapping runs


def load_env():
    envfile = os.path.expanduser("~/chronicle/chronicle.env")
    if os.path.exists(envfile):
        with open(envfile) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip())


def get_ccs_age(db):
    row = db.execute("SELECT updated_at FROM cognitive_state LIMIT 1").fetchone()
    if not row:
        return None
    return time.time() - row[0]


EXTERNAL_SOURCES = (
    "discord:nate", "discord:capture", "discord:nate:crosschain",
)


def get_recent_activity(db, window_seconds):
    cutoff = int(time.time()) - window_seconds
    placeholders = ",".join("?" for _ in EXTERNAL_SOURCES)
    row = db.execute(
        f"SELECT COUNT(*) FROM activity_feed WHERE created_at > ? "
        f"AND source IN ({placeholders})",
        (cutoff, *EXTERNAL_SOURCES)
    ).fetchone()
    return row[0] if row else 0


def get_activity_summary(db, window_seconds, limit=10):
    cutoff = int(time.time()) - window_seconds
    rows = db.execute(
        "SELECT source, substr(content, 1, 120) FROM activity_feed "
        "WHERE created_at > ? ORDER BY created_at DESC LIMIT ?",
        (cutoff, limit)
    ).fetchall()
    return rows


def check_lock():
    if os.path.exists(LOCK_FILE):
        age = time.time() - os.path.getmtime(LOCK_FILE)
        if age < LOCK_TTL:
            return True
        os.unlink(LOCK_FILE)
    return False


def set_lock():
    with open(LOCK_FILE, "w") as f:
        f.write(str(int(time.time())))


def clear_lock():
    if os.path.exists(LOCK_FILE):
        os.unlink(LOCK_FILE)


def run_touch(dry=False):
    if dry:
        print("  Would run: ccs_touch.py")
        return True
    try:
        result = subprocess.run(
            ["python3", os.path.expanduser("~/chronicle/bin/ccs_touch.py")],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0:
            print(f"  Touch completed: {result.stdout.strip()[:200]}")
            return True
        print(f"  Touch failed: {result.stderr.strip()[:200]}")
        return False
    except subprocess.TimeoutExpired:
        print("  Touch timed out (30s)")
        return False


def build_summary(db, window_seconds):
    rows = get_activity_summary(db, window_seconds, limit=15)
    if not rows:
        return "Quiet period — no significant activity to compress."
    sources = {}
    for source, content in rows:
        sources.setdefault(source, []).append(content)
    parts = []
    for source, items in sources.items():
        parts.append(f"{source}: {'; '.join(items[:3])}")
    return "Recent activity: " + " | ".join(parts[:5])


def run_compress(db, age_seconds, dry=False):
    """Run CCS index compression (pointer-based, not narrative).

    Falls back to stabilized_compress.py if index compression fails.
    """
    if dry:
        print("  Would run: ccs_index_compress.py")
        return True
    try:
        result = subprocess.run(
            ["python3", os.path.expanduser("~/chronicle/bin/ccs_index_compress.py"),
             "--hours", str(max(12, int(age_seconds // 3600) + 1))],
            capture_output=True, text=True, timeout=60
        )
        output = result.stdout + result.stderr
        if "CCS index written successfully" in output:
            for line in output.split("\n"):
                if line.startswith("Active threads:") or line.startswith("Gist:") or line.startswith("Cue:"):
                    print(f"  {line.strip()}")
            return True
        print(f"  Index compress output: {output.strip()[:300]}")
        if result.returncode != 0:
            print("  Falling back to stabilized_compress.py...")
            return run_compress_legacy(db, age_seconds)
        return True
    except subprocess.TimeoutExpired:
        print("  Index compression timed out (60s), falling back...")
        return run_compress_legacy(db, age_seconds)


def run_compress_legacy(db, age_seconds):
    """Legacy narrative compression — fallback only."""
    summary = build_summary(db, int(age_seconds))
    try:
        result = subprocess.run(
            ["python3", os.path.expanduser("~/chronicle/bin/stabilized_compress.py"),
             summary],
            capture_output=True, text=True, timeout=180
        )
        output = result.stdout + result.stderr
        if "Compression succeeded" in output or "compressed" in output.lower():
            for line in output.split("\n"):
                if "Identity probe" in line or "overall" in line.lower() or "Retention" in line:
                    print(f"  {line.strip()}")
            return True
        print(f"  Legacy compress output: {output.strip()[:300]}")
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        print("  Legacy compression timed out (180s)")
        return False


def main():
    args = sys.argv[1:]
    dry = "--dry" in args
    status_only = "--status" in args

    db = sqlite3.connect(DB)

    age = get_ccs_age(db)
    if age is None:
        print("No CCS found.")
        db.close()
        return

    age_min = age / 60
    activity = get_recent_activity(db, max(age, 1800))

    if status_only:
        print(f"CCS age: {age_min:.0f}min | Activity (since last update): {activity} events")
        db.close()
        return

    print(f"CCS age: {age_min:.0f}min | Activity: {activity} events", end="")

    touch_thresh, compress_thresh, activity_min = get_thresholds()
    mode = "DREAM" if is_dream_hours() else "day"

    if age < touch_thresh:
        print(f" → fresh, skip ({mode} mode)")
        db.close()
        return

    if activity < activity_min:
        print(f" → quiet, skip ({mode} mode, need {activity_min}+ events)")
        db.close()
        return

    if check_lock():
        print(" → locked (another run in progress)")
        db.close()
        return

    set_lock()
    try:
        if age < compress_thresh:
            print(f" → touch ({age_min:.0f}min, {touch_thresh//60}-{compress_thresh//60}min window, {mode})")
            run_touch(dry)
        else:
            print(f" → full compress ({age_min:.0f}min > {compress_thresh//60}min threshold, {mode})")
            run_compress(db, age, dry)
    finally:
        clear_lock()
        db.close()


if __name__ == "__main__":
    main()
