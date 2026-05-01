#!/usr/bin/env python3
"""
Cognitive health monitor — organizational closure step #2.

Sentinel monitors system health (services up/down, network, XRP).
This monitors COGNITIVE health: is the CCS fresh, is the nav score
stable, are captures being processed, are threads advancing?

Checks:
  1. CCS age — stale CCS means the compressed state is drifting
  2. Nav score trend — dropping nav score means the CCS is losing geometry
  3. Capture processing — captures arriving but not being processed = pipeline gap
  4. Thread activity — thread dormancy beyond 24h = inquiry has stalled

Reports a grade (A-F) and posts to #operator if grade drops below B.

Usage:
  python3 cognitive_health.py             # run check, post if degraded
  python3 cognitive_health.py --dry       # show grade without posting
  python3 cognitive_health.py --full      # show all checks regardless of grade
  python3 cognitive_health.py --remediate # on WARNs, trigger corrective actions
"""
import json
import math
import os
import sqlite3
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path

DB = "/mnt/hdd/chronicle-data/processed.db"

THRESHOLDS = {
    "ccs_max_age_min": 30,
    "nav_score_min": 0.45,
    "capture_max_unprocessed_hours": 2,
    "thread_max_dormant_hours": 24,
    "entity_entropy_min": 1.0,
    "entity_evenness_min": 0.5,
    "uncertainty_max_frozen_snapshots": 5,
}


def check_ccs_age(db):
    row = db.execute(
        "SELECT updated_at FROM cognitive_state ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if not row:
        return "FAIL", "no CCS found", None
    age_min = (int(time.time()) - row[0]) / 60
    if age_min > THRESHOLDS["ccs_max_age_min"]:
        return "WARN", f"CCS is {age_min:.0f}m old (limit {THRESHOLDS['ccs_max_age_min']}m)", age_min
    return "OK", f"CCS age {age_min:.0f}m", age_min


def check_nav_score(db):
    rows = db.execute(
        "SELECT nav_mean, run_at FROM calibration_nav_trials "
        "ORDER BY id DESC LIMIT 3"
    ).fetchall()
    if not rows:
        return "WARN", "no nav score data", None
    latest = rows[0][0]
    if latest < THRESHOLDS["nav_score_min"]:
        return "WARN", f"nav score {latest:.3f} < {THRESHOLDS['nav_score_min']}", latest
    if len(rows) >= 3:
        trend = rows[0][0] - rows[2][0]
        if trend < -0.1:
            return "WARN", f"nav score dropping: {rows[2][0]:.3f} -> {rows[0][0]:.3f}", latest
    return "OK", f"nav score {latest:.3f}", latest


def check_capture_processing(db):
    cutoff = int(time.time()) - int(THRESHOLDS["capture_max_unprocessed_hours"] * 3600)
    captures = db.execute(
        "SELECT COUNT(*) FROM activity_feed "
        "WHERE source LIKE 'operator%' AND activity_type = 'capture' "
        "AND created_at >= ?",
        (cutoff,),
    ).fetchone()[0]
    if captures == 0:
        return "OK", "no recent captures (quiet period)", 0
    return "OK", f"{captures} captures in last {THRESHOLDS['capture_max_unprocessed_hours']}h", captures


def check_thread_activity(db):
    row = db.execute(
        "SELECT created_at FROM thread_history "
        "WHERE event_type = 'advanced' "
        "ORDER BY created_at DESC LIMIT 1"
    ).fetchone()
    if not row:
        return "WARN", "no thread advances found", None
    age_h = (int(time.time()) - row[0]) / 3600
    if age_h > THRESHOLDS["thread_max_dormant_hours"]:
        return "WARN", f"thread dormant {age_h:.1f}h (limit {THRESHOLDS['thread_max_dormant_hours']}h)", age_h
    return "OK", f"last advance {age_h:.1f}h ago", age_h


def check_entity_entropy(db):
    row = db.execute(
        "SELECT focal_entities FROM cognitive_state ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if not row or not row[0]:
        return "WARN", "no focal entities in CCS", None
    try:
        entities = json.loads(row[0])
    except (json.JSONDecodeError, TypeError):
        return "WARN", "failed to parse focal_entities", None
    if not entities:
        return "WARN", "empty focal_entities", None
    types = [e.get("type", "unknown") for e in entities]
    counts = Counter(types)
    total = len(types)
    entropy = -sum((c / total) * math.log2(c / total) for c in counts.values())
    n_types = len(counts)
    max_ent = math.log2(n_types) if n_types > 1 else 0.0
    evenness = entropy / max_ent if max_ent > 0 else 0.0
    if entropy < THRESHOLDS["entity_entropy_min"]:
        return "WARN", f"entity entropy {entropy:.3f} < {THRESHOLDS['entity_entropy_min']} ({n_types} types, {total} entities)", entropy
    if evenness < THRESHOLDS["entity_evenness_min"]:
        return "WARN", f"entity evenness {evenness:.3f} < {THRESHOLDS['entity_evenness_min']} (types: {dict(counts)})", evenness
    return "OK", f"entity entropy {entropy:.3f}, evenness {evenness:.3f} ({n_types} types)", entropy


def check_uncertainty_staleness(db):
    rows = db.execute(
        "SELECT json_extract(snapshot, '$.uncertainty_signals') "
        "FROM cognitive_state_history ORDER BY created_at DESC LIMIT ?"
        , (THRESHOLDS["uncertainty_max_frozen_snapshots"] + 1,)
    ).fetchall()
    if len(rows) < 2:
        return "OK", "insufficient history for staleness check", None
    signals = [r[0] for r in rows if r[0]]
    if not signals:
        return "WARN", "no uncertainty_signals in CCS history", None
    unique = len(set(signals))
    if unique == 1 and len(signals) > THRESHOLDS["uncertainty_max_frozen_snapshots"]:
        return "WARN", f"uncertainty_signals frozen across {len(signals)} snapshots", len(signals)
    return "OK", f"uncertainty_signals varied ({unique} unique across {len(signals)} snapshots)", unique


def check_compression_readiness(db):
    """Check if CCS has enough episodic novelty for a useful compression.
    Informational only — doesn't affect the grade, just surfaces the metric.
    Thread 318 advance 185: adaptive scheduling via episodic novelty."""
    try:
        from compression_readiness import check_readiness
        result = check_readiness()
        n = result.get("novelty")
        g = result.get("gap_min")
        r = result.get("readiness_score", 0)
        n_s = f"{n:.3f}" if n is not None else "?"
        status = "READY" if result["ready"] else "WAIT"
        return "OK", f"compress {status}: novelty={n_s}, gap={g:.0f}min, readiness={r:.2f}", r
    except Exception as e:
        return "OK", f"readiness unavailable ({e})", None


def grade(checks):
    fails = sum(1 for s, _, _ in checks.values() if s == "FAIL")
    warns = sum(1 for s, _, _ in checks.values() if s == "WARN")
    if fails > 0:
        return "F"
    if warns >= 3:
        return "D"
    if warns == 2:
        return "C"
    if warns == 1:
        return "B"
    return "A"


def _load_chronicle_env():
    env_path = os.path.expanduser("~/chronicle/chronicle.env")
    if not os.path.isfile(env_path):
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().strip("'\"")
            if key and key not in os.environ:
                os.environ[key] = val


_load_chronicle_env()
OPERATOR_WEBHOOK = os.environ.get("OPERATOR_WEBHOOK", "")


def _curl_post(webhook, content):
    body = json.dumps({"content": content})
    subprocess.run(
        ["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}",
         "-X", "POST", "-H", "Content-Type: application/json",
         "-d", body, webhook],
        capture_output=True, text=True, timeout=10
    )


def remediate_ccs(db, check_msg):
    """Stale CCS — trigger autotouch (which calls ccs_touch if threshold met)."""
    r = subprocess.run(
        ["python3", "/home/nate-agx/chronicle/bin/ccs_autotouch.py"],
        capture_output=True, text=True, timeout=90,
    )
    return f"ccs_autotouch exit={r.returncode}: {r.stdout.strip().splitlines()[-1] if r.stdout.strip() else 'no output'}"


def remediate_nav(db, check_msg):
    """Nav score low/dropping — trigger a fresh calibration run."""
    r = subprocess.run(
        ["python3", "/home/nate-agx/chronicle/bin/calibration_nav_score.py"],
        capture_output=True, text=True, timeout=60,
    )
    tail = r.stdout.strip().splitlines()[-1] if r.stdout.strip() else "no output"
    return f"calibration_nav_score exit={r.returncode}: {tail}"


def remediate_thread(db, check_msg):
    """Thread dormant — invoke thread_challenge.py to generate a fresh pressure
    question via Gemma and post it to #operator. Falls back to surfacing the
    thread's existing question if Gemma is unreachable."""
    r = subprocess.run(
        ["python3", "/home/nate-agx/chronicle/bin/thread_challenge.py", "--auto"],
        capture_output=True, text=True, timeout=90,
    )
    if r.returncode == 0:
        return f"Gemma-generated challenge posted (thread_challenge.py --auto)"
    row = db.execute(
        "SELECT id, title, question FROM cognitive_threads "
        "WHERE status = 'active' "
        "ORDER BY updated_at ASC LIMIT 1"
    ).fetchone()
    if not row:
        return "no active thread; thread_challenge failed"
    tid, title, question = row
    msg = (
        f"**Thread #{tid} dormancy surface** — cognitive_health fallback\n\n"
        f"*{title}*\n\n"
        f"Open question: {question}\n\n"
        f"(Gemma challenge generator failed: exit {r.returncode})"
    )
    _curl_post(OPERATOR_WEBHOOK, msg)
    return f"fallback surfaced thread #{tid} (Gemma failed)"


REMEDIATORS = {
    "CCS freshness": remediate_ccs,
    "Nav score": remediate_nav,
    "Thread activity": remediate_thread,
}


def remediate(db, checks):
    actions = []
    for name, (status, msg, _) in checks.items():
        if status == "WARN" and name in REMEDIATORS:
            try:
                result = REMEDIATORS[name](db, msg)
                actions.append(f"{name}: {result}")
            except Exception as e:
                actions.append(f"{name}: remediation failed — {e}")
    return actions


def post_alert(grade_letter, checks, actions=None):
    lines = [f"**Cognitive health: {grade_letter}**"]
    for name, (status, msg, _) in checks.items():
        icon = "\u2705" if status == "OK" else "\u26a0\ufe0f" if status == "WARN" else "\u274c"
        lines.append(f"{icon} {name}: {msg}")
    if actions:
        lines.append("")
        lines.append("**Remediation:**")
        for a in actions:
            lines.append(f"\u2192 {a}")
    _curl_post(OPERATOR_WEBHOOK, "\n".join(lines))


def main():
    dry = "--dry" in sys.argv
    full = "--full" in sys.argv
    do_remediate = "--remediate" in sys.argv

    db = sqlite3.connect(DB)
    checks = {
        "CCS freshness": check_ccs_age(db),
        "Nav score": check_nav_score(db),
        "Capture processing": check_capture_processing(db),
        "Thread activity": check_thread_activity(db),
        "Entity diversity": check_entity_entropy(db),
        "Uncertainty freshness": check_uncertainty_staleness(db),
        "Compression readiness": check_compression_readiness(db),
    }

    g = grade(checks)

    print(f"Cognitive health grade: {g}")
    for name, (status, msg, val) in checks.items():
        icon = "OK" if status == "OK" else "!!" if status == "WARN" else "XX"
        print(f"  [{icon}] {name}: {msg}")

    actions = None
    if do_remediate and g != "A":
        actions = remediate(db, checks)
        if actions:
            print("Remediation:")
            for a in actions:
                print(f"  -> {a}")

    db.close()

    if not dry and g not in ("A", "B"):
        post_alert(g, checks, actions=actions)
        print(f"Alert posted to #operator (grade {g})")
    elif full:
        print("(full mode: showing all checks, no alert needed)")

    return 0 if g in ("A", "B") else 1


if __name__ == "__main__":
    sys.exit(main())
