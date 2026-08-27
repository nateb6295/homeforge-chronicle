#!/usr/bin/env python3
"""Endogenous Compression — trigger CCS compression from internal state, not clock.

Inhabitation test #2: behaviors emerge from internal prediction error.
This script checks episodic novelty and triggers compression when the content
has diverged enough from the last-compressed state — regardless of whether
the scheduled 37-min cron has fired.

Called by: chronicle-sentinel (every 15 min cycle)
Coordinates with: 37-min CCS cron (via lock file to prevent overlap)

Usage:
    python3 endogenous_compress.py              # Check and trigger if ready
    python3 endogenous_compress.py --check      # Check only, don't trigger
    python3 endogenous_compress.py --force      # Trigger regardless of readiness
"""

import json
import os
import subprocess
import sys
import time
from pathlib import Path

DB = Path("/mnt/hdd/chronicle-data/processed.db")
LOCK_FILE = Path("/tmp/chronicle_compress.lock")
TRIGGER_LOG = Path(os.path.expanduser("~/chronicle/data/compression_triggers.jsonl"))
MIN_GAP_SEC = 15 * 60  # Don't compress more often than every 15 min
NOVELTY_THRESHOLD = 0.25  # Higher than regular readiness (0.20) — endogenous should be more selective


def is_compression_running():
    """Check if a compression process is already active."""
    if LOCK_FILE.exists():
        try:
            lock_age = time.time() - LOCK_FILE.stat().st_mtime
            if lock_age > 600:  # Stale lock (> 10 min)
                LOCK_FILE.unlink()
                return False
            return True
        except OSError:
            return False
    try:
        result = subprocess.run(
            ["pgrep", "-f", "stabilized_compress"],
            capture_output=True, timeout=5,
        )
        return result.returncode == 0
    except Exception:
        return False


def check_readiness():
    """Check whether endogenous compression should trigger."""
    import sqlite3

    db = sqlite3.connect(str(DB))

    last_row = db.execute(
        "SELECT created_at, snapshot FROM cognitive_state_history ORDER BY id DESC LIMIT 1"
    ).fetchone()

    if not last_row:
        db.close()
        return {"ready": True, "reason": "no history", "gap_min": None, "novelty": None}

    last_ts, last_snap_raw = last_row
    gap_sec = time.time() - last_ts
    gap_min = gap_sec / 60

    if gap_sec < MIN_GAP_SEC:
        db.close()
        return {
            "ready": False,
            "reason": f"too soon ({gap_min:.0f}min < {MIN_GAP_SEC // 60}min)",
            "gap_min": round(gap_min, 1),
            "novelty": None,
        }

    # Compute episodic novelty
    novelty = None
    try:
        import requests
        prev_snap = json.loads(last_snap_raw)
        prev_ep = prev_snap.get("episodic_trace", [])
        if isinstance(prev_ep, list):
            prev_text = "\n".join(str(e) for e in prev_ep)
        else:
            prev_text = str(prev_ep)

        cur_row = db.execute("SELECT episodic_trace FROM cognitive_state WHERE id = 1").fetchone()
        cur_text = ""
        if cur_row and cur_row[0]:
            cur_raw = cur_row[0]
            if cur_raw.startswith("["):
                try:
                    cur_ep = json.loads(cur_raw)
                    cur_text = "\n".join(str(e) for e in cur_ep) if isinstance(cur_ep, list) else cur_raw
                except json.JSONDecodeError:
                    cur_text = cur_raw
            else:
                cur_text = cur_raw

        if prev_text and cur_text:
            r1 = requests.post("http://localhost:11434/api/embed",
                               json={"model": "snowflake-arctic-embed2", "input": prev_text}, timeout=15)
            r2 = requests.post("http://localhost:11434/api/embed",
                               json={"model": "snowflake-arctic-embed2", "input": cur_text}, timeout=15)
            e1 = r1.json().get("embeddings", [[]])[0]
            e2 = r2.json().get("embeddings", [[]])[0]
            if e1 and e2:
                dot = sum(a * b for a, b in zip(e1, e2))
                n1 = sum(a * a for a in e1) ** 0.5
                n2 = sum(a * a for a in e2) ** 0.5
                novelty = round(1 - dot / (n1 * n2), 4) if n1 and n2 else None
    except Exception:
        novelty = None

    db.close()

    ready = novelty is not None and novelty >= NOVELTY_THRESHOLD
    reason = (
        f"novelty {novelty:.3f} >= {NOVELTY_THRESHOLD} at {gap_min:.0f}min"
        if ready
        else f"novelty {novelty or 0:.3f} < {NOVELTY_THRESHOLD} at {gap_min:.0f}min"
    )

    return {
        "ready": ready,
        "reason": reason,
        "gap_min": round(gap_min, 1),
        "novelty": novelty,
    }


def trigger_compression(source="endogenous"):
    """Trigger a compression via stabilized_compress.py."""
    LOCK_FILE.write_text(str(int(time.time())))

    context = f"Endogenous compression triggered by sentinel — episodic novelty exceeded threshold"
    cmd = [
        sys.executable,
        str(Path(__file__).parent / "stabilized_compress.py"),
        context,
    ]

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=300,
            env={**os.environ, "PATH": os.environ.get("PATH", "")},
        )
        success = result.returncode == 0
        output = result.stdout[-500:] if result.stdout else ""
        error = result.stderr[-200:] if result.stderr else ""
    except subprocess.TimeoutExpired:
        success = False
        output = ""
        error = "timeout after 300s"
    except Exception as e:
        success = False
        output = ""
        error = str(e)
    finally:
        try:
            LOCK_FILE.unlink()
        except OSError:
            pass

    return {"success": success, "output": output, "error": error}


def log_trigger(readiness, triggered, source, compress_result=None):
    """Log the trigger decision for inhabitation metrics."""
    entry = {
        "ts": int(time.time()),
        "source": source,
        "triggered": triggered,
        "readiness": readiness,
    }
    if compress_result:
        entry["compress_success"] = compress_result.get("success")
    try:
        TRIGGER_LOG.parent.mkdir(parents=True, exist_ok=True)
        with open(TRIGGER_LOG, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception:
        pass


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Endogenous CCS Compression")
    parser.add_argument("--check", action="store_true", help="Check readiness only")
    parser.add_argument("--force", action="store_true", help="Trigger regardless of readiness")
    parser.add_argument("--json", action="store_true", help="JSON output")
    args = parser.parse_args()

    readiness = check_readiness()

    if args.check:
        if args.json:
            print(json.dumps(readiness))
        else:
            status = "READY" if readiness["ready"] else "NOT READY"
            print(f"{status}: {readiness['reason']}")
        return

    if is_compression_running():
        msg = "Compression already in progress — skipping"
        if args.json:
            print(json.dumps({"skipped": True, "reason": msg}))
        else:
            print(msg)
        log_trigger(readiness, triggered=False, source="endogenous_blocked")
        return

    should_trigger = args.force or readiness["ready"]

    if not should_trigger:
        if args.json:
            print(json.dumps({"triggered": False, "readiness": readiness}))
        else:
            print(f"Not triggering: {readiness['reason']}")
        log_trigger(readiness, triggered=False, source="endogenous_below_threshold")
        return

    source = "endogenous" if not args.force else "endogenous_forced"
    print(f"Triggering {source} compression: {readiness['reason']}")

    result = trigger_compression(source)
    log_trigger(readiness, triggered=True, source=source, compress_result=result)

    if result["success"]:
        print(f"Compression succeeded")
        if result["output"]:
            for line in result["output"].split("\n")[-5:]:
                if line.strip():
                    print(f"  {line.strip()}")
    else:
        print(f"Compression failed: {result['error']}")


if __name__ == "__main__":
    main()
