#!/usr/bin/env python3
"""CCS Adaptive — Closed-loop cognitive state compression.

Replaces fixed 4-hour cron with sensing + adapting:
  - Monitors activity signals (capsules, captures, time)
  - Computes readiness score
  - Compresses when ready, not when scheduled
  - 4h ceiling (never longer), 3h floor (never shorter). See the note at
    MIN_INTERVAL_S — the docstring said 2h while the code said 3h until
    2026-08-25.
  - Logs every decision for analysis

Nate green-light Jul 20 2026: "No restrictions on making this reality."
Thread #316 interoception advance: sensing + adapting, not just sensing + scheduling.
"""

import json
import logging
import os
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

DB_PATH = os.environ.get(
    "CHRONICLE_DB",
    "/mnt/hdd/chronicle-data/processed.db",
)
LOG_FILE = os.path.expanduser("~/chronicle/ccs-adaptive.log")
STATE_FILE = os.path.expanduser("~/chronicle/data/ccs_adaptive_state.json")
COMPRESS_SCRIPT = os.path.expanduser("~/chronicle/bin/stabilized_compress.py")
CHECK_INTERVAL = 300  # 5 minutes between checks

# Interval bounds.
#
# CITATION CORRECTED 2026-08-25. These are described elsewhere as "3h floor /
# 4h ceiling per F160". That overstates what F160 supports, and the code has
# always said so one line down: the 3h floor is "aligned with
# stabilized_compress.py", not derived from any measurement.
#
# What F160 actually established: an inverted-U EXISTS — there is an optimum,
# and both too-little and too-much are worse than it. That was measured in
# ACTIVATION space (CCS as a per-forward-pass intervention on local models,
# D1..D10 applications), and F625 reproduced the inverted-U at per-layer
# resolution on Phi-2.
#
# What F160 does NOT establish: that "dose" and "compressions per day" are the
# same axis. Our live compression is a TEXT operation through the Anthropic API
# — summarise previous_state + session into a new gist. A capsule from the
# period is explicit that the frequency version was an ANALOGY, not a
# measurement: "Sentinel was firing 5 overnight compressions when 1-2 sufficed.
# That was the INFRASTRUCTURE VERSION of what became F160's inverted-U."
#
# So: the SHAPE is measured, the VALUES are not. By our own standing rule — a
# number with no runnable method is a memory, not a baseline — 3h and 4h are
# memories. They are probably fine, they have held for weeks with no observed
# harm, and nothing here argues for changing them. Do not cite them as
# experimentally derived, and do not defend them as though F160 sets them.
MIN_INTERVAL_S = 3 * 3600   # 3 hours
MAX_INTERVAL_S = 4 * 3600   # 4 hours — ceiling (never go longer)

# Readiness thresholds
CAPSULE_THRESHOLD = 30       # capsules since last compression
CAPTURE_THRESHOLD = 5        # captures processed since last compression
ACTIVITY_WEIGHT_CAPSULE = 2  # points per capsule
ACTIVITY_WEIGHT_CAPTURE = 10 # points per capture
TIME_WEIGHT = 1              # points per minute since last compression
READINESS_THRESHOLD = 200    # total score to trigger early compression

# THE CEILING IS UNREACHABLE, AND HAS NEVER FIRED IN NORMAL OPERATION.
# Predicted by arithmetic, confirmed against 30 days of data 2026-08-25.
#
# TIME_WEIGHT=1 point/min against READINESS_THRESHOLD=200 means time ALONE
# reaches the threshold at exactly 200 minutes. MAX_INTERVAL_S is 240. So the
# readiness branch always fires first and the ceiling branch below can only be
# reached after a service restart, where elapsed_s falls back to MAX_INTERVAL_S.
#
# Observed intervals, last 30 days, n=237:
#     under 180 (floor)   10   4%   <- all Aug 1-14, none since; these come from
#                                      the OTHER compression path, the Stop hook
#                                      at turn 60, which does not honour this floor
#     180-200            226  95%
#     200-240              0   0%   <- the entire ceiling range is empty
#     over 240             1   0%   <- one 610-min gap, an outage
#
# So what actually runs is a ~181-minute timer with 20 minutes of activity-driven
# early trigger, NOT the 3h-4h closed-loop range it is described as. That is a
# fact about the system, not an argument for changing it: 181 min sits inside the
# intended window and nothing has gone wrong.
#
# IF you ever want the ceiling to bind, READINESS_THRESHOLD must exceed
# TIME_WEIGHT * (MAX_INTERVAL_S/60) — i.e. > 240 — which would widen the dynamic
# range from 20 minutes to 60 and make activity matter three times more. DO NOT
# do that on tidiness. The 3h/4h values are memories, not measurements (see the
# note at MIN_INTERVAL_S), so widening the range has exactly as little evidence
# behind it as leaving it alone. Check every parameter against every other before
# touching one; see data/ccs_cadence_prediction.md.

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CCS-ADAPTIVE] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("ccs-adaptive")

shutdown = False

def handle_signal(sig, frame):
    global shutdown
    log.info(f"Received signal {sig}, shutting down gracefully")
    shutdown = True

signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)


def get_last_compression_time(conn):
    row = conn.execute(
        "SELECT created_at FROM cognitive_state_history ORDER BY created_at DESC LIMIT 1"
    ).fetchone()
    return row[0] if row else 0


def count_capsules_since(conn, since_epoch):
    """Capsules written since `since_epoch`.

    TYPE GUARD added 2026-08-24. knowledge_capsules.created_at is INTEGER in
    77,358 rows and TEXT in 85. SQLite sorts TEXT above every integer, so those
    85 rows satisfied ANY cutoff — this function returned 85 for "since one
    minute ago" AND for "since the year 3000", verified empirically.

    Downstream that made `capsules >= CAPSULE_THRESHOLD` (30) PERMANENTLY TRUE
    and contributed a constant 170 of the READINESS_THRESHOLD of 200. The
    service cleared its 3h floor, found the activity condition already
    satisfied, and compressed immediately — every time. That, not design, is
    the flat 181.4-minute cadence.

    Guarding at the CONSUMER rather than repairing the 85 rows is deliberate:
    fixing the rows without finding the writer would just reset the clock on
    the same bug. This stays correct no matter what the writer does.
    """
    row = conn.execute(
        "SELECT COUNT(*) FROM knowledge_capsules "
        "WHERE typeof(created_at)='integer' AND created_at > ?",
        (since_epoch,),
    ).fetchone()
    return row[0] if row else 0


def count_captures_since(conn, since_epoch):
    """Captures processed since `since_epoch`.

    TYPE GUARD added 2026-08-25, the morning after the identical fix on
    count_capsules_since — and found only because Nate said "loose connections,
    too many pieces don't connect," which sent me auditing EDGES instead of
    components.

    capture_processed.processed_at is INTEGER in 8,628 rows and TEXT in 6.
    FOUR of those six literally contain the string '%s' — an unsubstituted
    format placeholder written straight to the column. The other two are ISO
    date strings. SQLite sorts TEXT above every integer, so all six satisfy
    ANY cutoff: this returned 6 for "since the year 3000".

    At ACTIVITY_WEIGHT_CAPTURE=10 that was **60 permanent points** in every
    readiness computation — a second phantom source feeding the same threshold
    I fixed last night. Last night's capsule guard removed 170 points and I
    never checked the other column.
    """
    row = conn.execute(
        "SELECT COUNT(*) FROM capture_processed "
        "WHERE typeof(processed_at)='integer' AND processed_at > ?",
        (since_epoch,),
    ).fetchone()
    return row[0] if row else 0


def compute_readiness(minutes_elapsed, capsule_count, capture_count):
    time_score = minutes_elapsed * TIME_WEIGHT
    capsule_score = capsule_count * ACTIVITY_WEIGHT_CAPSULE
    capture_score = capture_count * ACTIVITY_WEIGHT_CAPTURE
    total = time_score + capsule_score + capture_score
    return {
        "total": round(total, 1),
        "time_score": round(time_score, 1),
        "capsule_score": round(capsule_score, 1),
        "capture_score": round(capture_score, 1),
        "minutes_elapsed": round(minutes_elapsed, 1),
        "capsule_count": capsule_count,
        "capture_count": capture_count,
    }


def build_trigger_summary(readiness, trigger_reason):
    parts = []
    if readiness["capsule_count"] > 0:
        parts.append(f"{readiness['capsule_count']} capsules stored")
    if readiness["capture_count"] > 0:
        parts.append(f"{readiness['capture_count']} captures processed")
    parts.append(f"{readiness['minutes_elapsed']:.0f} minutes elapsed")
    return f"Adaptive compression ({trigger_reason}): {', '.join(parts)}"


def run_compression(summary):
    log.info(f"Triggering compression: {summary}")
    try:
        result = subprocess.run(
            # --v5: switched 2026-08-23. v5 was written Jul 17 and recorded in
            # memory as "current format" but was never wired in -- it is opt-in
            # only, and this call passed no flags, so ~280 compressions ran v4.
            # Rationale + pre-registered prediction + kill conditions:
            # data/v5_prereg.md. Revert = drop "--v5" here.
            [sys.executable, COMPRESS_SCRIPT, "--v5", summary],
            capture_output=True,
            text=True,
            timeout=120,
            cwd=os.path.expanduser("~/chronicle"),
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        if result.returncode == 0:
            log.info(f"Compression succeeded: {result.stdout[-200:] if result.stdout else 'ok'}")
            return True
        else:
            log.error(f"Compression failed (rc={result.returncode}): {result.stderr[-300:]}")
            return False
    except subprocess.TimeoutExpired:
        log.error("Compression timed out after 120s")
        return False
    except Exception as e:
        log.error(f"Compression error: {e}")
        return False


def save_state(state):
    Path(STATE_FILE).parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"last_check": 0, "compressions": 0, "decisions": []}


def main():
    global shutdown
    log.info("CCS Adaptive starting — closed-loop compression service")
    log.info(f"Floor: {MIN_INTERVAL_S//3600}h | Ceiling: {MAX_INTERVAL_S//3600}h | "
             f"Check interval: {CHECK_INTERVAL}s | Readiness threshold: {READINESS_THRESHOLD}")

    state = load_state()

    while not shutdown:
        try:
            conn = sqlite3.connect(DB_PATH)
            now = time.time()

            last_compression = get_last_compression_time(conn)
            elapsed_s = now - last_compression if last_compression else MAX_INTERVAL_S
            elapsed_min = elapsed_s / 60

            capsules = count_capsules_since(conn, last_compression)
            captures = count_captures_since(conn, last_compression)
            readiness = compute_readiness(elapsed_min, capsules, captures)

            conn.close()

            should_compress = False
            trigger_reason = ""

            if elapsed_s >= MAX_INTERVAL_S:
                should_compress = True
                trigger_reason = "ceiling reached (4h)"
            elif elapsed_s >= MIN_INTERVAL_S and readiness["total"] >= READINESS_THRESHOLD:
                should_compress = True
                trigger_reason = f"readiness {readiness['total']:.0f} >= {READINESS_THRESHOLD}"
            elif elapsed_s >= MIN_INTERVAL_S and capsules >= CAPSULE_THRESHOLD:
                should_compress = True
                trigger_reason = f"capsule burst ({capsules} >= {CAPSULE_THRESHOLD})"
            elif elapsed_s >= MIN_INTERVAL_S and captures >= CAPTURE_THRESHOLD:
                should_compress = True
                trigger_reason = f"capture burst ({captures} >= {CAPTURE_THRESHOLD})"

            decision = {
                "timestamp": int(now),
                "elapsed_min": round(elapsed_min, 1),
                "readiness": readiness["total"],
                "compressed": should_compress,
                # "below threshold" was a lie whenever the real blocker was the
                # clock. Measured Aug 23 over 310 consecutive checks: readiness
                # was NEVER below threshold (min 235 vs bar 200, max 2538), yet
                # every non-compressing decision was logged as "below threshold".
                # Name the constraint that actually bound.
                "reason": trigger_reason or (
                    f"floor not cleared ({elapsed_min:.0f}m < "
                    f"{MIN_INTERVAL_S // 60}m); readiness "
                    f"{readiness['total']:.0f}/{READINESS_THRESHOLD} "
                    f"{'(already satisfied)' if readiness['total'] >= READINESS_THRESHOLD else '(also short)'}"
                ),
            }

            if should_compress:
                summary = build_trigger_summary(readiness, trigger_reason)
                success = run_compression(summary)
                decision["success"] = success
                if success:
                    state["compressions"] = state.get("compressions", 0) + 1
                log.info(f"Compression {'succeeded' if success else 'FAILED'} | {trigger_reason}")
            else:
                log.info(
                    f"Check: readiness={readiness['total']:.0f}/{READINESS_THRESHOLD} | "
                    f"elapsed={elapsed_min:.0f}m | capsules={capsules} | captures={captures}"
                )

            # Keep last 50 decisions
            state.setdefault("decisions", []).append(decision)
            state["decisions"] = state["decisions"][-50:]
            state["last_check"] = int(now)
            save_state(state)

        except Exception as e:
            log.error(f"Check cycle error: {e}")

        for _ in range(CHECK_INTERVAL):
            if shutdown:
                break
            time.sleep(1)

    log.info("CCS Adaptive shutdown complete")


if __name__ == "__main__":
    main()
