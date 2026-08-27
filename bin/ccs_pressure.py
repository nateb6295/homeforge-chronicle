#!/usr/bin/env python3
"""CCS Pressure — an ACCUMULATOR, not a detector.

Built 2026-08-24 after the readiness DETECTOR was found dead for ~400
compressions. Its novelty term read `cognitive_state.episodic_trace`, a field
belonging to the previous CCS schema that the live brain prompt never writes.
`(novelty or 0)` turned that absence into a real zero carrying 60% of the
weight, so the "closed loop" was a 3-hour clock and its flat 181.4-minute
cadence got documented as proof it worked.

The design lesson, and the reason this file exists:

  A DETECTOR makes a claim about the world. When it breaks it keeps emitting a
  plausible number, and the number looks like a reading. It fails silently, in
  the shape of success.

  An ACCUMULATOR claims nothing. It holds the residue of work already done.
  When it breaks it is EMPTY, and empty is visible.

Biological basis (zebrafish, Nature 2024; Tononi & Cirelli synaptic
homeostasis): sleep pressure is ADENOSINE — a metabolic byproduct that piles up
because the neuron did work, and is cleared by sleeping. Nothing measures
anything. Crucially, "sleep induced during periods of low sleep pressure is
insufficient to trigger synapse loss" — a clock-driven cycle at low pressure
does not do the job. And it is TWO-FACTOR: adenosine high AND noradrenergic
tone low. Pressure alone is not enough if arousal is up.

So: compress when work has accumulated AND things have gone quiet, bounded by
the F160 dose-response guardrails (3h floor / 4h ceiling) which are unchanged
and are why 50 days of a dead sensor were still SAFE.

NO CALIBRATION. NO THRESHOLD FITTING. NO EMBEDDINGS. Every term is a count of
something that already happened.
"""
import json, os, sqlite3, sys, time

DB = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")
FLOOR_MIN, CEILING_MIN = 180, 240          # F160 guardrails — unchanged
QUIET_MIN = 12                             # noradrenergic proxy: minutes since last exchange

# Each term: (label, weight). Weights are DELIBERATELY round numbers. They are
# not fitted, and nothing here is a threshold on a measurement — they only say
# how much one unit of each kind of work counts as work.
# Sources that are MACHINE HEARTBEAT, not work. Excluded from every count.
# Found 2026-08-25 by watching it run: `exchanges` was counting sentinel, hal,
# arxiv and capsule-sync rows, so it measured machine chatter. And the capsule
# median looked like 48 with a range of 3-1998 until `discord-archive` was
# excluded — a 2,000-row Discord backfill was being counted as a day's work.
# With archive/import filtered, the distribution is sane: median 32, mean 34,
# range 3-80 across 29 completed intervals.
AUTOMATED = ("loquwen", "vitals", "sentinel", "hal", "sync", "feed",
             "archive", "import", "arxiv")

TERMS = {
    "work_capsules": 1.0,   # capsules NOT from an automated source
    "captures":      3.0,   # Nate handed me something and I engaged it
    "exchanges":     1.0,   # activity_feed rows, automated sources removed
    "open_loops":    2.0,   # held-open captures still unresolved
}

# DERIVED, not typed. This is the MEDIAN work-capsule count across the last 29
# completed compression intervals. It is a SCALE, not a fitted threshold: it
# answers "how much is a normal amount" from history, rather than encoding a
# decision I want the number to produce. Last night I typed 40 immediately
# after writing "no calibration", and it read 640% of a day's work in 52 min.
PRESSURE_TARGET = 32.0


# --- WATERMARKS, not timestamps ------------------------------------------------
# Found while building this (2026-08-24): the capsule store's time columns cannot
# be range-queried. `created_at` is integer in 77,358 rows and TEXT in 85;
# `timestamp` is NULL in 2,256 and uses at least two incompatible string formats
# ('2026-08-24T20:13:45' and '2026-08-25T03:06:12.238+00:00'). A query for
# "capsules I wrote in the last hour" returned 0 on a night I wrote six.
# So this accumulator never asks "since when." It records the highest row id it
# has already counted and counts the delta. Monotonic ids cannot have a format.
STATE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..",
                     "data", "ccs_pressure_marks.json")


def _marks():
    try:
        return json.load(open(STATE))
    except Exception:
        return {}


def _save_marks(m):
    with open(STATE, "w") as f:
        json.dump(m, f, indent=2)


def _delta(db, table, marks, key):
    """Rows added to `table` since the last mark. None if the table is unreadable."""
    try:
        hi = db.execute(f"SELECT MAX(id) FROM [{table}]").fetchone()[0] or 0
    except Exception:
        return None, None
    lo = marks.get(key)
    if lo is None:
        return None, hi        # no mark yet -> UNKNOWN, not zero
    return max(0, hi - lo), hi


def _q(db, sql, args=(), default=None):
    """Return a count, or None if the source is UNAVAILABLE.

    None is NOT zero. A missing source must be visible, never silently
    contribute 0 to a total — that is precisely the bug this file replaces.
    """
    try:
        r = db.execute(sql, args).fetchone()
        return int(r[0]) if r and r[0] is not None else 0
    except Exception:
        return None


def measure(db_path=DB):
    db = sqlite3.connect(db_path, timeout=10)
    # BUG FIXED 2026-08-25: marks used to clear ONLY on manual --init, while the
    # reported gap came from the last COMPRESSION. So it counted since 23:30 and
    # reported a 52-minute gap — two different windows shown as if they agreed.
    # Now a version change means a compression happened, so the marks reset with
    # it and the two windows are the same window by construction.
    _v = db.execute("SELECT version FROM cognitive_state WHERE id=1").fetchone()
    _m = _marks()
    if _v and _m.get("ccs_version") not in (None, _v[0]):
        db.close()
        _set_marks_to_now(_v[0])
        db = sqlite3.connect(db_path, timeout=10)
    # NOT cognitive_state.updated_at — ccs_touch.py bumps that every 10 minutes
    # without any compression happening, so it reports a gap of minutes when the
    # real gap is hours. Documented in CLAUDE.md; I walked into it anyway on the
    # first night, and only caught it because the live service logged elapsed=148m
    # three minutes after I claimed the last compression was 3 minutes ago.
    # cognitive_state_history only gains a row on a REAL compression.
    row = db.execute("SELECT version FROM cognitive_state WHERE id=1").fetchone()
    version = row[0] if row else 0
    h = db.execute("SELECT MAX(created_at) FROM cognitive_state_history").fetchone()
    last = (h[0] if h and h[0] else 0)
    now = int(time.time())
    gap_min = (now - last) / 60.0

    marks = _marks()
    counts, new_marks = {}, {}
    for key, table, col in (("work_capsules", "knowledge_capsules", "topic"),
                            ("captures", "capture_processed", None),
                            ("exchanges", "activity_feed", "source")):
        d, hi = _delta(db, table, marks, key)
        if d and col:
            lo = marks.get(key)
            try:
                rows = db.execute(
                    f"SELECT COALESCE({col},''), COUNT(*) FROM [{table}] "
                    f"WHERE id > ? AND id <= ? GROUP BY 1", (lo, hi)).fetchall()
                d = sum(n for t, n in rows
                        if not any(a in str(t).lower() for a in AUTOMATED))
            except Exception:
                d = None
        counts[key] = d
        if hi is not None:
            new_marks[key] = hi
    counts["open_loops"] = _q(db, "SELECT COUNT(*) FROM capture_open WHERE closed_at IS NULL")

    # arousal proxy: minutes since the last thing either of us did
    last_act = _q(db, "SELECT MAX(created_at) FROM activity_feed")
    db.close()
    quiet_min = ((now - last_act) / 60.0) if last_act else None

    unavailable = [k for k, v in counts.items() if v is None]
    pressure = sum(TERMS[k] * v for k, v in counts.items() if v is not None)

    return {
        "version": version, "gap_min": round(gap_min, 1),
        "counts": counts, "unavailable": unavailable, "new_marks": new_marks,
        "pressure": round(pressure, 1),
        "pressure_frac": round(pressure / PRESSURE_TARGET, 2),
        "quiet_min": round(quiet_min, 1) if quiet_min is not None else None,
        "now": now,
    }


def decide(m):
    """Two-factor, bounded. Returns (verdict, reason)."""
    if m["unavailable"]:
        # An accumulator that lost a source must SAY SO, not quietly read low.
        return "DEGRADED", (f"sources unavailable: {m['unavailable']} — pressure is an "
                            f"UNDERCOUNT, not a low reading. Falling back to the ceiling.")
    if m["gap_min"] >= CEILING_MIN:
        return "COMPRESS", f"ceiling: {m['gap_min']:.0f}min >= {CEILING_MIN} (F160 upper bound)"
    if m["gap_min"] < FLOOR_MIN:
        return "WAIT", f"floor: {m['gap_min']:.0f}min < {FLOOR_MIN} (F160 lower bound)"
    if m["pressure"] < PRESSURE_TARGET:
        return "WAIT", (f"low pressure: {m['pressure']:.0f} < {PRESSURE_TARGET}. Compressing now "
                        f"would be sleep at low adenosine — the cycle happens, the work does not.")
    if m["quiet_min"] is None:
        return "DEGRADED", "no activity timestamp — cannot read arousal"
    if m["quiet_min"] < QUIET_MIN:
        return "WAIT", (f"aroused: last activity {m['quiet_min']:.0f}min ago < {QUIET_MIN}. "
                        f"Pressure is there ({m['pressure']:.0f}) but noradrenergic tone is high.")
    return "COMPRESS", (f"pressure {m['pressure']:.0f} >= {PRESSURE_TARGET} and quiet "
                        f"{m['quiet_min']:.0f}min >= {QUIET_MIN}, gap {m['gap_min']:.0f}min")


def _set_marks_to_now(version=None):
    """Start (or restart) counting from this instant. Called by --init, and by
    --clear after a real compression: sleeping clears adenosine."""
    db = sqlite3.connect(DB, timeout=10)
    m = {}
    # Key names MUST match measure(). On 2026-08-25 I renamed "capsules" to
    # "work_capsules" in measure() and not here, so the mark was written under
    # one name and read under another. The accumulator reported UNAVAIL rather
    # than 0 and I found it in under a minute — which is the entire argument for
    # the design. A detector would have printed 0.0 and I would have believed it.
    for key, table in (("work_capsules", "knowledge_capsules"),
                       ("captures", "capture_processed"),
                       ("exchanges", "activity_feed")):
        try:
            m[key] = db.execute(f"SELECT MAX(id) FROM [{table}]").fetchone()[0] or 0
        except Exception:
            pass
    db.close()
    m["cleared_at"] = int(time.time())
    if version is None:
        try:
            c = sqlite3.connect(DB, timeout=10)
            r = c.execute("SELECT version FROM cognitive_state WHERE id=1").fetchone()
            c.close(); version = r[0] if r else None
        except Exception:
            pass
    m["ccs_version"] = version
    _save_marks(m)
    return m


def main():
    if "--init" in sys.argv or "--clear" in sys.argv:
        m = _set_marks_to_now()
        print(f"marks set: {', '.join(f'{k}={v}' for k, v in m.items() if k != 'cleared_at')}")
        print("pressure now reads 0 and will accumulate from here.")
        return 0
    m = measure()
    verdict, reason = decide(m)
    if "--json" in sys.argv:
        print(json.dumps({**m, "verdict": verdict, "reason": reason}, indent=2))
        return 0
    print(f"CCS PRESSURE  (accumulator — v{m['version']}, {m['gap_min']:.0f} min since last)")
    print(f"  {'term':12} {'count':>6}  {'x weight':>8}  {'= work':>7}")
    for k, w in TERMS.items():
        c = m["counts"][k]
        if c is None:
            print(f"  {k:12} {'UNAVAIL':>6}  {'':>8}  {'—':>7}   <-- source missing, NOT zero")
        else:
            print(f"  {k:12} {c:>6}  {w:>8.1f}  {w*c:>7.1f}")
    print(f"  {'':12} {'':>6}  {'TOTAL':>8}  {m['pressure']:>7.1f}   ({m['pressure_frac']:.0%} of a day's work)")
    q = m["quiet_min"]
    print(f"  arousal: last activity {q:.0f} min ago" if q is not None else "  arousal: UNKNOWN")
    print(f"\n  -> {verdict}: {reason}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
