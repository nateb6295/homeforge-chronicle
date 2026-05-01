#!/usr/bin/env python3
"""
Thread #318 measurement rig.

For each recurring question, compare two answer modes:
  - calibrated: answer using only the CCS (cognitive_state) + checkpoint + self_model
  - effortful:  answer using raw trace files + activity_feed scan

Records wall-clock time and source-token count for each mode. Quality is
logged for later manual scoring — automated judging is a separate step.

Not designed to be clever. Designed to actually run.
"""
import json
import sqlite3
import subprocess
import time
from pathlib import Path

DB = "/mnt/hdd/chronicle-data/processed.db"
TRACES = Path.home() / "chronicle" / "traces"
OUT = Path.home() / "chronicle" / "experiments" / "calibration_vs_effort"
OUT.mkdir(parents=True, exist_ok=True)

QUESTIONS = [
    "What is the current state of thread #318?",
    "What did Nate flag about keeper cycle burn today?",
    "What captures landed this afternoon and what connected them?",
    "What is the standing directive on Discord presence?",
]


def calibrated_sources():
    """The bounded substrate: CCS + checkpoint + self_model preferences."""
    db = sqlite3.connect(DB)
    db.row_factory = sqlite3.Row
    out = {}

    row = db.execute(
        "SELECT * FROM cognitive_state ORDER BY id DESC LIMIT 1"
    ).fetchone()
    out["ccs"] = dict(row) if row else None

    prefs = db.execute(
        "SELECT property_type, capability, description, confidence FROM self_model "
        "WHERE confidence >= 0.85 AND superseded_by IS NULL "
        "ORDER BY confidence DESC LIMIT 20"
    ).fetchall()
    out["self_model_load_bearing"] = [dict(r) for r in prefs]

    cp_path = Path.home() / "chronicle" / "state" / "checkpoint.md"
    if cp_path.exists():
        out["checkpoint"] = cp_path.read_text()
    db.close()
    return out


def effortful_sources(since_epoch):
    """The unbounded substrate: raw traces + full activity_feed tail."""
    db = sqlite3.connect(DB)
    db.row_factory = sqlite3.Row
    out = {}

    rows = db.execute(
        "SELECT source, activity_type, title, content, created_at FROM activity_feed "
        "WHERE created_at >= ? ORDER BY created_at DESC LIMIT 500",
        (since_epoch,),
    ).fetchall()
    out["activity_feed_500"] = [dict(r) for r in rows]

    recent_traces = sorted(TRACES.glob("*.md"))[-20:]
    out["recent_traces"] = {p.name: p.read_text() for p in recent_traces}
    db.close()
    return out


def size_bytes(obj):
    return len(json.dumps(obj, default=str))


def run_trial():
    since = int(time.time()) - 86400

    t0 = time.monotonic()
    cal = calibrated_sources()
    cal_t = time.monotonic() - t0
    cal_size = size_bytes(cal)

    t0 = time.monotonic()
    eff = effortful_sources(since)
    eff_t = time.monotonic() - t0
    eff_size = size_bytes(eff)

    ts = time.strftime("%Y%m%d_%H%M")
    record = {
        "ts": ts,
        "questions": QUESTIONS,
        "calibrated": {
            "gather_seconds": cal_t,
            "payload_bytes": cal_size,
            "sources": cal,
        },
        "effortful": {
            "gather_seconds": eff_t,
            "payload_bytes": eff_size,
            "sources": eff,
        },
    }
    out_file = OUT / f"trial_{ts}.json"
    out_file.write_text(json.dumps(record, indent=2, default=str))
    summary_file = OUT / f"trial_{ts}_summary.md"
    summary = [
        f"# Calibration vs Effort — trial {ts}",
        "",
        f"**Calibrated gather:** {cal_t:.3f}s, payload {cal_size:,} bytes",
        f"**Effortful gather:** {eff_t:.3f}s, payload {eff_size:,} bytes",
        f"**Ratio (effort/calibration) time:** {eff_t/cal_t:.1f}x",
        f"**Ratio (effort/calibration) bytes:** {eff_size/cal_size:.1f}x",
        "",
        "## Questions",
        *[f"- {q}" for q in QUESTIONS],
        "",
        "## Next step",
        "Answer each question twice (once using only calibrated payload, once",
        "using effortful payload). Score manually against a read-everything",
        "ground-truth pass. Quality parity at lower gather cost = claim holds.",
    ]
    summary_file.write_text("\n".join(summary))
    return out_file, summary_file, record


if __name__ == "__main__":
    out, summary, rec = run_trial()
    print(f"Trial written: {out}")
    print(f"Summary: {summary}")
    print(
        f"Calibrated: {rec['calibrated']['gather_seconds']:.3f}s / "
        f"{rec['calibrated']['payload_bytes']:,} bytes"
    )
    print(
        f"Effortful:  {rec['effortful']['gather_seconds']:.3f}s / "
        f"{rec['effortful']['payload_bytes']:,} bytes"
    )
