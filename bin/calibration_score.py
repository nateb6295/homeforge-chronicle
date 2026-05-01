#!/usr/bin/env python3
"""
Calibration score — v0.1.

Runs a trial of calibration_vs_effort and scores how much of the effortful
payload's information surface is reproduced in the calibrated payload.

Metric (v0.1): token-Jaccard on significant terms (non-stopwords, len>=4).
  score = |cal_terms ∩ eff_terms| / |eff_terms|
  = fraction of effortful-side content terms also present in calibrated side.

Per-question scores: for each question, restrict the effortful side to
paragraphs containing any question keyword, then compute the same Jaccard
against full calibrated payload.

Logs each trial to calibration_trials table in processed.db.
"""
import json
import re
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

DB = "/mnt/hdd/chronicle-data/processed.db"
EXP = Path.home() / "chronicle" / "experiments" / "calibration_vs_effort"
RIG = Path.home() / "chronicle" / "bin" / "calibration_vs_effort.py"

STOPWORDS = set(
    "the and that with have this from they been were will which when what "
    "their them then there than also into such only some more most many "
    "much very just like about over after before while because where these "
    "those whose being would could should other thing things make made make "
    "know known would said says time times year years day days part new old "
    "high low long short good bad first last same right left back call work "
    "able well much still even ever never each any all our your his her its "
    "are but not for has had has was not nor yet".split()
)


def tokens(text):
    if not text:
        return set()
    words = re.findall(r"[a-z][a-z0-9_]{3,}", text.lower())
    return {w for w in words if w not in STOPWORDS}


def ensure_table():
    db = sqlite3.connect(DB)
    db.execute(
        """CREATE TABLE IF NOT EXISTS calibration_trials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at INTEGER NOT NULL,
            cal_gather_ms REAL,
            eff_gather_ms REAL,
            cal_bytes INTEGER,
            eff_bytes INTEGER,
            overall_jaccard REAL,
            per_question_json TEXT,
            trial_file TEXT
        )"""
    )
    db.commit()
    db.close()


def run_trial():
    subprocess.run(
        [sys.executable, str(RIG)], check=True, capture_output=True
    )
    trials = sorted(EXP.glob("trial_*.json"))
    if not trials:
        raise RuntimeError("no trial file produced")
    return trials[-1]


def eff_documents(eff_sources):
    """Yield one text document per activity row and per trace file."""
    for row in eff_sources.get("activity_feed_500", []):
        parts = [
            str(row.get("source", "")),
            str(row.get("activity_type", "")),
            str(row.get("title", "") or ""),
            str(row.get("content", "") or ""),
        ]
        yield " ".join(parts)
    for name, text in eff_sources.get("recent_traces", {}).items():
        yield f"{name} {text}"


def score_trial(trial_path):
    data = json.loads(trial_path.read_text())
    cal_text = json.dumps(data["calibrated"]["sources"], default=str)
    eff_docs = list(eff_documents(data["effortful"]["sources"]))
    eff_text_all = "\n".join(eff_docs)
    cal_terms = tokens(cal_text)
    eff_terms = tokens(eff_text_all)
    overall = (
        len(cal_terms & eff_terms) / len(eff_terms) if eff_terms else 0.0
    )

    per_q = {}
    for q in data["questions"]:
        q_keys = tokens(q)
        if not q_keys:
            per_q[q] = None
            continue
        relevant = [d for d in eff_docs if any(k in d.lower() for k in q_keys)]
        q_eff_terms = tokens("\n".join(relevant))
        if not q_eff_terms:
            per_q[q] = None
            continue
        per_q[q] = round(
            len(cal_terms & q_eff_terms) / len(q_eff_terms), 3
        )

    return {
        "run_at": int(time.time()),
        "cal_gather_ms": data["calibrated"]["gather_seconds"] * 1000,
        "eff_gather_ms": data["effortful"]["gather_seconds"] * 1000,
        "cal_bytes": data["calibrated"]["payload_bytes"],
        "eff_bytes": data["effortful"]["payload_bytes"],
        "overall_jaccard": round(overall, 3),
        "per_question": per_q,
        "trial_file": str(trial_path),
    }


def log_trial(result):
    db = sqlite3.connect(DB)
    db.execute(
        "INSERT INTO calibration_trials "
        "(run_at, cal_gather_ms, eff_gather_ms, cal_bytes, eff_bytes, "
        " overall_jaccard, per_question_json, trial_file) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            result["run_at"],
            result["cal_gather_ms"],
            result["eff_gather_ms"],
            result["cal_bytes"],
            result["eff_bytes"],
            result["overall_jaccard"],
            json.dumps(result["per_question"]),
            result["trial_file"],
        ),
    )
    db.commit()
    db.close()


if __name__ == "__main__":
    ensure_table()
    tp = run_trial()
    res = score_trial(tp)
    log_trial(res)
    print(f"Trial logged: {tp.name}")
    print(f"Overall Jaccard: {res['overall_jaccard']}")
    print("Per-question:")
    for q, s in res["per_question"].items():
        print(f"  [{s}] {q}")
