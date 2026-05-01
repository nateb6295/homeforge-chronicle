#!/usr/bin/env python3
"""Prediction tracker — list, mark outcomes, record resolution.

Predictions live in knowledge_capsules with restatements shaped like
"PREDICTION #N (conf=X): <claim>. Deadline: <date>." or
"PREDICTION #N ADJUSTED: 0.X -> 0.Y. Reason: ..." — a chain per number.

This tool groups capsules by prediction number, shows the latest adjustment
for each, and lets you record a final outcome once a prediction resolves.
A terminal outcome isn't a supersession (that's what the contradiction
detector handles for intermediate updates) — it's a resolution event the
survival filter has been missing.

Usage:
    prediction_check.py list
    prediction_check.py show N           # show full chain for PREDICTION #N
    prediction_check.py mark N hit|miss|partial --note "..." [--source "..."]
    prediction_check.py outcomes         # all recorded outcomes
    prediction_check.py stats            # hit rate, counts
"""

import argparse
import os
import re
import sqlite3
import sys
import time
import textwrap

DB = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")

PRED_RE = re.compile(r"PREDICTION #(\d+)")


def conn():
    c = sqlite3.connect(DB, timeout=60.0)
    c.execute("PRAGMA busy_timeout = 60000")
    c.row_factory = sqlite3.Row
    return c


def ensure_schema(c):
    c.executescript("""
    CREATE TABLE IF NOT EXISTS prediction_outcomes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        prediction_num INTEGER NOT NULL,
        outcome TEXT NOT NULL CHECK(outcome IN ('hit','miss','partial')),
        note TEXT,
        source TEXT,
        resolved_at INTEGER NOT NULL,
        final_capsule_id INTEGER
    );
    CREATE INDEX IF NOT EXISTS idx_pred_outcome_num
        ON prediction_outcomes(prediction_num);
    """)
    c.commit()


def collect_predictions(c):
    """Group non-superseded prediction capsules by prediction number."""
    rows = c.execute("""
        SELECT id, restatement, confidence_score, created_at, topic
          FROM knowledge_capsules
         WHERE superseded_at IS NULL
           AND (restatement LIKE 'PREDICTION #%'
                OR topic LIKE 'prediction%'
                OR topic LIKE 'predictions%')
         ORDER BY created_at ASC
    """).fetchall()
    groups = {}
    for r in rows:
        m = PRED_RE.search(r["restatement"] or "")
        if not m:
            continue
        n = int(m.group(1))
        groups.setdefault(n, []).append(dict(r))
    return groups


def latest_conf(chain):
    """Extract latest 'ADJUSTED: A -> B' end value, fallback to capsule conf."""
    for cap in reversed(chain):
        m = re.search(r"ADJUSTED: [\d.]+ -> ([\d]+(?:\.[\d]+)?)", cap["restatement"])
        if m:
            return float(m.group(1))
    return chain[-1]["confidence_score"]


def cmd_list(args):
    c = conn()
    ensure_schema(c)
    groups = collect_predictions(c)
    resolved = {r["prediction_num"]: r["outcome"] for r in c.execute(
        "SELECT prediction_num, outcome FROM prediction_outcomes").fetchall()}

    print(f"{'#':>4} {'chain':>6} {'conf':>5} {'state':<9} {'last':<17} claim")
    print("-" * 100)
    for n in sorted(groups.keys()):
        chain = groups[n]
        conf = latest_conf(chain)
        state = resolved.get(n, "open")
        last_ts = time.strftime("%Y-%m-%d %H:%M",
                                time.localtime(chain[-1]["created_at"]))
        # Initial claim is usually the first capsule without "ADJUSTED"
        initial = next((c for c in chain
                        if "ADJUSTED" not in c["restatement"]), chain[0])
        claim = re.sub(r"\s+", " ",
                       (initial["restatement"] or "")[:90])
        print(f"{n:>4} {len(chain):>6} {conf:>5.2f} {state:<9} {last_ts:<17} {claim}")


def cmd_show(args):
    c = conn()
    ensure_schema(c)
    groups = collect_predictions(c)
    n = args.num
    if n not in groups:
        print(f"no chain for PREDICTION #{n}")
        return
    chain = groups[n]
    print(f"=== PREDICTION #{n} — {len(chain)} entries ===\n")
    for cap in chain:
        ts = time.strftime("%Y-%m-%d %H:%M",
                           time.localtime(cap["created_at"]))
        print(f"[{ts}] #{cap['id']} conf={cap['confidence_score']:.2f}")
        print(textwrap.indent(
            textwrap.fill(cap["restatement"], width=92), "  "))
        print()

    # Show any recorded outcome
    out = c.execute(
        "SELECT * FROM prediction_outcomes WHERE prediction_num = ? "
        "ORDER BY resolved_at DESC", (n,)).fetchall()
    if out:
        print("=== RESOLVED ===")
        for r in out:
            ts = time.strftime("%Y-%m-%d %H:%M",
                               time.localtime(r["resolved_at"]))
            print(f"[{ts}] {r['outcome']}: {r['note']}")
            if r["source"]:
                print(f"  source: {r['source']}")


def cmd_mark(args):
    c = conn()
    ensure_schema(c)
    groups = collect_predictions(c)
    n = args.num
    if n not in groups:
        print(f"no chain for PREDICTION #{n}")
        return
    final_id = groups[n][-1]["id"]
    now = int(time.time())
    c.execute(
        "INSERT INTO prediction_outcomes "
        "(prediction_num, outcome, note, source, resolved_at, final_capsule_id) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (n, args.outcome, args.note, args.source, now, final_id))
    c.commit()
    print(f"[#{n}] recorded {args.outcome}")


def cmd_outcomes(args):
    c = conn()
    ensure_schema(c)
    rows = c.execute("""
        SELECT prediction_num, outcome, note, source, resolved_at
          FROM prediction_outcomes
         ORDER BY resolved_at DESC
    """).fetchall()
    for r in rows:
        ts = time.strftime("%Y-%m-%d %H:%M",
                           time.localtime(r["resolved_at"]))
        print(f"[{ts}] PREDICTION #{r['prediction_num']} — {r['outcome']}")
        print(f"  {r['note']}")
        if r["source"]:
            print(f"  source: {r['source']}")


def cmd_stats(args):
    c = conn()
    ensure_schema(c)
    groups = collect_predictions(c)
    open_n = len(groups)
    rows = c.execute(
        "SELECT outcome, COUNT(*) n FROM prediction_outcomes "
        "GROUP BY outcome").fetchall()
    resolved = {r["outcome"]: r["n"] for r in rows}
    hit = resolved.get("hit", 0)
    miss = resolved.get("miss", 0)
    partial = resolved.get("partial", 0)
    total_resolved = hit + miss + partial
    rate = hit / total_resolved if total_resolved else 0.0
    print(f"Chains tracked:       {open_n}")
    print(f"Resolved outcomes:    {total_resolved} "
          f"(hit={hit} miss={miss} partial={partial})")
    if total_resolved:
        print(f"Hit rate:             {rate:.1%}")


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("list").set_defaults(fn=cmd_list)

    s = sub.add_parser("show")
    s.add_argument("num", type=int)
    s.set_defaults(fn=cmd_show)

    m = sub.add_parser("mark")
    m.add_argument("num", type=int)
    m.add_argument("outcome", choices=["hit", "miss", "partial"])
    m.add_argument("--note", required=True)
    m.add_argument("--source", default=None)
    m.set_defaults(fn=cmd_mark)

    sub.add_parser("outcomes").set_defaults(fn=cmd_outcomes)
    sub.add_parser("stats").set_defaults(fn=cmd_stats)

    args = p.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
