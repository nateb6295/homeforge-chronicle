#!/usr/bin/env python3
"""Assign memory_type to knowledge_capsules via heuristic classification.

Four types: prediction, observation, question, claim.

Heuristic order (first match wins):
  prediction  — restatement starts with 'PREDICTION #' (chain entries)
  observation — bracket-prefix log records ([WALLET-TX], [FEED], [HA], ...),
                heartbeats (Sentinel:, HAL:, Gemma:), [RESEARCH:capture] tags,
                capture ingest markers
  question    — ends with '?' (conservative: 'when'/'if' clauses are not questions)
  claim       — default (everything else)

Usage:
  capsule_type.py stats                  # show current distribution
  capsule_type.py dry-run [--limit N]    # classify without writing
  capsule_type.py apply [--limit N]      # write memory_type column
  capsule_type.py sample TYPE [--n 10]   # inspect classified samples

The classifier is deliberately conservative — ambiguous items land as 'claim'
rather than mis-typed. Re-run is idempotent (only NULL memory_type updated by
default; pass --retype to overwrite).
"""

import argparse
import os
import re
import sqlite3
import sys
import time

DB = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")

PREDICTION_RE = re.compile(r"^\s*PREDICTION #\d+", re.IGNORECASE)
LOG_PREFIX_RE = re.compile(r"^\s*\[[A-Z][A-Z0-9_\-:]{1,40}\]")
HEARTBEAT_RE = re.compile(r"^(Sentinel|HAL|Gemma|Pulse|Keeper):\s", re.IGNORECASE)
CAPTURE_RE = re.compile(r"^\s*\[RESEARCH:capture\]|^\s*capture\s*#\d+\s", re.IGNORECASE)
def classify(text):
    if not text:
        return "observation"
    if PREDICTION_RE.match(text):
        return "prediction"
    if LOG_PREFIX_RE.match(text) or HEARTBEAT_RE.match(text) or CAPTURE_RE.match(text):
        return "observation"
    stripped = text.strip()
    if stripped.endswith("?"):
        return "question"
    return "claim"


def cmd_stats(_args):
    c = sqlite3.connect(DB)
    rows = c.execute(
        "SELECT COALESCE(memory_type, 'NULL'), COUNT(*) "
        "FROM knowledge_capsules WHERE superseded_at IS NULL "
        "GROUP BY memory_type ORDER BY 2 DESC"
    ).fetchall()
    print(f"{'type':<14} {'count':>8}")
    print("-" * 24)
    for t, n in rows:
        print(f"{t:<14} {n:>8}")


def _iter_targets(c, limit=None, retype=False):
    q = (
        "SELECT id, restatement FROM knowledge_capsules "
        "WHERE superseded_at IS NULL"
    )
    if not retype:
        q += " AND memory_type IS NULL"
    q += " ORDER BY id ASC"
    if limit:
        q += f" LIMIT {int(limit)}"
    return c.execute(q).fetchall()


def cmd_dry_run(args):
    c = sqlite3.connect(DB)
    rows = _iter_targets(c, args.limit, args.retype)
    counts = {}
    for cid, text in rows:
        t = classify(text)
        counts[t] = counts.get(t, 0) + 1
    total = sum(counts.values())
    print(f"classified {total} capsules (dry run)")
    for t in ("claim", "observation", "prediction", "question"):
        n = counts.get(t, 0)
        pct = (n / total * 100) if total else 0
        print(f"  {t:<12} {n:>6} ({pct:4.1f}%)")


def cmd_apply(args):
    c = sqlite3.connect(DB, timeout=60.0)
    c.execute("PRAGMA busy_timeout = 60000")
    rows = _iter_targets(c, args.limit, args.retype)
    if not rows:
        print("nothing to type")
        return
    t0 = time.time()
    updates = [(classify(r[1]), r[0]) for r in rows]
    c.executemany(
        "UPDATE knowledge_capsules SET memory_type = ? WHERE id = ?", updates
    )
    c.commit()
    elapsed = time.time() - t0
    print(f"typed {len(updates)} capsules in {elapsed:.1f}s")


def cmd_sample(args):
    c = sqlite3.connect(DB)
    target = args.type
    rows = _iter_targets(c, limit=None, retype=False)
    picked = []
    for cid, text in rows:
        if classify(text) == target:
            picked.append((cid, text))
            if len(picked) >= args.n:
                break
    for cid, text in picked:
        snippet = re.sub(r"\s+", " ", (text or ""))[:180]
        print(f"[{cid}] {snippet}")


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("stats").set_defaults(fn=cmd_stats)

    d = sub.add_parser("dry-run")
    d.add_argument("--limit", type=int, default=None)
    d.add_argument("--retype", action="store_true")
    d.set_defaults(fn=cmd_dry_run)

    a = sub.add_parser("apply")
    a.add_argument("--limit", type=int, default=None)
    a.add_argument("--retype", action="store_true")
    a.set_defaults(fn=cmd_apply)

    s = sub.add_parser("sample")
    s.add_argument("type", choices=["claim", "observation", "prediction", "question"])
    s.add_argument("--n", type=int, default=10)
    s.set_defaults(fn=cmd_sample)

    args = p.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
