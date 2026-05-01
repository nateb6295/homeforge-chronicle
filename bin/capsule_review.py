#!/usr/bin/env python3
"""Capsule contradiction review — walk, decide, and batch-apply.

The contradiction_detector writes rows to capsule_contradictions. This tool
walks the unapplied ones, lets you inspect each in detail, and applies or
rejects in batch. No more SQL pokes.

Usage:
    capsule_review.py --status
    capsule_review.py --list [--verdict contradicts] [--limit 50]
    capsule_review.py --show ID
    capsule_review.py --apply ID [ID ...]    # honor the judge's loser call
    capsule_review.py --apply-loser ID older|newer [ID older|newer ...]
    capsule_review.py --reject ID [ID ...]
    capsule_review.py --apply-all            # bulk apply every unapplied contradicts
                                             # (prompts unless --yes)

Applying = set superseded_at on loser capsule, record in capsule_contradictions
(applied=1), DEMOTION_STEP confidence drop on loser.
Rejecting = mark applied=-1, no mutation elsewhere.
"""

import argparse
import os
import sqlite3
import sys
import time
import textwrap

DB = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")
DEMOTION_STEP = 0.08


def conn():
    c = sqlite3.connect(DB, timeout=60.0)
    c.execute("PRAGMA busy_timeout = 60000")
    c.row_factory = sqlite3.Row
    return c


def cmd_status(_args):
    c = conn()
    rows = c.execute("""
        SELECT verdict, applied, COUNT(*) n
          FROM capsule_contradictions
         GROUP BY verdict, applied
         ORDER BY verdict, applied
    """).fetchall()
    print(f"{'verdict':<14} {'applied':>8} {'count':>7}")
    print("-" * 32)
    for r in rows:
        state = {0: "pending", 1: "applied", -1: "rejected"}.get(r["applied"], str(r["applied"]))
        print(f"{r['verdict']:<14} {state:>8} {r['n']:>7}")


def cmd_list(args):
    c = conn()
    q = """
        SELECT cc.id, cc.newer_id, cc.older_id, cc.similarity, cc.verdict, cc.loser,
               substr(cc.rationale,1,100) rationale,
               substr(nk.restatement,1,80) newer_text,
               substr(ok.restatement,1,80) older_text
          FROM capsule_contradictions cc
          LEFT JOIN knowledge_capsules nk ON nk.id = cc.newer_id
          LEFT JOIN knowledge_capsules ok ON ok.id = cc.older_id
         WHERE cc.applied = 0
    """
    params = []
    if args.verdict:
        q += " AND cc.verdict = ?"
        params.append(args.verdict)
    q += " ORDER BY cc.verdict, cc.similarity DESC LIMIT ?"
    params.append(args.limit)
    for r in c.execute(q, params).fetchall():
        print(f"[#{r['id']}] {r['verdict']:<12} sim={r['similarity']:.3f} "
              f"loser={r['loser']}")
        print(f"  NEW #{r['newer_id']}: {r['newer_text']}...")
        print(f"  OLD #{r['older_id']}: {r['older_text']}...")
        print(f"  why: {r['rationale']}")
        print()


def cmd_show(args):
    c = conn()
    r = c.execute("""
        SELECT cc.*, nk.restatement newer_text, nk.topic newer_topic,
               nk.confidence_score newer_conf,
               ok.restatement older_text, ok.topic older_topic,
               ok.confidence_score older_conf,
               nk.created_at newer_ts, ok.created_at older_ts
          FROM capsule_contradictions cc
          LEFT JOIN knowledge_capsules nk ON nk.id = cc.newer_id
          LEFT JOIN knowledge_capsules ok ON ok.id = cc.older_id
         WHERE cc.id = ?
    """, (args.id,)).fetchone()
    if not r:
        print(f"no contradiction record #{args.id}")
        return
    state = {0: "pending", 1: "applied", -1: "rejected"}.get(r["applied"], str(r["applied"]))
    print(f"=== contradiction #{r['id']} [{state}] ===")
    print(f"verdict: {r['verdict']}  sim: {r['similarity']:.3f}  loser: {r['loser']}")
    print(f"rationale: {r['rationale']}")
    print()
    print(f"NEWER  #{r['newer_id']}  topic={r['newer_topic']}  conf={r['newer_conf']:.2f}")
    print(textwrap.indent(textwrap.fill(r['newer_text'] or '', width=92), "  "))
    print()
    print(f"OLDER  #{r['older_id']}  topic={r['older_topic']}  conf={r['older_conf']:.2f}")
    print(textwrap.indent(textwrap.fill(r['older_text'] or '', width=92), "  "))


def _apply_single(c, row, loser_override=None):
    """Apply a single contradiction. Returns (ok, message)."""
    loser = loser_override or row["loser"]
    if loser not in ("older", "newer"):
        return False, f"loser={loser}, can't auto-apply"
    loser_id = row["older_id"] if loser == "older" else row["newer_id"]

    cap = c.execute(
        "SELECT id, confidence_score, superseded_at "
        "FROM knowledge_capsules WHERE id = ?", (loser_id,)).fetchone()
    if not cap:
        return False, f"loser capsule #{loser_id} missing"
    if cap["superseded_at"]:
        c.execute("UPDATE capsule_contradictions SET applied = 1 WHERE id = ?",
                  (row["id"],))
        return True, f"#{loser_id} already superseded, marked applied"

    winner_id = row["newer_id"] if loser == "older" else row["older_id"]
    new_conf = max(0.0, cap["confidence_score"] - DEMOTION_STEP)
    now = int(time.time())
    c.execute(
        "UPDATE knowledge_capsules "
        "SET superseded_at = ?, superseded_by = ?, confidence_score = ? "
        "WHERE id = ?",
        (now, winner_id, new_conf, loser_id))
    c.execute("UPDATE capsule_contradictions SET applied = 1 WHERE id = ?",
              (row["id"],))
    return True, f"superseded #{loser_id} → #{winner_id} (conf {cap['confidence_score']:.2f} → {new_conf:.2f})"


def cmd_apply(args):
    c = conn()
    for cid in args.ids:
        row = c.execute("SELECT * FROM capsule_contradictions WHERE id = ?",
                        (cid,)).fetchone()
        if not row:
            print(f"[#{cid}] not found"); continue
        if row["applied"] != 0:
            print(f"[#{cid}] already {row['applied']}, skip"); continue
        ok, msg = _apply_single(c, row)
        print(f"[#{cid}] {'✓' if ok else '✗'} {msg}")
    c.commit()


def cmd_apply_loser(args):
    """Override the judge's loser pick.  args.pairs = [ID, loser, ID, loser, ...]"""
    if len(args.pairs) % 2 != 0:
        print("need ID loser pairs")
        return
    c = conn()
    for i in range(0, len(args.pairs), 2):
        cid = int(args.pairs[i])
        loser = args.pairs[i+1]
        if loser not in ("older", "newer"):
            print(f"[#{cid}] bad loser {loser!r}"); continue
        row = c.execute("SELECT * FROM capsule_contradictions WHERE id = ?",
                        (cid,)).fetchone()
        if not row:
            print(f"[#{cid}] not found"); continue
        ok, msg = _apply_single(c, row, loser_override=loser)
        print(f"[#{cid}/{loser}] {'✓' if ok else '✗'} {msg}")
    c.commit()


def cmd_reject(args):
    c = conn()
    for cid in args.ids:
        c.execute("UPDATE capsule_contradictions SET applied = -1 "
                  "WHERE id = ? AND applied = 0", (cid,))
        print(f"[#{cid}] rejected" if c.total_changes else f"[#{cid}] no change")
    c.commit()


def cmd_apply_all(args):
    c = conn()
    pending = c.execute("""
        SELECT * FROM capsule_contradictions
         WHERE applied = 0 AND verdict = 'contradicts'
           AND loser IN ('older','newer')
         ORDER BY id
    """).fetchall()
    if not pending:
        print("nothing pending")
        return
    print(f"about to apply {len(pending)} contradictions")
    if not args.yes:
        print("pass --yes to commit")
        return
    applied = skipped = 0
    for row in pending:
        ok, msg = _apply_single(c, row)
        if ok:
            applied += 1
        else:
            skipped += 1
        print(f"[#{row['id']}] {'✓' if ok else '✗'} {msg}")
    c.commit()
    print(f"\napplied={applied} skipped={skipped}")


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("--status", aliases=["status"]).set_defaults(fn=cmd_status)

    l = sub.add_parser("list")
    l.add_argument("--verdict", default=None, help="filter by verdict (contradicts/extends/agrees)")
    l.add_argument("--limit", type=int, default=50)
    l.set_defaults(fn=cmd_list)

    s = sub.add_parser("show")
    s.add_argument("id", type=int)
    s.set_defaults(fn=cmd_show)

    a = sub.add_parser("apply")
    a.add_argument("ids", type=int, nargs="+")
    a.set_defaults(fn=cmd_apply)

    al = sub.add_parser("apply-loser")
    al.add_argument("pairs", nargs="+", help="alternating ID loser [ID loser ...]")
    al.set_defaults(fn=cmd_apply_loser)

    r = sub.add_parser("reject")
    r.add_argument("ids", type=int, nargs="+")
    r.set_defaults(fn=cmd_reject)

    aa = sub.add_parser("apply-all")
    aa.add_argument("--yes", action="store_true")
    aa.set_defaults(fn=cmd_apply_all)

    st = sub.add_parser("status")
    st.set_defaults(fn=cmd_status)

    args = p.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
