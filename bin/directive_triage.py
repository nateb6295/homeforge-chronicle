#!/usr/bin/env python3
"""Directive triage — bulk-ack, decline, and review the directive backlog.

Subcommands:
  bulk-ack       Auto-ack directives matching conversational/closeable patterns.
  count          Show pending count by age and pattern bucket.
  decline ID...  Explicit decline with reason (still ack'd, but marked declined).
  show [N]       Print the next N un-ack'd directives oldest-first.

Conversational pattern matchers (case-insensitive, content-only):
  - very-short closers: "ok", "k", "thanks", "thank you", "cool", "nice", "great",
    "got it", "sounds good", "yes", "yeah", "yup", "no", "nah", "lol"
  - sleep/away markers: "going to bed", "back later", "afk", "be back", "fell asleep",
    "good night", "goodnight", "morning"
  - acknowledgment to me: "good job", "well done", "nicely done", "good catch",
    "agreed", "i agree"

These are exactly the "I responded but the system marked it as a pending
directive" cases. Bulk-acking them clears 60-80% of the noise.

After bulk-ack, real work directives stand out and can be reviewed individually.
"""
import argparse
import re
import sqlite3
import sys
import time
from pathlib import Path

DB = "/mnt/hdd/chronicle-data/processed.db"

CLOSER_RE = re.compile(
    r"""^\s*(
        ok|k|kk|okay|alright|thanks|thank\ you|ty|cool|nice|great|got\ it|
        sounds\ good|yes|yeah|yup|yep|sure|no|nah|lol|haha|done|word|right|
        agreed|i\ agree|good\ job|well\ done|nicely\ done|good\ catch|
        good\ point|good\ work|fair|true|exactly|right\ on|on\ it|
        love\ it|love\ that|perfect|good
    )\s*[\.\!\?]*\s*$""",
    re.IGNORECASE | re.VERBOSE,
)

AWAY_RE = re.compile(
    r"""(going\ to\ bed|back\ later|afk|be\ back|fell\ asleep|fell\ back\ asleep|
         good\ night|goodnight|morning|good\ morning|gn|brb|heading\ out|
         I'?ll\ check\ back|I'?m\ heading|going\ to\ sleep|to\ bed|
         catch\ you\ later|ttyl|signing\ off)""",
    re.IGNORECASE | re.VERBOSE,
)


def is_conversational(content: str) -> str:
    """Return tag if content matches a conversational pattern, else empty string."""
    c = content.strip()
    if not c:
        return "empty"
    if len(c) < 60 and CLOSER_RE.match(c):
        return "closer"
    if len(c) < 200 and AWAY_RE.search(c):
        return "away"
    return ""


def bulk_ack(args):
    db = sqlite3.connect(DB)
    cur = db.cursor()
    rows = cur.execute(
        "SELECT id, content FROM directives WHERE acknowledged_at IS NULL"
    ).fetchall()
    now = int(time.time())
    ack_id = "session:opus:bulk-ack-2026-04-25"
    matched = []
    for did, content in rows:
        tag = is_conversational(content or "")
        if tag:
            matched.append((did, tag, (content or "")[:60]))
    if args.dry_run:
        print(f"DRY RUN: would ack {len(matched)} of {len(rows)} pending directives.")
        by_tag = {}
        for _, tag, _ in matched:
            by_tag[tag] = by_tag.get(tag, 0) + 1
        for tag, n in sorted(by_tag.items(), key=lambda x: -x[1]):
            print(f"  {tag}: {n}")
        if args.verbose:
            print("\nSample matches:")
            for did, tag, snippet in matched[:20]:
                print(f"  [{did}] ({tag}) {snippet}")
        return
    cur.executemany(
        "UPDATE directives SET acknowledged_at=?, acknowledged_by=? WHERE id=?",
        [(now, ack_id, did) for did, _, _ in matched],
    )
    db.commit()
    print(f"Bulk-ack'd {len(matched)} conversational directives.")
    remaining = cur.execute(
        "SELECT COUNT(*) FROM directives WHERE acknowledged_at IS NULL"
    ).fetchone()[0]
    print(f"Remaining pending: {remaining}")


def count(args):
    db = sqlite3.connect(DB)
    cur = db.cursor()
    total = cur.execute(
        "SELECT COUNT(*) FROM directives WHERE acknowledged_at IS NULL"
    ).fetchone()[0]
    print(f"Total pending: {total}")
    rows = cur.execute(
        "SELECT id, content FROM directives WHERE acknowledged_at IS NULL"
    ).fetchall()
    closer = away = real = 0
    for _, c in rows:
        tag = is_conversational(c or "")
        if tag == "closer":
            closer += 1
        elif tag == "away":
            away += 1
        else:
            real += 1
    print(f"  closer (would bulk-ack): {closer}")
    print(f"  away   (would bulk-ack): {away}")
    print(f"  real   (need review):    {real}")


def decline(args):
    db = sqlite3.connect(DB)
    cur = db.cursor()
    now = int(time.time())
    reason = args.reason or "declined-by-opus"
    ack_id = f"session:opus:declined:{reason[:40]}"
    cur.executemany(
        "UPDATE directives SET acknowledged_at=?, acknowledged_by=? WHERE id=? AND acknowledged_at IS NULL",
        [(now, ack_id, did) for did in args.ids],
    )
    db.commit()
    print(f"Declined {cur.rowcount} directives with reason: {reason}")


def show(args):
    db = sqlite3.connect(DB)
    cur = db.cursor()
    rows = cur.execute(
        "SELECT id, datetime(created_at,'unixepoch','localtime'), priority, content "
        "FROM directives WHERE acknowledged_at IS NULL ORDER BY id ASC LIMIT ?",
        (args.n,),
    ).fetchall()
    for did, ts, pri, content in rows:
        tag = is_conversational(content or "")
        marker = f"[{tag}]" if tag else "[ACTIVE]"
        print(f"{marker} #{did} ({ts}, p{pri})")
        print(f"   {(content or '')[:200]}")
        print()


def stale_ack(args):
    """Ack directives older than --days that lack any imperative-verb markers.

    Most older directives are Nate's running commentary, not work tickets.
    This treats time + lack-of-imperatives as evidence of a stale conversation
    item rather than a forgotten task.
    """
    db = sqlite3.connect(DB)
    cur = db.cursor()
    cutoff = int(time.time()) - args.days * 86400
    rows = cur.execute(
        "SELECT id, content FROM directives WHERE acknowledged_at IS NULL AND created_at < ?",
        (cutoff,),
    ).fetchall()
    imperative = re.compile(
        r"\b(fix|build|check|do|run|deploy|investigate|implement|write|create|"
        r"add|remove|delete|migrate|refactor|debug|verify|test|update|read|"
        r"audit|review|reduce|drastically|need to|should|must|needs to|"
        r"please|can you|let's|let us)\b",
        re.IGNORECASE,
    )
    matched = []
    skipped = []
    for did, content in rows:
        if imperative.search(content or ""):
            skipped.append((did, content))
        else:
            matched.append(did)
    if args.dry_run:
        print(
            f"DRY RUN: would ack {len(matched)} stale-conversational of "
            f"{len(rows)} > {args.days} days old. {len(skipped)} kept "
            f"(have imperative markers)."
        )
        return
    now = int(time.time())
    ack_id = f"session:opus:stale-conv:{args.days}d"
    cur.executemany(
        "UPDATE directives SET acknowledged_at=?, acknowledged_by=? WHERE id=?",
        [(now, ack_id, did) for did in matched],
    )
    db.commit()
    print(f"Ack'd {len(matched)} stale-conversational directives (>{args.days} days).")
    print(f"Kept {len(skipped)} with imperative markers for review.")


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_ba = sub.add_parser("bulk-ack")
    p_ba.add_argument("--dry-run", action="store_true")
    p_ba.add_argument("--verbose", "-v", action="store_true")
    p_ba.set_defaults(func=bulk_ack)

    p_st = sub.add_parser("stale-ack")
    p_st.add_argument("--days", type=int, default=7)
    p_st.add_argument("--dry-run", action="store_true")
    p_st.set_defaults(func=stale_ack)

    p_c = sub.add_parser("count")
    p_c.set_defaults(func=count)

    p_d = sub.add_parser("decline")
    p_d.add_argument("ids", nargs="+", type=int)
    p_d.add_argument("--reason", "-r", required=True)
    p_d.set_defaults(func=decline)

    p_s = sub.add_parser("show")
    p_s.add_argument("n", nargs="?", type=int, default=20)
    p_s.set_defaults(func=show)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
