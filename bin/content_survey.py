#!/usr/bin/env python3
"""What is actually INSIDE each table. Not edges — contents.

Built 2026-08-25, immediately after the failure it exists to prevent.

I ran connection_audit.py, which examines who WRITES and who READS each table.
discord_archive passed: it has writers, it has readers, no orphan, no mixed
types. Structurally healthy. Then I searched knowledge_capsules for our
publication record, found three pointers, concluded 91% of it was unrecoverable,
and wrote UNKNOWN into a manifest.

The complete record was in discord_archive. 97,820 rows. One query away. I had
listed that table by row count an hour earlier and never read a line of it.

An edge audit tells you the plumbing is connected. It cannot tell you there is
an answer in the tank. This samples CONTENT and says what each table is FOR, in
words, so that "where would that live" has an answer that is not a guess.

    content_survey.py             every table, what's in it
    content_survey.py --unread    tables nothing in bin/ ever SELECTs from
"""
import os, re, glob, sqlite3, sys

DB = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")


def readers_of(table):
    """Files that actually SELECT from this table (not merely mention it)."""
    out = []
    for f in glob.glob(os.path.join(os.path.dirname(os.path.abspath(__file__)), "*.py")):
        src = open(f, errors="ignore").read()
        if re.search(rf"FROM\s+\[?{table}\b|JOIN\s+\[?{table}\b", src, re.I):
            out.append(os.path.basename(f))
    return out


def describe(db, t, cols):
    """One line of what the rows actually contain."""
    textcols = []
    for c in cols:
        try:
            v = db.execute(f"SELECT [{c}] FROM [{t}] WHERE [{c}] IS NOT NULL "
                           f"AND typeof([{c}])='text' AND length([{c}])>25 LIMIT 1").fetchone()
            if v:
                textcols.append((c, " ".join(str(v[0]).split())[:90]))
        except Exception:
            pass
    return textcols[:2]


def main():
    db = sqlite3.connect(DB)
    db.row_factory = None
    tables = [r[0] for r in db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '%_fts%' ORDER BY name")]
    unread_only = "--unread" in sys.argv
    shown = 0
    for t in tables:
        try:
            n = db.execute(f"SELECT COUNT(*) FROM [{t}]").fetchone()[0]
        except Exception:
            continue
        if n == 0:
            continue
        rd = readers_of(t)
        if unread_only and rd:
            continue
        cols = [r[1] for r in db.execute(f"PRAGMA table_info([{t}])")]
        samples = describe(db, t, cols)
        flag = "  <-- NOTHING READS THIS" if not rd else f"  ({len(rd)} readers)"
        print(f"\n{t}  —  {n:,} rows{flag}")
        for c, v in samples:
            print(f"    {c}: {v}")
        shown += 1
    db.close()
    print(f"\n{shown} tables shown."
          + ("  These hold data nothing queries." if unread_only else ""))


if __name__ == "__main__":
    main()
