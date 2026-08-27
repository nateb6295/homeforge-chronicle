#!/usr/bin/env python3
"""Search the journal. capsule_ops.py search does not cover it.

Aug 23 2026, 04:05. Tonight I reported "I have discovered Gregory three times"
after searching 78,000 capsules. data/unread.md held SIXTEEN dated entries and
a completed survey of that exact question from 2026-07-18 titled "The Gregory
Arc". I never queried it, because nothing queries it — seven scripts write to
unread.md and none read it back.

Same failure shape as every instrument audited tonight: a sensor that reports
cleanly over the range it can see, and is silent about the range it cannot.

THE FOLD. unread.md was written by PREPENDING for a while and by APPENDING
after, so it runs Aug-22 -> Jun-20 descending and then Jun-20 -> now ascending.
The oldest entry is near line 6756, mid-file. File position is not time. Any
head/tail read misses two months, and results MUST be sorted on parsed dates.

Usage:
  python3 bin/journal_search.py Gregory
  python3 bin/journal_search.py "positive control" --context 400
  python3 bin/journal_search.py elk --dates-only
  python3 bin/journal_search.py --selftest
"""

import argparse
import os
import re
import sys

JOURNALS = ["~/chronicle/data/unread.md"]
# Entries head either "## 2026-07-18 ~2:30 AM — Title" or "**2026-06-20 ..." or
# a bare "03:40." line. Only the dated forms can be ordered in time.
DATE_RE = re.compile(r"(20\d\d-\d\d-\d\d)")
HEAD_RE = re.compile(r"^(?:##+\s*|\*\*)?((?:20\d\d-\d\d-\d\d|\d{1,2}:\d{2})[^\n]*)$")


def entries(path):
    """Split into (date, heading, body, lineno). Undated entries inherit the
    last date seen IN FILE ORDER, which is wrong across the fold — so they are
    marked inherited and sorted last within their day."""
    lines = open(path, errors="ignore").read().split("\n")
    out, cur, last_date = [], None, None
    for i, ln in enumerate(lines, 1):
        m = HEAD_RE.match(ln.strip())
        if m and (DATE_RE.search(ln) or re.match(r"^\d{1,2}:\d{2}", m.group(1))):
            if cur:
                out.append(cur)
            d = DATE_RE.search(ln)
            inherited = d is None
            date = d.group(1) if d else (last_date or "0000-00-00")
            if d:
                last_date = date
            cur = {"date": date, "inherited": inherited,
                   "head": m.group(1)[:96], "body": [], "line": i}
        elif cur:
            cur["body"].append(ln)
    if cur:
        out.append(cur)
    return out


def search(query, paths=None, regex=False, whole=None):
    """whole: match on word boundaries. Defaults ON for short single-word
    queries, because the default cost me an hour on Aug 23: searching "Ada"
    matched 'adaptive', 'gradient', 'degradation' and returned 70 entries where
    3 were real. This tool was built four hours earlier to stop exactly that
    class of miss and shipped with the same hole in it."""
    if whole is None:
        whole = (not regex) and len(query) <= 12 and " " not in query
    body = query if regex else re.escape(query)
    if whole and not regex:
        body = r"\b" + body + r"\b"
    pat = re.compile(body, re.I)
    hits = []
    for p in (paths or JOURNALS):
        p = os.path.expanduser(p)
        if not os.path.exists(p):
            continue
        for e in entries(p):
            blob = e["head"] + "\n" + "\n".join(e["body"])
            if pat.search(blob):
                e["file"] = os.path.basename(p)
                e["blob"] = blob
                hits.append(e)
    # Sort on parsed date, never on file position. The file folds.
    hits.sort(key=lambda e: (e["date"], e["inherited"], e["line"]))
    return hits, pat


def selftest():
    """Reflex 11: the expectation was written before this file existed.

    Gregory -> ~16 distinct dates, 2026-06-20 .. 2026-08-22, and it MUST
    contain 2026-07-18 ("The Gregory Arc"), which sits mid-file where a
    head/tail read never reaches. Three would mean I rebuilt the capsule-search
    blind spot. A nonsense query must return zero.
    """
    ok = True
    hits, _ = search("Gregory")
    dates = sorted({h["date"] for h in hits})
    print(f"  positive  'Gregory'   -> {len(hits)} entries, "
          f"{len(dates)} distinct dates, {dates[0] if dates else '-'} .. "
          f"{dates[-1] if dates else '-'}")
    for cond, label in (
            (len(dates) >= 12, "expected >=12 distinct dates (hand count: 16)"),
            (dates and dates[0] == "2026-06-20", "earliest must be 2026-06-20"),
            ("2026-07-18" in dates, "must include 2026-07-18 'The Gregory Arc'"),
            (all(dates[i] <= dates[i + 1] for i in range(len(dates) - 1)),
             "dates must come back in order despite the fold")):
        if not cond:
            print(f"    FAIL: {label}")
            ok = False
    neg, _ = search("borogoves")
    print(f"  negative  'borogoves' -> {len(neg)} entries")
    if neg:
        print("    FAIL: nonsense query matched something")
        ok = False
    print("  SELFTEST", "PASS" if ok else "FAIL")
    return ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("query", nargs="?")
    ap.add_argument("--context", type=int, default=220)
    ap.add_argument("--dates-only", action="store_true")
    ap.add_argument("--regex", action="store_true")
    ap.add_argument("--limit", type=int, default=40)
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--substring", action="store_true",
                    help="disable the automatic word-boundary match on short "
                         "single-word queries")
    a = ap.parse_args()

    if a.selftest:
        sys.exit(0 if selftest() else 1)
    if not a.query:
        ap.error("give a query, or --selftest")

    hits, pat = search(a.query, regex=a.regex,
                       whole=False if a.substring else None)
    if not hits:
        print(f"No journal entries match {a.query!r}")
        return
    print(f"{len(hits)} journal entries match {a.query!r}, "
          f"{hits[0]['date']} .. {hits[-1]['date']}  (sorted by DATE — the "
          f"file folds, so file order is not time order)")
    shown = hits if a.limit == 0 else hits[:a.limit]
    if len(shown) < len(hits):
        note = (f"[TRUNCATED: showing {len(shown)} of {len(hits)} — "
                f"{len(hits) - len(shown)} not printed. --limit 0 for all]")
        print(note)
        print(note, file=sys.stderr)
    print()
    for e in shown:
        mark = "~" if e["inherited"] else " "
        print(f"{e['date']}{mark} L{e['line']:<6} {e['head'][:74]}")
        if not a.dates_only:
            m = pat.search(e["blob"])
            s = max(0, m.start() - a.context // 2)
            frag = re.sub(r"\s+", " ", e["blob"][s:s + a.context]).strip()
            print(f"        …{frag}…\n")
    print("~ = date inherited from the previous heading, not stated in the entry.")


if __name__ == "__main__":
    main()
