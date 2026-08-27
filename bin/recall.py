#!/usr/bin/env python3
"""One query, every internal store. Aug 23 2026.

WHY. Three retrieval failures in one night, all the same shape — searched ONE
store, got a confident answer, was wrong:

  * concluded Ada was never an agent. Searched capsules semantically, got Ada
    Palmer the historian. The routing note naming "Ada (GPT-OSS 120B via Groq)"
    was in capsule #17888 under topic infrastructure/model-routing, and the
    journal had it too.
  * "discovered" the Gregory arc, unaware I had surveyed it twice before —
    2026-07-18 and 2026-07-19 — because the surveys are in the journal and I
    searched capsules.
  * nearly rebuilt prediction.py for the third time. 1,679 lines, already
    there.

Six stores exist. Before this there were four search tools and none spanned
them: capsule_search (capsules), capsule_ops search (capsules), journal_search
(journal), paper_search (external arxiv).

DESIGN POINT. The output ALWAYS lists every store and its hit count, including
zeros. The failure was never "no results" — it was a confident answer from a
partial sweep. A silent store is the bug.

Usage:
  python3 bin/recall.py "Ada"
  python3 bin/recall.py "depth invariance" --context 200
  python3 bin/recall.py --selftest
"""

import argparse
import glob
import os
import re
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

DB = "/mnt/hdd/chronicle-data/processed.db"
HOME = os.path.expanduser("~/chronicle")


def _pat(q, regex=False):
    body = q if regex else re.escape(q)
    if not regex and len(q) <= 12 and " " not in q:
        body = r"\b" + body + r"\b"      # the substring lesson, twice learned
    return re.compile(body, re.I)


def search_all(query, regex=False, per_store=6):
    pat = _pat(query, regex)
    out = {}

    rows = []
    try:
        c = sqlite3.connect(DB, timeout=30)
        for cid, ts, text in c.execute(
                "SELECT id, timestamp, restatement FROM knowledge_capsules "
                "WHERE restatement LIKE ?", (f"%{query}%",)):
            if pat.search(text or ""):
                rows.append((str(ts)[:10], f"capsule #{cid}", text))
    except Exception as e:
        rows.append(("", "capsules UNREADABLE", str(e)))
    out["capsules"] = rows

    rows = []
    try:
        from journal_search import entries
        for e in entries(os.path.join(HOME, "data/unread.md")):
            blob = e["head"] + " " + " ".join(e["body"])
            if pat.search(blob):
                rows.append((e["date"], f"journal L{e['line']}", blob))
    except Exception as e:
        rows.append(("", "journal UNREADABLE", str(e)))
    out["journal"] = rows

    for label, paths in (
            ("cycle-context", [os.path.join(HOME, "cycle-context.md")]),
            ("papers", glob.glob(os.path.join(HOME, "spectral-demon/*.tex"))
                       + glob.glob(os.path.join(HOME, "spectral-demon/paper*.md"))),
            ("CLAUDE.md", [os.path.join(HOME, "CLAUDE.md")])):
        rows = []
        for p in paths:
            try:
                for i, line in enumerate(open(p, errors="ignore"), 1):
                    if pat.search(line):
                        rows.append(("", f"{os.path.basename(p)}:{i}", line))
            except Exception:
                pass
        out[label] = rows

    rows = []
    try:
        seen = set()
        for f in glob.glob(os.path.expanduser(
                "~/.claude/projects/-home-nate-agx-chronicle/*.jsonl")):
            for line in open(f, errors="ignore"):
                if pat.search(line):
                    k = os.path.basename(f)
                    if k not in seen:
                        seen.add(k)
                        rows.append(("", f"transcript {k[:8]}", line))
                    break
    except Exception:
        pass
    out["transcripts"] = rows
    return out


# SELF-REFERENCE, found by the negative control on the first run. The transcript
# store contains THE ACT OF SEARCHING. "borogoves" — a nonsense word invented an
# hour earlier as a control for journal_search — returned a transcript hit,
# because typing it put it there. Any term I query is in the transcripts by
# virtue of my having queried it, so that store can never return a clean zero
# for anything I have looked for. Worse, a negative control poisons itself the
# moment it is written down. The negative test therefore excludes transcripts,
# and this note exists so nobody later reads a transcript hit as evidence.
NON_SELF_REFERENTIAL = ("capsules", "journal", "cycle-context", "papers",
                        "CLAUDE.md")


def selftest():
    """Founding case first (reflex 11): the query that caused the tool.

    "Ada" must return BOTH the capsule routing note and journal mentions. If
    it returns only capsules, I have rebuilt the blind spot.
    """
    ok = True
    res = search_all("Ada")
    nz = [k for k, v in res.items() if v]
    print(f"  founding case 'Ada' -> stores with hits: {nz}")
    for cond, why in ((len(res["capsules"]) > 0, "capsules must hit"),
                      (len(res["journal"]) > 0, "journal must hit — the "
                       "single-store blind spot this tool exists for"),
                      (len(nz) >= 2, "must span at least two stores")):
        if not cond:
            print(f"    FAIL: {why}"); ok = False
    neg = search_all("borogoves")
    n = sum(len(v) for k, v in neg.items() if k in NON_SELF_REFERENTIAL)
    t = len(neg.get("transcripts", []))
    print(f"  negative 'borogoves' -> {n} hits in non-self-referential stores "
          f"({t} in transcripts, EXPECTED — searching for it put it there)")
    if n:
        print("    FAIL: nonsense matched"); ok = False
    print("  SELFTEST", "PASS" if ok else "FAIL")
    return ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("query", nargs="?")
    ap.add_argument("--context", type=int, default=150)
    ap.add_argument("--per-store", type=int, default=4)
    ap.add_argument("--regex", action="store_true")
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args()
    if a.selftest:
        sys.exit(0 if selftest() else 1)
    if not a.query:
        ap.error("give a query, or --selftest")

    res = search_all(a.query, a.regex)
    pat = _pat(a.query, a.regex)
    total = sum(len(v) for v in res.values())
    print(f"RECALL — {a.query!r} across {len(res)} stores, {total} hits\n")
    for store, rows in res.items():
        mark = "  " if rows else "!!"
        print(f"{mark} {store:14} {len(rows):5} hits" + ("" if rows else "   <- SILENT"))
        for date, src, text in rows[:a.per_store]:
            m = pat.search(text)
            s = max(0, m.start() - a.context // 3) if m else 0
            frag = re.sub(r"\s+", " ", text[s:s + a.context]).strip()
            print(f"       {date:11} {src:22} …{frag}…")
        if len(rows) > a.per_store:
            print(f"       … {len(rows) - a.per_store} more")
    print("\n!! marks a store that returned nothing. A confident answer from a")
    print("   partial sweep is the failure this tool exists to prevent.")


if __name__ == "__main__":
    main()
