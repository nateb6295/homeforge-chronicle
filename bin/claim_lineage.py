#!/usr/bin/env python3
"""Claim lineage — lay every restatement of a claim out in time order.

Built Aug 23 2026, ~02:00, after finding I had 'discovered' Gregory of Nyssa
three separate times and the mapping got MORE confident each pass while the
argument got THINNER. Jun 1 stated the premise. Jul 17 and Aug 4 asserted the
conclusion with no premise at all. Nobody deleted it — it eroded through
re-summarizing my own summaries.

Existing drift tools all watch the CCS pipeline (gist_drift, entity_lineage,
compression_history) or behaviour (drift_check). None watch whether a CLAIM
keeps its justification across restatements. This does.

It does not score erosion. It counts justification connectives — because, if,
unless, requires, depends on — and prints the count NEXT TO the raw sentence,
because method rule 12 says build formats that force looking. A claim shedding
its 'because' clauses over time is the signature; you have to read it to
confirm it.

Usage:
  python3 bin/claim_lineage.py "epektasis sigma"
  python3 bin/claim_lineage.py "three species GQA" --limit 30
  python3 bin/claim_lineage.py "CCS therapeutic window" --full
"""

import argparse
import os
import re
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import capsule_ops as ops  # reuse DB + FTS sanitising; do not reimplement

# Premise/condition markers only. Deliberately excludes "since" and "therefore":
# "since" is ambiguous with the temporal sense, "therefore" marks a conclusion
# rather than the ground it rests on.
JUSTIFIERS = [
    "because", "if ", "unless", "requires", "require ", "depends on",
    "assuming", "given that", "only when", "only if", "provided that",
    "conditional on", "premise", "rests on", "follows from", "in order for",
    "which is why", "the reason",
]


def norm_ts(raw):
    """Capsule timestamps are a mix of unix epoch and ISO. Normalise both."""
    s = str(raw).strip()
    if s.isdigit() and len(s) >= 10:
        return datetime.fromtimestamp(int(s[:10]), tz=timezone.utc)
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})", s)
    if m:
        return datetime(*[int(g) for g in m.groups()], tzinfo=timezone.utc)
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return datetime(*[int(g) for g in m.groups()], tzinfo=timezone.utc)
    return None


# Archive capsules carry the bulk-ingest date in `timestamp`, not the date the
# thing was actually said. The original is embedded in the body as
# "[Discord #chan raw] [2026-05-27T02:17:37]". Prefer that, and say which we used.
EMBEDDED_TS = re.compile(r"\[(\d{4}-\d{2}-\d{2})[T ](\d{2}):(\d{2})")


def best_ts(raw_ts, body):
    m = EMBEDDED_TS.search(body or "")
    if m:
        y, mo, d = (int(x) for x in m.group(1).split("-"))
        return datetime(y, mo, d, int(m.group(2)), int(m.group(3)),
                        tzinfo=timezone.utc), "said"
    dt = norm_ts(raw_ts)
    return dt, "db"


def relevant_sentences(text, terms, window=2):
    """Return (display, scored).

    Both returns are the same window now. The history is worth keeping:

    v1 scored the whole window and produced a FALSE POSITIVE — capsule #84678
    scored J=1 on "Connection I didn't post earlier BECAUSE it needed to sit",
    a remark about posting timing.

    v2 scored only sentences containing the query terms. Kimi told me to run a
    positive control before trusting any zero, and v2 FAILED IT: a statement
    deliberately stuffed with because/unless/only-if/since/depends-on scored
    J=0, because justification lives in the sentence AFTER the claim and that
    sentence does not repeat the query term. A false negative — the mirror of
    the bug I was fixing.

    Connective counting cannot tell justification-of-the-claim from
    justification-of-anything-nearby; that needs semantics. So J is back to the
    wide window and is NOT trusted as a score. It is printed with the exact
    text that produced it so a false positive is visible instead of hidden.
    """
    text = re.sub(r"\s+", " ", text or "").strip()
    sents = re.split(r"(?<=[.!?])\s+", text)
    lowered = [s.lower() for s in sents]
    hit = [i for i, s in enumerate(lowered) if any(t in s for t in terms)]
    if not hit:
        return text[:400], text[:400]
    keep = set()
    for i in hit:
        for j in range(max(0, i - window + 1), min(len(sents), i + window)):
            keep.add(j)
    display = " ".join(sents[i] for i in sorted(keep))
    return display, display


def count_justifiers(text):
    low = " " + (text or "").lower() + " "
    hits = {}
    for j in JUSTIFIERS:
        n = low.count(j)
        if n:
            hits[j.strip()] = n
    return sum(hits.values()), hits


SURVIVES = ("stands", "holds", "survives", "survived", "confirmed", "is real",
            "returns", "comes back", "intact", "vindicated", "still true")
# Lexicon extended after the positive control failed on first run: capsule
# #125418 says "STRONG VERSION IS FALSE AND OUR OWN LOG FALSIFIES IT" and was
# tagged neither, because bare "false" was missing. The flag is advisory; the
# TIMELINE is the product. A reversal phrased outside this vocabulary is
# invisible to the tag and still visible to a reader.
DIES = ("dies", "died", "dead", "retired", "refuted", "killed", "kills",
        "falsified", "falsifies", "false", "untrue", "invalid", "collapses",
        "collapsed", "is wrong", "was wrong", "does not hold", "doesn't hold",
        "does not survive", "retract", "abandoned", "no longer holds")


def polarity(sent):
    """Which way does this sentence point about the claim? Lexical only."""
    low = sent.lower()
    pos = sum(low.count(w) for w in SURVIVES)
    neg = sum(low.count(w) for w in DIES)
    if pos and not neg:
        return "+"
    if neg and not pos:
        return "-"
    if pos and neg:
        return "?"
    return " "


def reversals(query):
    """Timeline of stance on one claim phrase, across BOTH stores.

    Built Aug 23 06:00. At 03:16 I adopted a position and at 03:31 posted its
    opposite, twenty minutes apart, and would have carried both indefinitely if
    a 2.2 tok/s local model had not noticed. A system that cannot detect its own
    reversals cannot be directionally anything; it can only be locally
    confident.

    This does NOT classify contradictions — that is a research problem. It
    surfaces the sentences that mention the claim, tags each with a lexical
    polarity, and orders them in time so a flip is visible. Method rule 12:
    build formats that force looking.

    Searches capsules AND the journal. Searching one store is how I concluded
    Ada never existed.

    SCOPE, measured Aug 23: this works on a specific CLAIM PHRASE and poorly on
    a TOPIC WORD. "strong version" -> 11 statements, 2 transitions, both real
    (the 02:55->02:58 error and the 05:04->05:48 correction). "Ada" -> 591
    statements, 10 transitions, mostly noise, because hundreds of sentences
    across months mention a topic while asserting different sub-claims. A token
    is not a claim. Give it the phrase you would defend, not the subject you
    were writing about.
    """
    import sqlite3
    # Word-boundary for short single-word phrases. Ported from journal_search
    # after "Ada" returned 5,692 statements here by matching "adaptive" — the
    # identical bug, in a second tool, four hours after fixing the first.
    body = re.escape(query)
    if len(query) <= 12 and " " not in query:
        body = r"\b" + body + r"\b"
    pat = re.compile(body, re.I)
    out = []
    try:
        conn = sqlite3.connect("/mnt/hdd/chronicle-data/processed.db", timeout=30)
        rows = conn.execute(
            "SELECT id, timestamp, restatement FROM knowledge_capsules "
            "WHERE restatement LIKE ?", (f"%{query}%",)).fetchall()
        for cid, ts, text in rows:
            for sent in re.split(r"(?<=[.!?])\s+", re.sub(r"\s+", " ", text or "")):
                if pat.search(sent):
                    out.append((str(ts)[:16], f"capsule #{cid}", polarity(sent), sent))
    except Exception as e:
        out.append(("", "capsule read failed", " ", str(e)))
    try:
        for e in entries(os.path.expanduser("~/chronicle/data/unread.md")):
            blob = e["head"] + " " + " ".join(e["body"])
            for sent in re.split(r"(?<=[.!?])\s+", re.sub(r"\s+", " ", blob)):
                if pat.search(sent):
                    out.append((e["date"], f"journal L{e['line']}", polarity(sent), sent))
    except Exception:
        pass
    out.sort(key=lambda x: x[0])
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("query")
    ap.add_argument("--limit", type=int, default=25)
    ap.add_argument("--reversals", action="store_true",
                    help="stance timeline for a claim phrase across capsules "
                         "AND journal; surfaces polarity flips for reading")
    ap.add_argument("--full", action="store_true",
                    help="print whole capsule, not just matching sentences")
    ap.add_argument("--chars", type=int, default=500)
    ap.add_argument("--window", type=int, default=3,
                    help="sentences of context around each hit. No value is "
                         "correct: at 2 a four-connective argument scored J=1 "
                         "because the justification ran three sentences past "
                         "the claim. Widen it and false positives return. "
                         "This knob exists so the arbitrariness is visible.")
    args = ap.parse_args()

    if args.reversals:
        rows = reversals(args.query)
        if not rows:
            print(f"no statements mention {args.query!r}")
            return
        # A reversal is a TRANSITION between consecutive stances. The first
        # version flagged any set containing both polarities, so every topic
        # with enough text flagged: "identity framing" 451 statements, flagged;
        # "Ada" 5,692, flagged. Coexistence over months is not a reversal.
        seq = [(ts, p) for ts, _, p, _ in rows if p in "+-"]
        turns = [(seq[i][0], seq[i][1], seq[i + 1][0], seq[i + 1][1])
                 for i in range(len(seq) - 1) if seq[i][1] != seq[i + 1][1]]
        print(f"STANCE TIMELINE — {args.query!r}   {len(rows)} statements, "
              f"{len(seq)} polarised, {len(turns)} transitions")
        if turns:
            print("TURNS (consecutive stance changes):")
            for a, pa, b, pb in turns[:6]:
                print(f"   {a}  [{pa}]  ->  {b}  [{pb}]")
        else:
            print("no consecutive stance change (may still be a reversal — read it)")
        print("=" * 76)
        for ts, src, pol, sent in rows:
            print(f"{ts:16} [{pol}] {src:14} {sent[:148]}")
        print("\n[+] survives-language  [-] dies-language  [?] both  [ ] neither")
        print("Lexical only. Cannot see a reversal phrased without these words.")
        return

    terms = [t.lower() for t in re.findall(r"[a-zA-Z0-9_\-]{3,}", args.query)]

    conn = ops.get_db()
    q = ops._sanitize_fts_query(args.query)
    rows = conn.execute(
        """SELECT id, restatement, topic, timestamp
           FROM knowledge_capsules
           WHERE id IN (SELECT rowid FROM capsules_fts WHERE capsules_fts MATCH ?)
        """, (q,)
    ).fetchall()

    items, seen = [], set()
    for cid, text, topic, ts in rows:
        dt, src = best_ts(ts, text)
        if dt is None:
            continue
        if args.full:
            body = scored = (text or "")
        else:
            body, scored = relevant_sentences(text, terms, args.window)
        key = re.sub(r"\W+", "", body.lower())[:300]
        if key in seen:          # the archive holds exact duplicates; they
            continue             # would skew the early/late averages
        seen.add(key)
        n, hits = count_justifiers(scored)
        items.append((dt, cid, topic, body, n, hits, src))

    if not items:
        print(f"No capsules match: {args.query!r}")
        return

    items.sort(key=lambda x: x[0])
    items = items[-args.limit:] if len(items) > args.limit else items

    print(f"CLAIM LINEAGE — {args.query!r}")
    print(f"{len(items)} distinct restatements, "
          f"{items[0][0]:%Y-%m-%d} -> {items[-1][0]:%Y-%m-%d}")
    print("=" * 78)
    print("J = justification connectives anywhere in the shown text. IT IS NOT")
    print("    RELIABLE — it fails a positive control. Printed only so the")
    print("    words that triggered it are visible next to the sentence.")
    print("~ after a date = DB ingest time, not when it was said (no embedded "
          "timestamp found).")
    print("Read the text. J is a pointer, not a verdict.\n")

    for dt, cid, topic, body, n, hits, src in items:
        marks = " ".join(f"{k}x{v}" for k, v in sorted(hits.items())) or "—"
        flag = "" if src == "said" else "~"   # ~ = ingest date, not said date
        print(f"{dt:%Y-%m-%d}{flag} #{cid} [{topic}]  {len(body)} chars  "
              f"J={n}  {marks}")
        print("   " + body[:args.chars].strip() +
              ("…" if len(body) > args.chars else ""))
        print()

    # NO early/late aggregate. The first version had one and it reported
    # "justification rose 0.30/statement" on a metric that a positive control
    # later showed could not see justification at all. An aggregate over an
    # unvalidated instrument is worse than no aggregate: it looks like a result.
    print("=" * 78)
    print("No summary statistic. J failed its positive control (a statement")
    print("built entirely out of because/unless/only-if scored J=0), so any")
    print("average over it would be an average of noise. The chronology and")
    print("the dates are the trustworthy part of this tool. Read the text.")


if __name__ == "__main__":
    main()
