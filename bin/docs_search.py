#!/usr/bin/env python3
"""Search the markdown that carries findings — data/*.md AND the root files.

ROOT FILES ADDED 2026-08-27, and the hole was worse than an omission. CLAUDE.md is
82KB of VERDICTS, it LOADS every session, and it was in NO SEARCH CORPUS AT ALL --
this tool globbed data/*.md, and search_all.py's grep arm covers bin/ only. So every
`prior work searched:` line I have ever written excluded the file that holds my
conclusions.

Decisive test that found it: "compounding-nothings basin" appears exactly once in the
whole repo, in CLAUDE.md. This tool answered
    "no match in 157 files under data/ -- literal match, so this is a real absence
     of these WORDS, not of the topic"
A confidently-worded FALSE ABSENCE about my own governing document.

It is not hypothetical: on 2026-08-27 I re-derived the focal_entities/brain-v1
transition that was ALREADY WRITTEN IN CLAUDE.md, with the same snapshot id. Now I
know the mechanism -- search could not reach it.

Found while compressing CLAUDE.md, which is the other half of the same lesson: a
finding whose only home is the loaded file cannot be searched, cannot be compressed
without orphaning it, and is one edit from gone. See data/allayer_sigma2_retraction.md,
written that day because three of its numbers lived nowhere else.

BUILT 2026-08-26. Three tools search things here: capsule_ops (78k capsules),
discord_search (97k messages), codebase_index (what scripts are FOR). NOTHING searched
data/*.md, which is where every prereg lives — prediction, kill condition, outcome.

That became load-bearing the same day: I moved 264 lines of research reasoning out of
CLAUDE.md into data/research_history.md on the argument that it would be "searched
instead of loaded", and made `prior work searched:` a REQUIRED prereg field. Both assume
a searcher over data/ that did not exist. This is it.

Deliberately dumb — literal + word-boundary matching, ranked by hits. No embeddings.
The failure mode of the semantic path is a false ABSENCE (capsule_ops says so itself),
and for "have I already tested this?" a false absence is the expensive direction.

  python3 bin/docs_search.py "kill condition"        # all preregs with one
  python3 bin/docs_search.py witness negation        # any term, ranked
  python3 bin/docs_search.py --outcomes              # preregs with NO recorded outcome
"""
import argparse, glob, os, re, sys

ROOT = os.path.expanduser("~/chronicle")
DOCS = os.path.join(ROOT, "data")
# Root-level markdown that carries findings or standing decisions. Named
# explicitly rather than globbed: the root also holds README-ish files whose
# words would dilute a "have I already tested this?" search.
ROOT_DOCS = ["CLAUDE.md", "cycle-context.md", "values.md"]


def _files():
    fs = sorted(glob.glob(os.path.join(DOCS, "*.md")))
    fs += [os.path.join(ROOT, f) for f in ROOT_DOCS
           if os.path.exists(os.path.join(ROOT, f))]
    return fs


def _scope():
    """The literal scope, for the no-match line. An English gloss can claim more
    than the search had -- that is how three claims went wrong in one evening."""
    n_data = len(glob.glob(os.path.join(DOCS, "*.md")))
    roots = [f for f in ROOT_DOCS if os.path.exists(os.path.join(ROOT, f))]
    return f"{n_data} files under data/ plus {', '.join(roots)}"


def search(terms, limit=12, context=1):
    pats = [re.compile(r'\b' + re.escape(t) + r'\b', re.I) if ' ' not in t
            else re.compile(re.escape(t), re.I) for t in terms]
    hits = []
    for f in _files():
        try:
            lines = open(f, encoding="utf-8", errors="ignore").read().splitlines()
        except OSError:
            continue
        marks = [i for i, ln in enumerate(lines) if all(p.search(ln) for p in pats)] \
             or [i for i, ln in enumerate(lines) if any(p.search(ln) for p in pats)]
        if marks:
            hits.append((len(marks), f, lines, marks))
    hits.sort(reverse=True, key=lambda x: x[0])
    return hits[:limit], context


# THREE states, not two. 2026-08-26: a strict OUTCOME-heading rule called 20 preregs
# "open"; reading them showed most CONTAIN their results and simply do not use that
# heading. A loose rule had earlier called 29 "closed" by matching the word "outcome"
# inside PREDICTION sections ("## Pre-registered outcomes"). Neither binary is honest,
# so report the middle state instead of picking a side.
# THE CLOSURE HEADING, HOISTED TO MODULE LEVEL 2026-08-27 so there is exactly ONE
# definition of "this prereg has a verdict". health_alert.py had its own, narrower
# copy — a literal `"# OUTCOME" in txt` — and therefore could not see the three
# preregs closed under `## RESULT`. Its own comment, four lines above the bug,
# documents this exact class ("two criteria disagreeing ... is how a monitor
# silently under-reports"); it widened the PREDICTION side and left this side narrow.
# Same lesson as NON_OPUS_LOCATIONS: a criterion copied into two files is a criterion
# that will drift. Import it, do not restate it.
CLOSURE_HEADING = re.compile(
    r'^#+ *(OUTCOME|RESULT|VERDICT|FINDING|WHAT HAPPENED)\b'
    r'|^\s*(OUTCOME|RESULT) —', re.M | re.I)

RESULT_WORDS = re.compile(
    r'\b(fails?|failed|confirmed|correct|wrong|holds?|held|result|measured|observed'
    r'|VOID|survived|refuted|reproduc\w+)\b', re.I)


def outcomes():
    """(closed, likely, open) — heading / result-language-only / neither."""
    closed, likely, open_ = [], [], []
    for f in _files():
        b = os.path.basename(f)
        t = open(f, encoding="utf-8", errors="ignore").read()
        if "prereg" not in b.lower() or not re.search(r'\bPREDICT', t, re.I):
            continue
        # A VERDICT HEADING IS A VERDICT WHATEVER IT IS CALLED. 2026-08-26:
        # the OUTCOME-only rule flagged 8 files as "results present, no
        # heading" — but 3 of them (beta_sweep, organ_v3, position_masked_svd)
        # carry a fully written `## RESULT — ... CONTROL FAILED. CLAIM
        # WITHHELD.` They were closed in substance and miscounted as work
        # outstanding, which is the same defect as a monitor aimed at the
        # wrong gap: the instrument reported a shortfall that was its own
        # naming convention. Accept RESULT / VERDICT / FINDING / WHAT HAPPENED
        # as closure headings too.
        # The dash form must be at LINE START, not \b anywhere. First version
        # matched audit_decay's mid-sentence prose "result — not a direction.
        # A 15-point difference on n~12 is noise" and counted it as a verdict.
        if CLOSURE_HEADING.search(t):
            closed.append(b); continue
        tail = t.split("Predictions")[-1] if "Predictions" in t else t
        (likely if len(RESULT_WORDS.findall(tail)) >= 6 else open_).append(b)
    return closed, likely, open_


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("terms", nargs="*")
    ap.add_argument("--limit", type=int, default=12)
    ap.add_argument("--context", type=int, default=1)
    ap.add_argument("--outcomes", action="store_true")
    a = ap.parse_args()

    if a.outcomes:
        closed, likely, op = outcomes()
        print(f"{len(closed)} CLOSED (OUTCOME heading)")
        print(f"{len(likely)} LIKELY CLOSED — results present, no heading. Normalise these:")
        for f in likely:
            print(f"    {f}")
        print(f"{len(op)} GENUINELY OPEN — a prediction with no result anywhere in the file:")
        for f in op:
            print(f"    {f}")
        return 0
    if not a.terms:
        ap.error("give search terms, or --outcomes")

    hits, ctx = search(a.terms, a.limit, a.context)
    if not hits:
        print(f"no match in {_scope()} — "
              f"literal match, so this is a real absence of these WORDS, not of the topic")
        return 1
    for n, f, lines, marks in hits:
        print(f"\n=== {os.path.basename(f)}  ({n} hit{'s' if n > 1 else ''}) ===")
        for i in marks[:3]:
            lo, hi = max(0, i - ctx), min(len(lines), i + ctx + 1)
            for j in range(lo, hi):
                print(f"  {'>' if j == i else ' '} {lines[j][:110]}")
            print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
