#!/usr/bin/env python3
"""Measure how each CCS section moves between compressions.

Built 2026-08-25, after writing this analysis three times ad hoc in one day and
after it stopped me from shipping a bad experiment.

WHAT IT IS FOR. I was about to wire the compost digest into compression and
claim credit when CORE started moving. Taking the baseline first showed CORE
already moves 33% median — my premise was two months stale, and the prediction
went void instead of falsely confirmed. Take the before.

WHAT IT FOUND. Section behaviour tracks the prompt's OPERATIONAL INSTRUCTION,
to the minute, and not its label. All three prompt versions call CORE "the
sigma-1 invariant"; the label never changed and the behaviour changed twice:

  v1_stative   "what doesn't change between rotations"     CORE frozen, 0.0%
  v2_imperative "Use verbs: Hold X, Return to Y"           88.1% jump, opening
                                                           becomes "Hold the
                                                           shape..." — the
                                                           template, echoed back
  v3           "what I'm oriented toward RIGHT NOW"        95.7% jump at
               + SPINE added to carry what persists        Jul 5 17:19, SPINE
                                                           appears same
                                                           compression

READ THIS BEFORE CITING SPINE. SPINE sits at 0.0% median, 98% of pairs under 2%
change. That is INSTRUCTED, not discovered — v4 says "the spine IS stative. It's
what doesn't move" and "2-3 sentences that rarely change". Measuring SPINE
stable measures COMPLIANCE. It is not evidence for F12 direction>coupling, and
that mapping is decoration however well it fits.

Usage:
  ccs_section_dynamics.py                     baseline, all sections, all time
  ccs_section_dynamics.py --daily CORE        per-day medians for one section
  ccs_section_dynamics.py --around 2026-07-05 window around a prompt change
  ccs_section_dynamics.py --since 2026-08-01  restrict the window
"""
import argparse, datetime, difflib, json, re, sqlite3, statistics as st, sys

DB = "/mnt/hdd/chronicle-data/processed.db"
HEAD = re.compile(r"^##\s+([A-Z][A-Z ]*)\s*$", re.M)


def load(since=None, until=None):
    c = sqlite3.connect(DB, timeout=60.0)
    c.row_factory = sqlite3.Row
    q = ("SELECT snapshot, created_at FROM cognitive_state_history "
         "WHERE trigger='brain-compression'")
    p = []
    if since: q += " AND created_at >= ?"; p.append(since)
    if until: q += " AND created_at <= ?"; p.append(until)
    out = []
    for r in c.execute(q + " ORDER BY created_at", p):
        try:
            g = json.loads(r["snapshot"]).get("semantic_gist") or ""
        except Exception:
            continue
        secs = {}
        for m in HEAD.finditer(g):
            s = m.end()
            nx = HEAD.search(g, s)
            secs[m.group(1).strip()] = g[s:nx.start() if nx else len(g)].strip()
        if secs:
            out.append((r["created_at"], secs))
    return out


def changes(rows, name, word=False):
    """Change between consecutive compressions.

    METRIC CAVEAT, measured 2026-08-25 — read before quoting a single number.

    Character-level SequenceMatcher OVERSTATES small edits in short sections.
    A real case: CORE at Jul 3 19:56 scored 62.4% change, and the entire diff
    was three word swaps — "shift between rotations:"->"shift:", "to have
    gotten it right,"->"to,", "I learned"->"I've learned". I nearly logged that
    as an unexplained pre-prompt-change event.

    Word-level runs 6-9 points lower across every section, and changes NOTHING
    qualitatively: SPINE 0.0% both ways, CORE 33.3% char / 26.8% word, the
    high-movement sections stay high, the ranking is identical.

    So: medians over hundreds of pairs are sound either way. A SINGLE
    comparison on a 400-600 char section is not — use --word and look at the
    actual diff before calling one compression an event.
    """
    seq = [(t, s[name]) for t, s in rows if s.get(name)]
    if word:
        return [(seq[i][0], 1 - difflib.SequenceMatcher(
                    None, seq[i-1][1].split(), seq[i][1].split()).ratio())
                for i in range(1, len(seq))]
    return [(seq[i][0], 1 - difflib.SequenceMatcher(None, seq[i-1][1], seq[i][1]).ratio())
            for i in range(1, len(seq))]


def ts(d):
    return int(datetime.datetime.strptime(d, "%Y-%m-%d")
               .replace(tzinfo=datetime.timezone.utc).timestamp())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--daily", metavar="SECTION")
    ap.add_argument("--saturation", action="store_true",
                    help="is a section already at its random-pair ceiling? "
                         "(a saturated section CANNOT serve as a control)")
    ap.add_argument("--carry", metavar="SECTION",
                    help="weekly carry-forward rate + magnitude-when-moved — "
                         "the correct statistic for a bimodal section")
    ap.add_argument("--around", metavar="YYYY-MM-DD")
    ap.add_argument("--since"); ap.add_argument("--until")
    ap.add_argument("--days", type=int, default=3)
    a = ap.parse_args()

    since = ts(a.since) if a.since else None
    until = ts(a.until) if a.until else None
    if a.around:
        mid = ts(a.around)
        since, until = mid - a.days * 86400, mid + a.days * 86400

    rows = load(since, until)
    if len(rows) < 3:
        # Never print an empty table as though it were a flat result.
        print(f"Only {len(rows)} snapshots in range — NOT a finding of no change. "
              f"Widen the window.", file=sys.stderr)
        return 1

    names = sorted({k for _, s in rows for k in s},
                   key=lambda n: -sum(1 for _, s in rows if n in s))
    names = [n for n in names if sum(1 for _, s in rows if n in s) >= 3]

    if a.around:
        print(f"=== {len(rows)} compressions, {a.days}d either side of {a.around} ===\n")
        print(f"  {'when':<14}" + "".join(f"{n[:8]:>10}" for n in names[:6]))
        prev = None
        for t, s in rows:
            if prev is None:
                prev = s; continue
            cells = ""
            for n in names[:6]:
                if s.get(n) and prev.get(n):
                    d = (1 - difflib.SequenceMatcher(None, prev[n], s[n]).ratio()) * 100
                    cells += f"{d:>9.1f}%"
                else:
                    cells += f"{'NEW' if s.get(n) else '-':>10}"
            when = datetime.datetime.utcfromtimestamp(t).strftime("%m-%d %H:%M")
            print(f"  {when:<14}{cells}")
            prev = s
        return 0

    if a.saturation:
        # Added 2026-08-25. I built an argument on "these control sections
        # stayed flat at the boundary" and Kimi asked whether flat meant a
        # control passing or a ceiling. It was a ceiling. A section whose
        # adjacent-pair change already equals its RANDOM-pair change is
        # resampled from scratch every pass and can never show an effect.
        import random
        random.seed(11)
        print("=== saturation — adjacent change vs random-pair ceiling ===\n")
        print(f"  {'section':<11}{'adjacent':>10}{'random':>9}{'ratio':>8}   verdict")
        for n in names:
            seq = [s[n] for _, s in rows if s.get(n)]
            if len(seq) < 40:
                continue
            adj = st.median([1 - difflib.SequenceMatcher(
                None, seq[i-1].split(), seq[i].split()).ratio()
                for i in range(1, len(seq))])
            pr = [(random.randrange(len(seq)), random.randrange(len(seq)))
                  for _ in range(300)]
            rnd = st.median([1 - difflib.SequenceMatcher(
                None, seq[x].split(), seq[y].split()).ratio()
                for x, y in pr if abs(x - y) > 20])
            ratio = adj / rnd if rnd else 0
            v = ("SATURATED — not usable as a control" if ratio > 0.95 else
                 "near ceiling — weak control" if ratio > 0.85 else
                 "has headroom")
            print(f"  {n:<11}{adj*100:>9.1f}%{rnd*100:>8.1f}%{ratio:>8.2f}   {v}")
        return 0

    if a.carry:
        # A bimodal section (copied verbatim some passes, rewritten others) has
        # a meaningless median. Report the MIXTURE instead.
        n = a.carry
        seq = [(t, s[n]) for t, s in rows if s.get(n)]
        byw = {}
        for i in range(1, len(seq)):
            d = 1 - difflib.SequenceMatcher(
                None, seq[i-1][1].split(), seq[i][1].split()).ratio()
            byw.setdefault(datetime.datetime.utcfromtimestamp(seq[i][0])
                           .strftime("%Y-W%W"), []).append(d)
        print(f"=== {n}: carry-forward rate and magnitude-when-moved ===\n")
        print(f"  {'week':<10}{'carry-fwd':>11}{'when moved':>13}   n")
        for wk in sorted(byw):
            v = byw[wk]
            if len(v) < 5:
                continue
            mv = [x for x in v if x >= 0.02]
            print(f"  {wk:<10}{sum(1 for x in v if x < 0.02)/len(v)*100:>10.0f}%"
                  f"{(st.median(mv)*100 if mv else 0):>12.1f}%   {len(v)}")
        return 0

    if a.daily:
        ch = changes(rows, a.daily)
        byday = {}
        for t, d in ch:
            byday.setdefault(datetime.datetime.utcfromtimestamp(t)
                             .strftime("%Y-%m-%d"), []).append(d)
        print(f"=== {a.daily}: median change per day ===\n")
        for day in sorted(byday):
            v = byday[day]
            print(f"  {day}  {st.median(v)*100:>5.1f}%  n={len(v):<3} "
                  f"{'#' * int(st.median(v)*100/4)}")
        return 0

    print(f"=== CCS section dynamics — {len(rows)} brain-compressions ===")
    print(f"    {datetime.datetime.utcfromtimestamp(rows[0][0]).date()} to "
          f"{datetime.datetime.utcfromtimestamp(rows[-1][0]).date()}\n")
    print(f"  {'section':<12}{'pairs':>7}{'char':>8}{'word':>8}{'at floor':>10}")
    for n in names:
        ch = [d for _, d in changes(rows, n)]
        wd = [d for _, d in changes(rows, n, word=True)]
        if len(ch) < 3:
            continue
        floor = sum(1 for x in ch if x < 0.02) / len(ch) * 100
        note = "  <- INSTRUCTED to hold" if n == "SPINE" else ""
        print(f"  {n:<12}{len(ch):>7}{st.median(ch)*100:>7.1f}%"
              f"{st.median(wd)*100:>7.1f}%{floor:>9.0f}%{note}")
    print("\n  char overstates small edits in short sections by 6-9 points; the")
    print("  ranking is identical either way. Never call ONE compression an event")
    print("  on the char number alone — look at the diff.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
