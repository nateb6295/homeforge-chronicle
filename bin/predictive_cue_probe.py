#!/usr/bin/env python3
"""Probe whether the CCS predictive_cue actually predicts.

Tests the claim implicit in the ACC architecture: predictive_cue at rotation N
should correspond to what shows up in episodic_trace at rotation N+1.

Method: extract distinctive content tokens (≥5 chars, strip stopwords) from
each cue. Check overlap with the next rotation's episodic_trace. Report the
jaccard distribution over all 49 transition pairs.

Null: if predictive_cue is a calibrated forward signal, we expect meaningful
overlap. If it's aspirational task-planning that gets overwritten by emergent
events, overlap will be near chance.
"""
import json
import re
import sqlite3
import unicodedata
from collections import Counter

DB = "/mnt/hdd/chronicle-data/processed.db"

STOP = set(
    "the and for with from into that this have will also but not are was were "
    "should would could can may might must shall about over after before into "
    "their there these those then than when where which while what who whom "
    "continue start begin finish complete monitor check update run next more "
    "plus also etc thread build cycle rotation ".split()
)


def tokens(s: str) -> set:
    if not s:
        return set()
    s = unicodedata.normalize("NFKC", s).lower()
    s = re.sub(r"[^a-z0-9_\- ]", " ", s)
    toks = {t for t in s.split() if len(t) >= 5 and t not in STOP}
    return toks


def jaccard(a: set, b: set) -> float:
    u = a | b
    if not u:
        return 0.0
    return len(a & b) / len(u)


def main():
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT id, snapshot FROM cognitive_state_history ORDER BY created_at ASC"
    ).fetchall()
    snaps = [(rid, json.loads(snap_json)) for rid, snap_json in rows]

    pairs = []
    for i in range(len(snaps) - 1):
        cue = snaps[i][1].get("predictive_cue") or ""
        epi = snaps[i + 1][1].get("episodic_trace") or []
        if isinstance(epi, list):
            epi_text = " ".join(str(x) for x in epi)
        else:
            epi_text = str(epi)
        cue_toks = tokens(cue)
        epi_toks = tokens(epi_text)
        j = jaccard(cue_toks, epi_toks)
        pairs.append((snaps[i][0], snaps[i + 1][0], j, len(cue_toks), len(epi_toks)))

    # Shuffle cue→epi pairs to get null distribution (random pairing)
    import random
    random.seed(0)
    epi_pool = [(p[1], p[4]) for p in pairs]
    cue_pool = [(p[0], p[3]) for p in pairs]
    # Re-extract for null
    cue_toks_list = []
    epi_toks_list = []
    for i in range(len(snaps) - 1):
        cue = snaps[i][1].get("predictive_cue") or ""
        epi = snaps[i + 1][1].get("episodic_trace") or []
        if isinstance(epi, list):
            epi_text = " ".join(str(x) for x in epi)
        else:
            epi_text = str(epi)
        cue_toks_list.append(tokens(cue))
        epi_toks_list.append(tokens(epi_text))
    null_vals = []
    shuffled = epi_toks_list[:]
    random.shuffle(shuffled)
    for c, e in zip(cue_toks_list, shuffled):
        null_vals.append(jaccard(c, e))

    signal_vals = [p[2] for p in pairs]

    def summary(vals, label):
        n = len(vals)
        m = sum(vals) / n
        vs = sorted(vals)
        median = vs[n // 2]
        print(f"  {label}: n={n} mean={m:.3f} median={median:.3f} min={min(vals):.3f} max={max(vals):.3f}")

    print("PREDICTIVE_CUE → NEXT EPISODIC_TRACE")
    print("(token-level jaccard, ≥5-char content words)\n")
    summary(signal_vals, "signal (N→N+1)")
    summary(null_vals, "null   (shuffled)")

    # Lift
    lift = (sum(signal_vals) / len(signal_vals)) / max(sum(null_vals) / len(null_vals), 1e-9)
    print(f"\n  lift over null: {lift:.2f}x")

    # Highest and lowest matches
    pairs_sorted = sorted(pairs, key=lambda p: -p[2])
    print(f"\nTop 5 best-predicting cues:")
    for pre, post, j, nc, ne in pairs_sorted[:5]:
        print(f"  rot {pre}→{post}: jaccard={j:.3f}  cue_toks={nc}  epi_toks={ne}")
    print(f"\nTop 5 worst-predicting cues:")
    for pre, post, j, nc, ne in pairs_sorted[-5:]:
        print(f"  rot {pre}→{post}: jaccard={j:.3f}  cue_toks={nc}  epi_toks={ne}")

    db.close()


if __name__ == "__main__":
    main()
