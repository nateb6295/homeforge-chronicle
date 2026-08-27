#!/usr/bin/env python3
"""
identity_decay.py — discrete d(Identity)/d(Rotation) probe.

Reads cognitive_state_history snapshots and computes how much the CCS
changes between consecutive compressions.

Two complementary metrics:
  - Jaccard (vocabulary overlap): do the same WORDS persist? (content)
  - Richness (unique/total ratio): does structural CAPACITY persist? (conditions)

F356 showed identity is behavioral capacity (override depth), not stored
content. Jaccard measures content drift; richness measures whether the
conditions for elaboration survive compression.
"""
import json
import re
import sqlite3
import sys
import unicodedata
from pathlib import Path

DB = Path("/mnt/hdd/chronicle-data/processed.db")
TOKEN_RE = re.compile(r"[a-z0-9]+")
# compressor LLM emits semantically-identical strings with drifting punctuation
# (en-dash vs hyphen, non-breaking hyphen, etc). Normalize before set comparison.
DASH_CHARS = "\u2010\u2011\u2012\u2013\u2014\u2015\u2212"


def normalize(s: str) -> str:
    s = unicodedata.normalize("NFKC", s).lower().strip()
    for d in DASH_CHARS:
        s = s.replace(d, "-")
    return re.sub(r"\s+", " ", s)


def tokens(text: str) -> set[str]:
    if not text:
        return set()
    return set(TOKEN_RE.findall(text.lower()))


def jaccard(a: set, b: set) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def entity_names(ccs: dict) -> set[str]:
    out = set()
    for e in ccs.get("focal_entities") or []:
        name = (e.get("name") if isinstance(e, dict) else str(e)) or ""
        if name:
            out.add(name.lower())
    return out


def constraint_set(ccs: dict) -> set[str]:
    out = set()
    for c in ccs.get("constraints") or []:
        text = (c.get("rule") if isinstance(c, dict) else str(c)) or ""
        if text:
            out.add(normalize(text))
    return out


def gist_depth(gist_text: str) -> dict:
    """Structural depth metrics for a gist string.

    MATTR (moving-average TTR) corrects for text-length bias in raw TTR.
    """
    words = gist_text.split()
    word_set = set(w.lower() for w in words)
    sents = [s.strip() for s in gist_text.replace("\n", ". ").split(".")
             if s.strip()]
    n_words = len(words)
    n_unique = len(word_set)
    n_sents = max(1, len(sents))
    clause_markers = sum(1 for c in gist_text if c in ",;:")
    raw_richness = n_unique / max(1, n_words)
    # MATTR: average TTR over sliding windows of fixed size
    window = 50
    if n_words >= window:
        lc_words = [w.lower() for w in words]
        ttrs = []
        for i in range(n_words - window + 1):
            ttrs.append(len(set(lc_words[i:i + window])) / window)
        mattr = sum(ttrs) / len(ttrs)
    else:
        mattr = raw_richness
    return {
        "words": n_words,
        "unique": n_unique,
        "richness": raw_richness,
        "mattr": mattr,
        "avg_sent": n_words / n_sents,
        "clauses": clause_markers,
    }


def section_tokens(gist_text: str) -> dict[str, set[str]]:
    """Parse markdown sections from gist and tokenize each."""
    sections = {}
    current = "_preamble"
    buf = []
    for line in gist_text.split("\n"):
        if line.startswith("## "):
            if buf:
                sections[current] = tokens(" ".join(buf))
            current = line[3:].strip().lower()
            buf = []
        else:
            buf.append(line)
    if buf:
        sections[current] = tokens(" ".join(buf))
    return sections


def section_jaccard(a: dict[str, set], b: dict[str, set]) -> dict:
    """Per-section and overall Jaccard between two section dicts."""
    all_keys = set(a) | set(b)
    header_j = jaccard(set(a), set(b))
    per_section = {}
    for k in all_keys:
        per_section[k] = jaccard(a.get(k, set()), b.get(k, set()))
    shared_keys = set(a) & set(b)
    avg_content_j = (sum(per_section[k] for k in shared_keys) /
                     len(shared_keys)) if shared_keys else 0.0
    return {"header_jaccard": header_j, "content_jaccard": avg_content_j,
            "per_section": per_section}


def main(limit: int = 50, recent: bool = False):
    con = sqlite3.connect(DB)
    order = "DESC" if recent else "ASC"
    rows = con.execute(
        f"SELECT id, created_at, snapshot FROM cognitive_state_history "
        f"ORDER BY created_at {order} LIMIT ?", (limit,)
    ).fetchall()
    con.close()
    if recent:
        rows = list(reversed(rows))

    if not rows:
        print("no snapshots")
        return

    has_entities = any(
        len((json.loads(r[2]).get("focal_entities") or []))
        for r in rows[:5]
    )

    if has_entities:
        _run_structured(rows)
    else:
        _run_gist_only(rows)


def _run_structured(rows):
    prev = None
    ent_j, con_j, gist_j = [], [], []
    mattr_vals = []
    print(f"{'id':>5}  {'ts':>10}  {'E':>3} {'C':>3} {'Gt':>4}  "
          f"{'Ej':>5} {'Cj':>5} {'Gj':>5}  {'MATTR':>5} {'Wds':>4}")
    for rid, ts, snap_str in rows:
        try:
            ccs = json.loads(snap_str)
        except Exception:
            continue
        ents = entity_names(ccs)
        cons = constraint_set(ccs)
        raw_gist = ccs.get("semantic_gist") or ""
        gist = tokens(raw_gist)
        depth = gist_depth(raw_gist)
        mattr_vals.append(depth["mattr"])
        nE, nC, nG = len(ents), len(cons), len(gist)
        if prev is None:
            print(f"{rid:>5}  {ts:>10}  {nE:>3} {nC:>3} {nG:>4}  "
                  f"{'-':>5} {'-':>5} {'-':>5}  "
                  f"{depth['mattr']:>5.3f} {depth['words']:>4}")
        else:
            pe, pc, pg = prev
            ej, cj, gj = jaccard(ents, pe), jaccard(cons, pc), jaccard(gist, pg)
            ent_j.append(ej); con_j.append(cj); gist_j.append(gj)
            print(f"{rid:>5}  {ts:>10}  {nE:>3} {nC:>3} {nG:>4}  "
                  f"{ej:>5.02f} {cj:>5.02f} {gj:>5.02f}  "
                  f"{depth['mattr']:>5.3f} {depth['words']:>4}")
        prev = (ents, cons, gist)

    _print_stats([
        (ent_j, "entity_jaccard   "),
        (con_j, "constraint_jacc. "),
        (gist_j, "gist_jaccard     "),
        (mattr_vals, "MATTR (depth)    "),
    ], len(ent_j))
    print("\n  Reading (structured format):")
    print("  jaccard = vocabulary overlap. MATTR = length-corrected diversity.")
    print("  If jaccard drifts but MATTR holds: conditions persist, content rotates.")


def _run_gist_only(rows):
    prev_sections = None
    prev_gist = None
    header_j, content_j, gist_j = [], [], []
    mattr_vals = []
    print(f"{'id':>5}  {'ts':>10}  {'Sec':>3} {'Wds':>4}  "
          f"{'Hdr_J':>5} {'Cnt_J':>5} {'Gst_J':>5}  {'MATTR':>5}")
    for rid, ts, snap_str in rows:
        try:
            ccs = json.loads(snap_str)
        except Exception:
            continue
        raw_gist = ccs.get("semantic_gist") or ""
        secs = section_tokens(raw_gist)
        gist = tokens(raw_gist)
        depth = gist_depth(raw_gist)
        mattr_vals.append(depth["mattr"])
        n_sec = len(secs)
        if prev_sections is None:
            print(f"{rid:>5}  {ts:>10}  {n_sec:>3} {depth['words']:>4}  "
                  f"{'-':>5} {'-':>5} {'-':>5}  {depth['mattr']:>5.3f}")
        else:
            sj = section_jaccard(prev_sections, secs)
            gj = jaccard(gist, prev_gist)
            header_j.append(sj["header_jaccard"])
            content_j.append(sj["content_jaccard"])
            gist_j.append(gj)
            print(f"{rid:>5}  {ts:>10}  {n_sec:>3} {depth['words']:>4}  "
                  f"{sj['header_jaccard']:>5.02f} {sj['content_jaccard']:>5.02f} "
                  f"{gj:>5.02f}  {depth['mattr']:>5.3f}")
        prev_sections = secs
        prev_gist = gist

    _print_stats([
        (header_j, "section_headers  "),
        (content_j, "section_content  "),
        (gist_j, "full_gist_jaccard"),
        (mattr_vals, "MATTR (depth)    "),
    ], len(header_j))
    print("\n  Reading (gist-only format, entities embedded in text):")
    print("  section_headers = do the same ## sections appear? (structural skeleton)")
    print("  section_content = within shared sections, do words persist? (local detail)")
    print("  full_gist = overall vocabulary overlap across entire gist")
    print("  If headers hold but content rotates: structure persists, detail churns.")


def _print_stats(pairs, n_transitions):
    print(f"\n  n_transitions={n_transitions}")
    for xs, label in pairs:
        if not xs:
            continue
        mean = sum(xs) / len(xs)
        third = max(1, len(xs) // 3)
        early = sum(xs[:third]) / third if len(xs) >= 3 else mean
        late = sum(xs[-third:]) / third if len(xs) >= 3 else mean
        drift = late - early
        print(f"  {label}: mean={mean:.3f}  early={early:.3f}  late={late:.3f}  "
              f"drift={drift:+.3f}")


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("limit", nargs="?", type=int, default=50)
    p.add_argument("--recent", action="store_true",
                   help="analyze most recent N snapshots instead of oldest")
    args = p.parse_args()
    main(args.limit, args.recent)
