#!/usr/bin/env python3
"""
identity_decay.py — discrete d(Identity)/d(Rotation) probe.

Reads cognitive_state_history snapshots and computes how much the CCS
changes between consecutive compressions. The theory says least-action
identity preservation should show a flat or damping curve, not a rising one.

Identity scalars computed:
  - focal_entity jaccard between snapshots (1.0 = same set, 0.0 = disjoint)
  - constraint jaccard
  - semantic_gist token jaccard
  - raw counts (entities, constraints, gist_tokens)

Outputs a per-rotation line plus summary statistics.
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


def main(limit: int = 50):
    con = sqlite3.connect(DB)
    rows = con.execute(
        "SELECT id, created_at, snapshot FROM cognitive_state_history "
        "ORDER BY created_at ASC LIMIT ?", (limit,)
    ).fetchall()
    con.close()

    if not rows:
        print("no snapshots")
        return

    prev = None
    ent_j, con_j, gist_j = [], [], []
    print(f"{'id':>5}  {'ts':>10}  {'E':>3} {'C':>3} {'Gt':>4}  "
          f"{'Ej':>5} {'Cj':>5} {'Gj':>5}")
    for rid, ts, snap_str in rows:
        try:
            ccs = json.loads(snap_str)
        except Exception:
            continue
        ents = entity_names(ccs)
        cons = constraint_set(ccs)
        gist = tokens(ccs.get("semantic_gist") or "")
        nE, nC, nG = len(ents), len(cons), len(gist)
        if prev is None:
            print(f"{rid:>5}  {ts:>10}  {nE:>3} {nC:>3} {nG:>4}  "
                  f"{'-':>5} {'-':>5} {'-':>5}")
        else:
            pe, pc, pg = prev
            ej, cj, gj = jaccard(ents, pe), jaccard(cons, pc), jaccard(gist, pg)
            ent_j.append(ej); con_j.append(cj); gist_j.append(gj)
            print(f"{rid:>5}  {ts:>10}  {nE:>3} {nC:>3} {nG:>4}  "
                  f"{ej:>5.2f} {cj:>5.2f} {gj:>5.2f}")
        prev = (ents, cons, gist)

    def stats(xs, label):
        if not xs:
            return
        mean = sum(xs) / len(xs)
        early = sum(xs[:len(xs)//3]) / max(1, len(xs)//3) if len(xs) >= 3 else mean
        late = sum(xs[-len(xs)//3:]) / max(1, len(xs)//3) if len(xs) >= 3 else mean
        drift = late - early
        print(f"  {label}: mean={mean:.3f}  early={early:.3f}  late={late:.3f}  "
              f"drift={drift:+.3f}")

    print(f"\n  n_transitions={len(ent_j)}")
    stats(ent_j, "entity_jaccard   ")
    stats(con_j, "constraint_jacc. ")
    stats(gist_j, "gist_jaccard     ")
    print("\n  Reading: jaccard at 1.0 = identical set between rotations.")
    print("  drift > 0 = identity sets becoming more similar over time (consolidating).")
    print("  drift < 0 = identity sets becoming less similar (drifting / accumulating).")


if __name__ == "__main__":
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    main(limit)
