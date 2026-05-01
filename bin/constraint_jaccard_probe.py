#!/usr/bin/env python3
"""Constraint-layer invariance probe with semantic normalization.

String-level jaccard under-counts invariance because the compressor rewrites
constraints with typographic variation (en-dash vs hyphen, NBH vs hyphen,
curly vs straight quotes). This probe normalizes before comparing.

Reports raw jaccard vs normalized jaccard across all 49 transitions in
cognitive_state_history.
"""
import json
import re
import sqlite3
import unicodedata

DB = "/mnt/hdd/chronicle-data/processed.db"


def normalize(s: str) -> str:
    s = unicodedata.normalize("NFKC", s)
    # Map common typographic variants to ascii equivalents
    trans = str.maketrans({
        "\u2010": "-",  # hyphen
        "\u2011": "-",  # non-breaking hyphen
        "\u2012": "-",  # figure dash
        "\u2013": "-",  # en dash
        "\u2014": "-",  # em dash
        "\u2015": "-",  # horizontal bar
        "\u2018": "'", "\u2019": "'",
        "\u201C": '"', "\u201D": '"',
        "\u2026": "...",
        "\xa0": " ",  # nbsp
    })
    s = s.translate(trans)
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s


def jaccard(a: set, b: set) -> float:
    u = a | b
    if not u:
        return 1.0
    return len(a & b) / len(u)


def main():
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT id, snapshot FROM cognitive_state_history ORDER BY created_at ASC"
    ).fetchall()

    raw_snaps = []
    norm_snaps = []
    for rid, snap_json in rows:
        snap = json.loads(snap_json)
        constraints = snap.get("constraints") or []
        raw_snaps.append({"id": rid, "c": set(constraints)})
        norm_snaps.append({"id": rid, "c": {normalize(c) for c in constraints}})

    raw_j = []
    norm_j = []
    divergences = []
    for i in range(1, len(raw_snaps)):
        rj = jaccard(raw_snaps[i - 1]["c"], raw_snaps[i]["c"])
        nj = jaccard(norm_snaps[i - 1]["c"], norm_snaps[i]["c"])
        raw_j.append(rj)
        norm_j.append(nj)
        if nj - rj > 0.1:
            divergences.append((raw_snaps[i]["id"], rj, nj))

    def summary(vals, label):
        n = len(vals)
        m = sum(vals) / n
        lo = min(vals); hi = max(vals)
        print(f"  {label}: n={n} mean={m:.4f} min={lo:.3f} max={hi:.3f}")

    print("CONSTRAINT JACCARD: raw vs semantic-normalized")
    summary(raw_j, "raw     ")
    summary(norm_j, "norm NFKC")

    print(f"\nTransitions where normalization rescued invariance (Δ > 0.1): {len(divergences)}")
    for rid, rj, nj in divergences[:20]:
        print(f"  rot #{rid}: raw={rj:.3f} -> norm={nj:.3f}  (Δ={nj-rj:+.3f})")

    # Per-transition at flush #436 specifically
    for i in range(1, len(raw_snaps)):
        if raw_snaps[i]["id"] == 436:
            print(f"\nAt flush #436 boundary:")
            print(f"  raw constraint_j  = {raw_j[i-1]:.3f}")
            print(f"  norm constraint_j = {norm_j[i-1]:.3f}")
            break

    db.close()


if __name__ == "__main__":
    main()
