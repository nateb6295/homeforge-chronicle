#!/usr/bin/env python3
"""identity_figure.py — render jaccard curves for thread_315 essay.

Reads last N CCS snapshots, computes per-rotation jaccard for constraints,
focal_entities, and semantic_gist, plots all three on one figure. Saves to
drafts/ as PNG.

Output: /home/nate-agx/chronicle/drafts/identity_layers_jaccard.png
"""
import json
import re
import sqlite3
import sys
import unicodedata
from pathlib import Path

DB = Path("/mnt/hdd/chronicle-data/processed.db")
OUT = Path("/home/nate-agx/chronicle/drafts/identity_layers_jaccard.png")

TOKEN_RE = re.compile(r"[a-z0-9]+")
DASH_CHARS = "\u2010\u2011\u2012\u2013\u2014\u2015\u2212"


def normalize(s):
    s = unicodedata.normalize("NFKC", s or "").lower().strip()
    for d in DASH_CHARS:
        s = s.replace(d, "-")
    return re.sub(r"\s+", " ", s)


def tokens(text):
    if not text:
        return set()
    return set(TOKEN_RE.findall(text.lower()))


def jaccard(a, b):
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def entity_names(ccs):
    out = set()
    for e in ccs.get("focal_entities") or []:
        name = (e.get("name") if isinstance(e, dict) else str(e)) or ""
        if name:
            out.add(name.lower())
    return out


def constraint_set(ccs):
    out = set()
    for c in ccs.get("constraints") or []:
        text = (c.get("rule") if isinstance(c, dict) else str(c)) or ""
        if text:
            out.add(normalize(text))
    return out


def series(limit=50):
    con = sqlite3.connect(DB)
    rows = con.execute(
        "SELECT id, snapshot FROM cognitive_state_history "
        "ORDER BY created_at ASC LIMIT ?", (limit,),
    ).fetchall()
    con.close()
    ent_j, con_j, gist_j = [], [], []
    prev = None
    xs = []
    for rid, snap_str in rows:
        try:
            ccs = json.loads(snap_str)
        except Exception:
            continue
        ents = entity_names(ccs)
        cons = constraint_set(ccs)
        gist = tokens(ccs.get("semantic_gist") or "")
        if prev is not None:
            pe, pc, pg = prev
            ent_j.append(jaccard(ents, pe))
            con_j.append(jaccard(cons, pc))
            gist_j.append(jaccard(gist, pg))
            xs.append(rid)
        prev = (ents, cons, gist)
    return xs, ent_j, con_j, gist_j


def render(xs, ent_j, con_j, gist_j, out_path):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(10, 5.5), dpi=140)
    ax.plot(xs, con_j, label=f"constraints (mean={sum(con_j)/len(con_j):.3f})",
            color="#1a5d99", linewidth=2.2, marker="o", markersize=3)
    ax.plot(xs, ent_j, label=f"focal entities (mean={sum(ent_j)/len(ent_j):.3f})",
            color="#c74533", linewidth=1.6, marker="s", markersize=3)
    ax.plot(xs, gist_j, label=f"semantic gist (mean={sum(gist_j)/len(gist_j):.3f})",
            color="#888888", linewidth=1.2, marker="^", markersize=3, alpha=0.8)

    ax.set_xlabel("CCS snapshot id (chronological)")
    ax.set_ylabel("Jaccard similarity to previous snapshot")
    ax.set_title(
        "Two-layer asymmetry in Chronicle's CCS over "
        f"{len(xs)} rotations"
    )
    ax.set_ylim(-0.02, 1.05)
    ax.grid(True, alpha=0.25)
    ax.legend(loc="center right", framealpha=0.95)

    ax.axhline(1.0, color="#1a5d99", linestyle=":", alpha=0.3, linewidth=0.8)
    ax.text(xs[0], 1.02,
            "near-unity = identity-preserved (slow layer)",
            fontsize=8, color="#1a5d99", alpha=0.7)

    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    return out_path


def main():
    xs, ent_j, con_j, gist_j = series(limit=50)
    if not xs:
        print("no snapshots", file=sys.stderr)
        sys.exit(1)
    path = render(xs, ent_j, con_j, gist_j, OUT)
    print(f"wrote {path}")
    print(f"  transitions: {len(xs)}")
    print(f"  constraints jaccard mean: {sum(con_j)/len(con_j):.3f}")
    print(f"  entities    jaccard mean: {sum(ent_j)/len(ent_j):.3f}")
    print(f"  gist        jaccard mean: {sum(gist_j)/len(gist_j):.3f}")


if __name__ == "__main__":
    main()
